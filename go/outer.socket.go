package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	frameHello byte = iota + 1
	frameHelloAck
	frameOpen
	frameOpenOK
	frameOpenErr
	frameData
	frameClose
)

const (
	frameHeaderSize = 9
	maxFramePayload = 64 * 1024
)

var (
	apiKey          = flag.String("key", "my_super_secret_123456789", "Shared relay key")
	agentListen     = flag.String("agent-listen", "0.0.0.0:28081", "Public TCP listener for PHP relay connections")
	dialTimeout     = flag.Duration("dial-timeout", 10*time.Second, "TCP dial timeout to target")
	keepalivePeriod = flag.Duration("keepalive", 30*time.Second, "TCP keepalive period")
	logFilePath     = flag.String("log-file", "logs.txt", "Path to debug log file")
)

var (
	logMu  sync.Mutex
	logger = log.New(io.Discard, "", log.LstdFlags|log.Lmicroseconds)
)

func logEvent(format string, args ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	logger.Printf(format, args...)
}

func setTCPOptions(conn net.Conn) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcp.SetKeepAlive(true)
	_ = tcp.SetKeepAlivePeriod(*keepalivePeriod)
	_ = tcp.SetNoDelay(true)
	_ = tcp.SetReadBuffer(256 * 1024)
	_ = tcp.SetWriteBuffer(256 * 1024)
}

type frame struct {
	typ      byte
	streamID uint32
	payload  []byte
}

func writeFrame(w io.Writer, typ byte, streamID uint32, payload []byte) error {
	header := make([]byte, frameHeaderSize)
	header[0] = typ
	binary.BigEndian.PutUint32(header[1:5], streamID)
	binary.BigEndian.PutUint32(header[5:9], uint32(len(payload)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

func readFrame(r io.Reader) (frame, error) {
	var hdr [frameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return frame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[5:9])
	if n > 16*1024*1024 {
		return frame{}, fmt.Errorf("frame too large: %d", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return frame{}, err
	}
	return frame{
		typ:      hdr[0],
		streamID: binary.BigEndian.Uint32(hdr[1:5]),
		payload:  payload,
	}, nil
}

func decodeOpenPayload(payload []byte) (string, uint16, error) {
	if len(payload) < 4 {
		return "", 0, errors.New("open payload too short")
	}
	hostLen := int(binary.BigEndian.Uint16(payload[0:2]))
	if len(payload) != 2+hostLen+2 {
		return "", 0, errors.New("open payload malformed")
	}
	host := string(payload[2 : 2+hostLen])
	port := binary.BigEndian.Uint16(payload[2+hostLen:])
	if host == "" || port == 0 {
		return "", 0, errors.New("invalid target")
	}
	return host, port, nil
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

type TargetStream struct {
	id   uint32
	conn net.Conn
	once sync.Once
}

func (t *TargetStream) close() {
	t.once.Do(func() {
		_ = t.conn.Close()
	})
}

type Session struct {
	mu       sync.Mutex
	writerMu sync.Mutex
	conn     net.Conn
	gen      uint64
	authOK   bool
	streams  map[uint32]*TargetStream
}

func newSession() *Session {
	return &Session{streams: make(map[uint32]*TargetStream)}
}

func (s *Session) setConn(conn net.Conn) {
	setTCPOptions(conn)

	s.mu.Lock()
	oldConn := s.conn
	oldStreams := s.snapshotAndResetLocked()
	s.conn = conn
	s.authOK = false
	s.gen++
	gen := s.gen
	s.mu.Unlock()

	if oldConn != nil {
		_ = oldConn.Close()
	}
	for _, stream := range oldStreams {
		stream.close()
	}

	logEvent("[OUT] session accepted remote=%s", conn.RemoteAddr())
	go s.readLoop(gen, conn)
}

func (s *Session) snapshotAndResetLocked() []*TargetStream {
	streams := make([]*TargetStream, 0, len(s.streams))
	for id, stream := range s.streams {
		streams = append(streams, stream)
		delete(s.streams, id)
	}
	return streams
}

func (s *Session) closeSession(gen uint64, err error) {
	s.mu.Lock()
	if gen != 0 && gen != s.gen {
		s.mu.Unlock()
		return
	}
	conn := s.conn
	streams := s.snapshotAndResetLocked()
	s.conn = nil
	s.authOK = false
	s.gen++
	s.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	for _, stream := range streams {
		stream.close()
	}
	logEvent("[OUT] session closed err=%v", err)
}

func (s *Session) sendFrame(typ byte, streamID uint32, payload []byte) error {
	s.mu.Lock()
	conn := s.conn
	authOK := s.authOK
	s.mu.Unlock()

	if conn == nil {
		return errors.New("session not connected")
	}
	if typ != frameHelloAck && !authOK {
		return errors.New("session not authenticated")
	}

	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	return writeFrame(conn, typ, streamID, payload)
}

func (s *Session) addStream(stream *TargetStream) {
	s.mu.Lock()
	s.streams[stream.id] = stream
	s.mu.Unlock()
}

func (s *Session) getStream(streamID uint32) *TargetStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[streamID]
}

func (s *Session) removeStream(streamID uint32) *TargetStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := s.streams[streamID]
	delete(s.streams, streamID)
	return stream
}

func (s *Session) activeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *Session) readLoop(gen uint64, conn net.Conn) {
	for {
		fr, err := readFrame(conn)
		if err != nil {
			s.closeSession(gen, err)
			return
		}

		s.mu.Lock()
		authOK := s.authOK
		s.mu.Unlock()

		if !authOK {
			if fr.typ != frameHello || string(fr.payload) != *apiKey {
				s.closeSession(gen, errors.New("bad hello"))
				return
			}
			s.mu.Lock()
			if gen == s.gen && s.conn == conn {
				s.authOK = true
			}
			s.mu.Unlock()
			if err := s.sendFrame(frameHelloAck, 0, nil); err != nil {
				s.closeSession(gen, err)
				return
			}
			logEvent("[OUT] session ready")
			continue
		}

		switch fr.typ {
		case frameOpen:
			s.handleOpen(fr.streamID, fr.payload)
		case frameData:
			stream := s.getStream(fr.streamID)
			if stream == nil {
				_ = s.sendFrame(frameClose, fr.streamID, nil)
				continue
			}
			if err := writeAll(stream.conn, fr.payload); err != nil {
				logEvent("[OUT] stream=%d write_target_failed err=%v", fr.streamID, err)
				stream.close()
				s.removeStream(fr.streamID)
				_ = s.sendFrame(frameClose, fr.streamID, nil)
			}
		case frameClose:
			if stream := s.removeStream(fr.streamID); stream != nil {
				stream.close()
			}
		default:
			s.closeSession(gen, fmt.Errorf("unexpected frame type=%d", fr.typ))
			return
		}
	}
}

func (s *Session) handleOpen(streamID uint32, payload []byte) {
	host, port, err := decodeOpenPayload(payload)
	if err != nil {
		_ = s.sendFrame(frameOpenErr, streamID, []byte(err.Error()))
		return
	}

	targetAddr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	dialer := net.Dialer{Timeout: *dialTimeout, KeepAlive: *keepalivePeriod}
	start := time.Now()
	targetConn, err := dialer.Dial("tcp", targetAddr)
	if err != nil {
		logEvent("[OUT] stream=%d dial_failed target=%s dur_ms=%d err=%v", streamID, targetAddr, time.Since(start).Milliseconds(), err)
		_ = s.sendFrame(frameOpenErr, streamID, []byte("dial-failed"))
		return
	}
	setTCPOptions(targetConn)

	stream := &TargetStream{id: streamID, conn: targetConn}
	s.addStream(stream)
	if err := s.sendFrame(frameOpenOK, streamID, nil); err != nil {
		stream.close()
		s.removeStream(streamID)
		return
	}

	logEvent("[OUT] stream=%d open_ok target=%s dur_ms=%d active=%d", streamID, targetAddr, time.Since(start).Milliseconds(), s.activeCount())
	go s.pumpTarget(stream, targetAddr)
}

func (s *Session) pumpTarget(stream *TargetStream, targetAddr string) {
	buf := make([]byte, maxFramePayload)
	for {
		n, err := stream.conn.Read(buf)
		if n > 0 {
			payload := make([]byte, n)
			copy(payload, buf[:n])
			if err := s.sendFrame(frameData, stream.id, payload); err != nil {
				logEvent("[OUT] stream=%d send_data_failed err=%v", stream.id, err)
				break
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				logEvent("[OUT] stream=%d read_target_failed target=%s err=%v", stream.id, targetAddr, err)
			}
			break
		}
	}

	stream.close()
	s.removeStream(stream.id)
	_ = s.sendFrame(frameClose, stream.id, nil)
	logEvent("[OUT] stream=%d closed target=%s active=%d", stream.id, targetAddr, s.activeCount())
}

func main() {
	flag.Parse()

	logFile, err := os.OpenFile(*logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("failed to open log file %s: %v", *logFilePath, err)
	}
	logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
	log.Printf("[OUT-SOCK] debug log file: %s", *logFilePath)
	logEvent("[OUT] startup agent_listen=%s dial_timeout=%s", *agentListen, dialTimeout.String())

	ln, err := net.Listen("tcp", *agentListen)
	if err != nil {
		log.Fatalf("[OUT-SOCK] listen failed on %s: %v", *agentListen, err)
	}

	session := newSession()
	log.Printf("[OUT-SOCK] agent listening on %s", *agentListen)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[OUT-SOCK] accept error: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		session.setConn(conn)
	}
}
