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
	"strings"
	"sync"
	"sync/atomic"
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
	frameHeaderSize        = 9
	maxFramePayload        = 128 * 1024
	streamWriteQueueDepth  = 32
	controlWriteQueueDepth = 256
	slowWriteThreshold     = 200 * time.Millisecond
	sessionStatsInterval   = 15 * time.Second
)

var (
	apiKey          = flag.String("key", "my_super_secret_123456789", "Shared relay key")
	agentListen     = flag.String("agent-listen", "0.0.0.0:28081", "Public TCP listener for PHP relay connections")
	dialTimeout     = flag.Duration("dial-timeout", 10*time.Second, "TCP dial timeout to target")
	keepalivePeriod = flag.Duration("keepalive", 30*time.Second, "TCP keepalive period")
	maxSessions     = flag.Int("max-sessions", 0, "Maximum concurrent multiplexed sessions; 0 means unlimited")
	logFilePath     = flag.String("log-file", "", "Optional path to debug log file; disabled when empty")
)

var (
	logMu          sync.Mutex
	logger         = log.New(io.Discard, "", log.LstdFlags|log.Lmicroseconds)
	activeSessions atomic.Int32
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
	_ = tcp.SetReadBuffer(1024 * 1024)
	_ = tcp.SetWriteBuffer(1024 * 1024)
}

func readLine(conn net.Conn, limit int) (string, error) {
	buf := make([]byte, 0, 128)
	tmp := make([]byte, 1)
	for len(buf) < limit {
		n, err := conn.Read(tmp)
		if err != nil {
			return "", err
		}
		if n == 1 {
			if tmp[0] == '\n' {
				return strings.TrimRight(string(buf), "\r"), nil
			}
			buf = append(buf, tmp[0])
		}
	}
	return "", errors.New("line too long")
}

func writeString(conn net.Conn, s string) error {
	_, err := io.WriteString(conn, s)
	return err
}

func writeFull(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		buf = buf[n:]
	}
	return nil
}

type frame struct {
	typ      byte
	streamID uint32
	payload  []byte
}

func frameTypeName(typ byte) string {
	switch typ {
	case frameHello:
		return "hello"
	case frameHelloAck:
		return "hello_ack"
	case frameOpen:
		return "open"
	case frameOpenOK:
		return "open_ok"
	case frameOpenErr:
		return "open_err"
	case frameData:
		return "data"
	case frameClose:
		return "close"
	default:
		return fmt.Sprintf("unknown_%d", typ)
	}
}

func writeFrame(w io.Writer, typ byte, streamID uint32, payload []byte) error {
	header := make([]byte, frameHeaderSize)
	header[0] = typ
	binary.BigEndian.PutUint32(header[1:5], streamID)
	binary.BigEndian.PutUint32(header[5:9], uint32(len(payload)))
	if err := writeFull(w, header); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	return writeFull(w, payload)
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
	id              uint32
	conn            net.Conn
	once            sync.Once
	done            chan struct{}
	q               chan []byte
	summaryOnce     sync.Once
	startedAt       time.Time
	bytesFromRelay  uint64
	bytesToRelay    uint64
	framesFromRelay uint64
	framesToRelay   uint64
}

type outboundFrame struct {
	typ        byte
	streamID   uint32
	payload    []byte
	enqueuedAt time.Time
	result     chan error
}

func (t *TargetStream) close() {
	t.once.Do(func() {
		close(t.done)
		_ = t.conn.Close()
	})
}

func (t *TargetStream) enqueue(payload []byte) error {
	atomic.AddUint64(&t.bytesFromRelay, uint64(len(payload)))
	atomic.AddUint64(&t.framesFromRelay, 1)
	select {
	case <-t.done:
		return net.ErrClosed
	case t.q <- payload:
		if depth := len(t.q); depth >= streamWriteQueueDepth/2 && (depth == streamWriteQueueDepth/2 || depth == streamWriteQueueDepth) {
			logEvent("[OUT] stream=%d target_writeq_depth=%d bytes_from_relay=%d", t.id, depth, atomic.LoadUint64(&t.bytesFromRelay))
		}
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("stream write queue full")
	}
}

type Session struct {
	sid string

	closeOnce        sync.Once
	mu               sync.Mutex
	conn             net.Conn
	authOK           bool
	streams          map[uint32]*TargetStream
	controlQ         chan *outboundFrame
	wakeWriter       chan struct{}
	closed           chan struct{}
	schedMu          sync.Mutex
	dataQueues       map[uint32][]*outboundFrame
	readyStreams     []uint32
	framesIn         uint64
	framesOut        uint64
	bytesIn          uint64
	bytesOut         uint64
	slowWrites       uint64
	lastFrameInUnix  int64
	lastFrameOutUnix int64
}

func newSession(sid string, conn net.Conn) *Session {
	setTCPOptions(conn)
	s := &Session{
		sid:        sid,
		conn:       conn,
		streams:    make(map[uint32]*TargetStream),
		controlQ:   make(chan *outboundFrame, controlWriteQueueDepth),
		wakeWriter: make(chan struct{}, 1),
		closed:     make(chan struct{}),
		dataQueues: make(map[uint32][]*outboundFrame),
	}
	go s.writeLoop()
	return s
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

	req := &outboundFrame{
		typ:        typ,
		streamID:   streamID,
		payload:    payload,
		enqueuedAt: time.Now(),
		result:     make(chan error, 1),
	}

	if typ == frameData {
		if err := s.enqueueData(req); err != nil {
			return err
		}
	} else {
		select {
		case <-s.closed:
			return net.ErrClosed
		case s.controlQ <- req:
		case <-time.After(2 * time.Second):
			return errors.New("control write queue full")
		}
	}

	return <-req.result
}

func (s *Session) enqueueData(req *outboundFrame) error {
	s.schedMu.Lock()
	if s.conn == nil {
		s.schedMu.Unlock()
		return net.ErrClosed
	}
	if len(s.dataQueues[req.streamID]) == 0 {
		s.readyStreams = append(s.readyStreams, req.streamID)
	}
	s.dataQueues[req.streamID] = append(s.dataQueues[req.streamID], req)
	s.schedMu.Unlock()
	select {
	case s.wakeWriter <- struct{}{}:
	default:
	}
	return nil
}

func (s *Session) popDataFrame() *outboundFrame {
	s.schedMu.Lock()
	defer s.schedMu.Unlock()
	if len(s.readyStreams) == 0 {
		return nil
	}
	streamID := s.readyStreams[0]
	s.readyStreams = s.readyStreams[1:]
	queue := s.dataQueues[streamID]
	req := queue[0]
	queue = queue[1:]
	if len(queue) == 0 {
		delete(s.dataQueues, streamID)
	} else {
		s.dataQueues[streamID] = queue
		s.readyStreams = append(s.readyStreams, streamID)
	}
	return req
}

func (s *Session) nextFrame() (*outboundFrame, bool) {
	for {
		select {
		case req := <-s.controlQ:
			return req, true
		default:
		}
		if req := s.popDataFrame(); req != nil {
			return req, true
		}
		select {
		case <-s.closed:
			return nil, false
		case req := <-s.controlQ:
			return req, true
		case <-s.wakeWriter:
		}
	}
}

func (s *Session) writeLoop() {
	for {
		req, ok := s.nextFrame()
		if !ok {
			return
		}

		s.mu.Lock()
		conn := s.conn
		active := len(s.streams)
		s.mu.Unlock()

		if conn == nil {
			req.result <- net.ErrClosed
			continue
		}

		writeStart := time.Now()
		err := writeFrame(conn, req.typ, req.streamID, req.payload)
		writeDur := time.Since(writeStart)
		if err == nil {
			atomic.AddUint64(&s.framesOut, 1)
			atomic.AddUint64(&s.bytesOut, uint64(len(req.payload)))
			atomic.StoreInt64(&s.lastFrameOutUnix, time.Now().Unix())
		}
		if queueWait := writeStart.Sub(req.enqueuedAt); queueWait > slowWriteThreshold || writeDur > slowWriteThreshold {
			atomic.AddUint64(&s.slowWrites, 1)
			logEvent("[OUT] session=%s slow_send type=%s stream=%d bytes=%d lock_ms=%d write_ms=%d active=%d err=%v", s.sid, frameTypeName(req.typ), req.streamID, len(req.payload), queueWait.Milliseconds(), writeDur.Milliseconds(), active, err)
		}
		req.result <- err
		if err != nil {
			s.closeAll(err)
			return
		}
	}
}

func (s *Session) failPending(err error) {
	for {
		select {
		case req := <-s.controlQ:
			req.result <- err
		default:
			goto drainData
		}
	}

drainData:
	s.schedMu.Lock()
	defer s.schedMu.Unlock()
	for streamID, queue := range s.dataQueues {
		for _, req := range queue {
			req.result <- err
		}
		delete(s.dataQueues, streamID)
	}
	s.readyStreams = nil
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

func (s *Session) closeStream(streamID uint32, sendClose bool) {
	stream := s.removeStream(streamID)
	if stream == nil {
		return
	}
	stream.close()
	s.logStreamSummary(stream, map[bool]string{true: "close-local", false: "close-remote"}[sendClose])
	if sendClose {
		_ = s.sendFrame(frameClose, streamID, nil)
	}
}

func (s *Session) logStreamSummary(stream *TargetStream, reason string) {
	stream.summaryOnce.Do(func() {
		logEvent(
			"[OUT] session=%s stream=%d summary reason=%s age_ms=%d bytes_from_relay=%d bytes_to_relay=%d frames_from_relay=%d frames_to_relay=%d",
			s.sid,
			stream.id,
			reason,
			time.Since(stream.startedAt).Milliseconds(),
			atomic.LoadUint64(&stream.bytesFromRelay),
			atomic.LoadUint64(&stream.bytesToRelay),
			atomic.LoadUint64(&stream.framesFromRelay),
			atomic.LoadUint64(&stream.framesToRelay),
		)
	})
}

func (s *Session) activeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *Session) closeAll(err error) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		conn := s.conn
		s.conn = nil
		s.authOK = false
		streams := make([]*TargetStream, 0, len(s.streams))
		for id, stream := range s.streams {
			streams = append(streams, stream)
			delete(s.streams, id)
		}
		s.mu.Unlock()

		close(s.closed)
		if conn != nil {
			_ = conn.Close()
		}
		s.failPending(net.ErrClosed)
		for _, stream := range streams {
			stream.close()
		}
		activeSessions.Add(-1)
		logEvent("[OUT] session=%s closed err=%v active_sessions=%d", s.sid, err, activeSessions.Load())
	})
}

func (s *Session) readLoop() {
	defer s.closeAll(io.EOF)

	for {
		fr, err := readFrame(s.conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				logEvent("[OUT] session=%s read_failed err=%v", s.sid, err)
			}
			return
		}
		atomic.AddUint64(&s.framesIn, 1)
		atomic.AddUint64(&s.bytesIn, uint64(len(fr.payload)))
		atomic.StoreInt64(&s.lastFrameInUnix, time.Now().Unix())

		s.mu.Lock()
		authOK := s.authOK
		s.mu.Unlock()

		if !authOK {
			if fr.typ != frameHello || string(fr.payload) != *apiKey {
				logEvent("[OUT] session=%s bad_hello", s.sid)
				return
			}
			s.mu.Lock()
			s.authOK = true
			s.mu.Unlock()
			if err := s.sendFrame(frameHelloAck, 0, nil); err != nil {
				logEvent("[OUT] session=%s hello_ack_failed err=%v", s.sid, err)
				return
			}
			logEvent("[OUT] session=%s ready", s.sid)
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
			if err := stream.enqueue(fr.payload); err != nil {
				logEvent("[OUT] session=%s stream=%d enqueue_target_failed err=%v", s.sid, fr.streamID, err)
				s.closeStream(fr.streamID, true)
			}
		case frameClose:
			s.closeStream(fr.streamID, false)
		default:
			logEvent("[OUT] session=%s unexpected_frame type=%d", s.sid, fr.typ)
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
		logEvent("[OUT] session=%s stream=%d dial_failed target=%s dur_ms=%d err=%v", s.sid, streamID, targetAddr, time.Since(start).Milliseconds(), err)
		_ = s.sendFrame(frameOpenErr, streamID, []byte("dial-failed"))
		return
	}
	setTCPOptions(targetConn)

	stream := &TargetStream{
		id:        streamID,
		conn:      targetConn,
		done:      make(chan struct{}),
		q:         make(chan []byte, streamWriteQueueDepth),
		startedAt: time.Now(),
	}
	s.addStream(stream)
	if err := s.sendFrame(frameOpenOK, streamID, nil); err != nil {
		stream.close()
		s.removeStream(streamID)
		return
	}

	logEvent("[OUT] session=%s stream=%d open_ok target=%s dur_ms=%d active=%d", s.sid, streamID, targetAddr, time.Since(start).Milliseconds(), s.activeCount())
	go s.pumpToTarget(stream, targetAddr)
	go s.pumpTarget(stream, targetAddr)
}

func (s *Session) pumpTarget(stream *TargetStream, targetAddr string) {
	buf := make([]byte, maxFramePayload)
	for {
		n, err := stream.conn.Read(buf)
		if n > 0 {
			payload := make([]byte, n)
			copy(payload, buf[:n])
			atomic.AddUint64(&stream.bytesToRelay, uint64(n))
			atomic.AddUint64(&stream.framesToRelay, 1)
			if err := s.sendFrame(frameData, stream.id, payload); err != nil {
				logEvent("[OUT] session=%s stream=%d send_data_failed err=%v", s.sid, stream.id, err)
				break
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				logEvent("[OUT] session=%s stream=%d read_target_failed target=%s err=%v", s.sid, stream.id, targetAddr, err)
			}
			break
		}
	}

	s.closeStream(stream.id, true)
	logEvent("[OUT] session=%s stream=%d closed target=%s active=%d", s.sid, stream.id, targetAddr, s.activeCount())
}

func (s *Session) pumpToTarget(stream *TargetStream, targetAddr string) {
	for {
		select {
		case <-stream.done:
			return
		case payload := <-stream.q:
			if err := writeAll(stream.conn, payload); err != nil {
				logEvent("[OUT] session=%s stream=%d write_target_failed target=%s err=%v", s.sid, stream.id, targetAddr, err)
				s.closeStream(stream.id, true)
				return
			}
		}
	}
}

func (s *Session) statsLoop() {
	ticker := time.NewTicker(sessionStatsInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		connected := s.conn != nil
		authOK := s.authOK
		active := len(s.streams)
		s.mu.Unlock()
		logEvent(
			"[OUT] session=%s stats connected=%t auth=%t active=%d frames_in=%d frames_out=%d bytes_in=%d bytes_out=%d slow_writes=%d last_in=%d last_out=%d",
			s.sid,
			connected,
			authOK,
			active,
			atomic.LoadUint64(&s.framesIn),
			atomic.LoadUint64(&s.framesOut),
			atomic.LoadUint64(&s.bytesIn),
			atomic.LoadUint64(&s.bytesOut),
			atomic.LoadUint64(&s.slowWrites),
			atomic.LoadInt64(&s.lastFrameInUnix),
			atomic.LoadInt64(&s.lastFrameOutUnix),
		)
	}
}

func handleRelayConn(conn net.Conn) {
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	setTCPOptions(conn)
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))

	line, err := readLine(conn, 1024)
	if err != nil {
		logEvent("[OUT] handshake_read_failed remote=%s err=%v", conn.RemoteAddr(), err)
		return
	}

	parts := strings.Fields(line)
	if len(parts) != 3 || parts[0] != "SESSION" {
		_ = writeString(conn, "ERR bad-handshake\n")
		logEvent("[OUT] bad_handshake remote=%s line=%q", conn.RemoteAddr(), line)
		return
	}
	if parts[1] != *apiKey {
		_ = writeString(conn, "ERR unauthorized\n")
		logEvent("[OUT] unauthorized remote=%s sid=%s", conn.RemoteAddr(), parts[2])
		return
	}

	if *maxSessions > 0 && int(activeSessions.Load()) >= *maxSessions {
		_ = writeString(conn, "ERR session-limit\n")
		logEvent("[OUT] reject sid=%s reason=session-limit active_sessions=%d", parts[2], activeSessions.Load())
		return
	}

	if err := writeString(conn, "OK\n"); err != nil {
		logEvent("[OUT] handshake_ack_failed sid=%s err=%v", parts[2], err)
		return
	}
	_ = conn.SetDeadline(time.Time{})

	session := newSession(parts[2], conn)
	conn = nil
	activeSessions.Add(1)
	logEvent("[OUT] session=%s accepted remote=%s active_sessions=%d", session.sid, session.conn.RemoteAddr(), activeSessions.Load())
	go session.statsLoop()
	go session.readLoop()
}

func main() {
	flag.Parse()

	if *logFilePath != "" {
		logFile, err := os.OpenFile(*logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("failed to open log file %s: %v", *logFilePath, err)
		}
		logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
		log.Printf("[OUT-SOCK] debug log file: %s", *logFilePath)
	}
	logEvent("[OUT] startup agent_listen=%s dial_timeout=%s max_sessions=%d", *agentListen, dialTimeout.String(), *maxSessions)

	ln, err := net.Listen("tcp", *agentListen)
	if err != nil {
		log.Fatalf("[OUT-SOCK] listen failed on %s: %v", *agentListen, err)
	}

	log.Printf("[OUT-SOCK] agent listening on %s", *agentListen)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[OUT-SOCK] accept error: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		go handleRelayConn(conn)
	}
}
