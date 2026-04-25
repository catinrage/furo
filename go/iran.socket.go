package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/armon/go-socks5"
	xcontext "golang.org/x/net/context"
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
	frameHeaderSize       = 9
	maxFramePayload       = 128 * 1024
	sessionPollDelay      = 1200 * time.Millisecond
	streamWriteQueueDepth = 32
	slowWriteThreshold    = 200 * time.Millisecond
	sessionStatsInterval  = 15 * time.Second
)

var (
	relayURL        = flag.String("relay", "https://hidaco.site/tools/rel/soc/index.php", "PHP relay URL")
	apiKey          = flag.String("key", "my_super_secret_123456789", "Shared relay key")
	listenIP        = flag.String("listen", "127.0.0.1", "SOCKS5 listen IP")
	listenPort      = flag.String("port", "1080", "SOCKS5 listen port")
	agentListen     = flag.String("agent-listen", "0.0.0.0:28080", "Public TCP listener for PHP back-connect")
	publicHost      = flag.String("public-host", "", "Public host or IP that the PHP relay can reach for the Iran agent")
	publicPort      = flag.Int("public-port", 28080, "Public port that the PHP relay can reach for the Iran agent")
	outerHost       = flag.String("outer-host", "", "Outer agent public host or IP")
	outerPort       = flag.Int("outer-port", 28081, "Outer agent public TCP port")
	openTimeout     = flag.Duration("open-timeout", 15*time.Second, "HTTP setup timeout for session request")
	keepalivePeriod = flag.Duration("keepalive", 30*time.Second, "TCP keepalive period")
	sessionCount    = flag.Int("session-count", 1, "Number of multiplexed PHP sessions to maintain")
	maxRelays       = flag.Int("max-relays", 0, "Compatibility flag; ignored in multiplex mode")
	logFilePath     = flag.String("log-file", "", "Optional path to debug log file; disabled when empty")
)

var (
	logMu  sync.Mutex
	logger = log.New(io.Discard, "", log.LstdFlags|log.Lmicroseconds)

	httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   16,
			MaxConnsPerHost:       16,
			IdleConnTimeout:       30 * time.Second,
			DisableCompression:    true,
			DisableKeepAlives:     false,
			ForceAttemptHTTP2:     false,
			ResponseHeaderTimeout: 15 * time.Second,
			TLSHandshakeTimeout:   15 * time.Second,
		},
	}
)

func logEvent(format string, args ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	logger.Printf(format, args...)
}

type upstreamResolver struct {
	fallback *net.Resolver
}

type preserveFQDNRewriter struct{}

func (preserveFQDNRewriter) Rewrite(ctx xcontext.Context, request *socks5.Request) (xcontext.Context, *socks5.AddrSpec) {
	dest := request.DestAddr
	if dest == nil || dest.FQDN == "" {
		return ctx, dest
	}
	return ctx, &socks5.AddrSpec{FQDN: dest.FQDN, Port: dest.Port}
}

func newUpstreamResolver() *upstreamResolver {
	return &upstreamResolver{fallback: &net.Resolver{PreferGo: true}}
}

func (r *upstreamResolver) Resolve(ctx xcontext.Context, name string) (xcontext.Context, net.IP, error) {
	if ip := net.ParseIP(name); ip != nil {
		return ctx, ip, nil
	}
	ipAddrs, err := r.fallback.LookupIPAddr(ctx, name)
	if err != nil {
		return ctx, net.IPv4zero, nil
	}
	for _, addr := range ipAddrs {
		if v4 := addr.IP.To4(); v4 != nil {
			return ctx, v4, nil
		}
	}
	if len(ipAddrs) > 0 {
		return ctx, ipAddrs[0].IP, nil
	}
	return ctx, net.IPv4zero, nil
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

func encodeOpenPayload(host string, port uint16) []byte {
	hostBytes := []byte(host)
	payload := make([]byte, 2+len(hostBytes)+2)
	binary.BigEndian.PutUint16(payload[0:2], uint16(len(hostBytes)))
	copy(payload[2:2+len(hostBytes)], hostBytes)
	binary.BigEndian.PutUint16(payload[2+len(hostBytes):], port)
	return payload
}

func parsePort(value string) (uint16, error) {
	var port int
	if _, err := fmt.Sscanf(value, "%d", &port); err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid port: %s", value)
	}
	return uint16(port), nil
}

type MuxConn struct {
	id      uint32
	session *MuxSession

	mu              sync.Mutex
	cond            *sync.Cond
	openReady       bool
	openErr         error
	readQ           [][]byte
	readBuf         []byte
	closed          bool
	remoteClosed    bool
	done            chan struct{}
	writeQ          chan []byte
	closeOnce       sync.Once
	summaryOnce     sync.Once
	startedAt       time.Time
	bytesFromClient uint64
	bytesFromOuter  uint64
	framesToOuter   uint64
	framesFromOuter uint64
}

func newMuxConn(session *MuxSession, id uint32) *MuxConn {
	c := &MuxConn{id: id, session: session}
	c.cond = sync.NewCond(&c.mu)
	c.done = make(chan struct{})
	c.writeQ = make(chan []byte, streamWriteQueueDepth)
	c.startedAt = time.Now()
	return c
}

func (c *MuxConn) waitForOpen(ctx context.Context) error {
	for {
		c.mu.Lock()
		switch {
		case c.openReady:
			c.mu.Unlock()
			return nil
		case c.openErr != nil:
			err := c.openErr
			c.mu.Unlock()
			return err
		case c.closed:
			c.mu.Unlock()
			return io.EOF
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (c *MuxConn) markOpenReady() {
	c.mu.Lock()
	if !c.closed && c.openErr == nil {
		c.openReady = true
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

func (c *MuxConn) markOpenErr(err error) {
	c.mu.Lock()
	if !c.closed && c.openErr == nil {
		c.openErr = err
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

func (c *MuxConn) enqueueData(payload []byte) {
	if len(payload) == 0 {
		return
	}
	buf := make([]byte, len(payload))
	copy(buf, payload)
	atomic.AddUint64(&c.bytesFromOuter, uint64(len(buf)))
	atomic.AddUint64(&c.framesFromOuter, 1)
	c.mu.Lock()
	if !c.closed {
		c.readQ = append(c.readQ, buf)
		c.cond.Broadcast()
		if depth := len(c.readQ); depth >= streamWriteQueueDepth/2 && (depth == streamWriteQueueDepth/2 || depth == streamWriteQueueDepth) {
			logEvent("[IR] session=%s stream=%d readq_depth=%d bytes_from_outer=%d", c.session.sid, c.id, depth, atomic.LoadUint64(&c.bytesFromOuter))
		}
	}
	c.mu.Unlock()
}

func (c *MuxConn) markRemoteClosed() {
	c.mu.Lock()
	c.remoteClosed = true
	c.cond.Broadcast()
	c.mu.Unlock()
	c.closeOnce.Do(func() { close(c.done) })
	c.logSummary("remote-close")
}

func (c *MuxConn) fail(err error) {
	c.mu.Lock()
	if !c.closed && c.openErr == nil {
		c.openErr = err
	}
	c.cond.Broadcast()
	c.mu.Unlock()
	c.closeOnce.Do(func() { close(c.done) })
	logEvent("[IR] session=%s stream=%d fail err=%v", c.session.sid, c.id, err)
	c.logSummary("fail")
}

func (c *MuxConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for len(c.readBuf) == 0 && len(c.readQ) == 0 && !c.remoteClosed && !c.closed && c.openErr == nil {
		c.cond.Wait()
	}
	if len(c.readBuf) == 0 && len(c.readQ) > 0 {
		c.readBuf = c.readQ[0]
		c.readQ = c.readQ[1:]
	}
	if len(c.readBuf) > 0 {
		n := copy(b, c.readBuf)
		c.readBuf = c.readBuf[n:]
		return n, nil
	}
	if c.openErr != nil {
		return 0, c.openErr
	}
	if c.remoteClosed || c.closed {
		return 0, io.EOF
	}
	return 0, nil
}

func (c *MuxConn) Write(b []byte) (int, error) {
	if err := c.waitForOpen(context.Background()); err != nil {
		return 0, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, net.ErrClosed
	}
	c.mu.Unlock()

	total := 0
	for len(b) > 0 {
		chunkLen := len(b)
		if chunkLen > maxFramePayload {
			chunkLen = maxFramePayload
		}
		payload := make([]byte, chunkLen)
		copy(payload, b[:chunkLen])
		if err := c.enqueueWrite(payload); err != nil {
			c.fail(err)
			return total, err
		}
		atomic.AddUint64(&c.bytesFromClient, uint64(chunkLen))
		atomic.AddUint64(&c.framesToOuter, 1)
		total += chunkLen
		b = b[chunkLen:]
	}
	return total, nil
}

func (c *MuxConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()

	c.closeOnce.Do(func() { close(c.done) })
	c.session.removeStream(c.id)
	_ = c.session.sendFrame(frameClose, c.id, nil)
	c.logSummary("local-close")
	return nil
}

func (c *MuxConn) enqueueWrite(payload []byte) error {
	select {
	case <-c.done:
		return net.ErrClosed
	case c.writeQ <- payload:
		if depth := len(c.writeQ); depth >= streamWriteQueueDepth/2 && (depth == streamWriteQueueDepth/2 || depth == streamWriteQueueDepth) {
			logEvent("[IR] session=%s stream=%d writeq_depth=%d bytes_from_client=%d", c.session.sid, c.id, depth, atomic.LoadUint64(&c.bytesFromClient))
		}
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("stream write queue full")
	}
}

func (c *MuxConn) pumpWrites() {
	for {
		select {
		case <-c.done:
			return
		case payload := <-c.writeQ:
			if err := c.session.sendFrame(frameData, c.id, payload); err != nil {
				c.fail(err)
				return
			}
		}
	}
}

func (c *MuxConn) logSummary(reason string) {
	c.summaryOnce.Do(func() {
		logEvent(
			"[IR] session=%s stream=%d summary reason=%s age_ms=%d bytes_client=%d bytes_outer=%d frames_client=%d frames_outer=%d",
			c.session.sid,
			c.id,
			reason,
			time.Since(c.startedAt).Milliseconds(),
			atomic.LoadUint64(&c.bytesFromClient),
			atomic.LoadUint64(&c.bytesFromOuter),
			atomic.LoadUint64(&c.framesToOuter),
			atomic.LoadUint64(&c.framesFromOuter),
		)
	})
}

func (c *MuxConn) LocalAddr() net.Addr              { return &net.TCPAddr{IP: net.IPv4zero, Port: 0} }
func (c *MuxConn) RemoteAddr() net.Addr             { return &net.TCPAddr{IP: net.IPv4zero, Port: 0} }
func (c *MuxConn) SetDeadline(time.Time) error      { return nil }
func (c *MuxConn) SetReadDeadline(time.Time) error  { return nil }
func (c *MuxConn) SetWriteDeadline(time.Time) error { return nil }

type MuxSession struct {
	pool *SessionPool
	sid  string
	idx  int

	mu               sync.Mutex
	writerMu         sync.Mutex
	conn             net.Conn
	ready            bool
	requestRunning   bool
	streams          map[uint32]*MuxConn
	nextStreamID     uint32
	sessionGen       uint64
	framesIn         uint64
	framesOut        uint64
	bytesIn          uint64
	bytesOut         uint64
	slowWrites       uint64
	lastFrameInUnix  int64
	lastFrameOutUnix int64
}

func newMuxSession(pool *SessionPool, idx int, sid string) *MuxSession {
	return &MuxSession{
		pool:    pool,
		idx:     idx,
		sid:     sid,
		streams: make(map[uint32]*MuxConn),
	}
}

func (s *MuxSession) activeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *MuxSession) isReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready && s.conn != nil
}

func (s *MuxSession) snapshotAndResetStreamsLocked() []*MuxConn {
	streams := make([]*MuxConn, 0, len(s.streams))
	for id, stream := range s.streams {
		streams = append(streams, stream)
		delete(s.streams, id)
	}
	return streams
}

func (s *MuxSession) attachConn(conn net.Conn) {
	setTCPOptions(conn)

	s.mu.Lock()
	oldConn := s.conn
	oldStreams := s.snapshotAndResetStreamsLocked()
	s.conn = conn
	s.ready = false
	s.sessionGen++
	gen := s.sessionGen
	s.mu.Unlock()

	if oldConn != nil {
		_ = oldConn.Close()
	}
	for _, stream := range oldStreams {
		stream.fail(errors.New("session replaced"))
	}

	logEvent("[IR] session=%s accepted remote=%s", s.sid, conn.RemoteAddr())
	go s.readLoop(gen, conn)
	if err := s.sendHello(gen); err != nil {
		logEvent("[IR] session=%s hello_failed err=%v", s.sid, err)
		s.closeSession(gen, err)
	}
}

func (s *MuxSession) closeSession(gen uint64, err error) {
	s.mu.Lock()
	if gen != 0 && gen != s.sessionGen {
		s.mu.Unlock()
		return
	}
	conn := s.conn
	streams := s.snapshotAndResetStreamsLocked()
	s.conn = nil
	s.ready = false
	s.sessionGen++
	s.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	for _, stream := range streams {
		stream.fail(err)
	}
	logEvent("[IR] session=%s closed err=%v", s.sid, err)
}

func (s *MuxSession) sendHello(gen uint64) error {
	s.mu.Lock()
	if gen != s.sessionGen || s.conn == nil {
		s.mu.Unlock()
		return errors.New("no live session")
	}
	conn := s.conn
	s.mu.Unlock()

	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	return writeFrame(conn, frameHello, 0, []byte(*apiKey))
}

func (s *MuxSession) sendFrame(typ byte, streamID uint32, payload []byte) error {
	s.mu.Lock()
	conn := s.conn
	ready := s.ready
	active := len(s.streams)
	s.mu.Unlock()

	if conn == nil {
		return errors.New("session not connected")
	}
	if typ != frameHello && !ready {
		return errors.New("session not ready")
	}

	lockStart := time.Now()
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	lockWait := time.Since(lockStart)
	writeStart := time.Now()
	err := writeFrame(conn, typ, streamID, payload)
	writeDur := time.Since(writeStart)
	if err == nil {
		atomic.AddUint64(&s.framesOut, 1)
		atomic.AddUint64(&s.bytesOut, uint64(len(payload)))
		atomic.StoreInt64(&s.lastFrameOutUnix, time.Now().Unix())
	}
	if lockWait > slowWriteThreshold || writeDur > slowWriteThreshold {
		atomic.AddUint64(&s.slowWrites, 1)
		logEvent("[IR] session=%s slow_send type=%s stream=%d bytes=%d lock_ms=%d write_ms=%d active=%d err=%v", s.sid, frameTypeName(typ), streamID, len(payload), lockWait.Milliseconds(), writeDur.Milliseconds(), active, err)
	}
	return err
}

func (s *MuxSession) nextStream() uint32 {
	return atomic.AddUint32(&s.nextStreamID, 2)
}

func (s *MuxSession) registerStream(stream *MuxConn) {
	s.mu.Lock()
	s.streams[stream.id] = stream
	s.mu.Unlock()
}

func (s *MuxSession) removeStream(streamID uint32) {
	s.mu.Lock()
	delete(s.streams, streamID)
	s.mu.Unlock()
}

func (s *MuxSession) getStream(streamID uint32) *MuxConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[streamID]
}

func (s *MuxSession) readLoop(gen uint64, conn net.Conn) {
	for {
		fr, err := readFrame(conn)
		if err != nil {
			s.closeSession(gen, err)
			return
		}
		atomic.AddUint64(&s.framesIn, 1)
		atomic.AddUint64(&s.bytesIn, uint64(len(fr.payload)))
		atomic.StoreInt64(&s.lastFrameInUnix, time.Now().Unix())

		switch fr.typ {
		case frameHelloAck:
			s.mu.Lock()
			if gen == s.sessionGen && s.conn == conn {
				s.ready = true
			}
			s.mu.Unlock()
			logEvent("[IR] session=%s ready", s.sid)
		case frameOpenOK:
			stream := s.getStream(fr.streamID)
			if stream != nil {
				stream.markOpenReady()
				logEvent("[IR] session=%s stream=%d open_ok active=%d", s.sid, fr.streamID, s.activeCount())
			}
		case frameOpenErr:
			stream := s.getStream(fr.streamID)
			if stream != nil {
				stream.markOpenErr(fmt.Errorf("open failed: %s", string(fr.payload)))
				s.removeStream(fr.streamID)
				logEvent("[IR] session=%s stream=%d open_err detail=%q", s.sid, fr.streamID, string(fr.payload))
			}
		case frameData:
			stream := s.getStream(fr.streamID)
			if stream != nil {
				stream.enqueueData(fr.payload)
			}
		case frameClose:
			stream := s.getStream(fr.streamID)
			if stream != nil {
				stream.markRemoteClosed()
				s.removeStream(fr.streamID)
			}
		default:
			s.closeSession(gen, fmt.Errorf("unexpected frame type=%d", fr.typ))
			return
		}
	}
}

func (s *MuxSession) statsLoop() {
	ticker := time.NewTicker(sessionStatsInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		connected := s.conn != nil
		ready := s.ready
		active := len(s.streams)
		running := s.requestRunning
		s.mu.Unlock()
		logEvent(
			"[IR] session=%s stats connected=%t ready=%t request_running=%t active=%d frames_in=%d frames_out=%d bytes_in=%d bytes_out=%d slow_writes=%d last_in=%d last_out=%d",
			s.sid,
			connected,
			ready,
			running,
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

func (s *MuxSession) dialStream(ctx context.Context, host, port string) (net.Conn, error) {
	portNum, err := parsePort(port)
	if err != nil {
		return nil, err
	}

	streamID := s.nextStream()
	stream := newMuxConn(s, streamID)
	s.registerStream(stream)

	if err := s.sendFrame(frameOpen, streamID, encodeOpenPayload(host, portNum)); err != nil {
		s.removeStream(streamID)
		return nil, err
	}

	waitCtx, cancel := context.WithTimeout(ctx, *openTimeout)
	defer cancel()
	if err := stream.waitForOpen(waitCtx); err != nil {
		s.removeStream(streamID)
		return nil, err
	}

	go stream.pumpWrites()
	logEvent("[IR] session=%s stream=%d new_tunnel target=%s:%s active=%d", s.sid, streamID, host, port, s.activeCount())
	return stream, nil
}

func (s *MuxSession) requestSession() {
	s.mu.Lock()
	if s.requestRunning || s.conn != nil {
		s.mu.Unlock()
		return
	}
	s.requestRunning = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.requestRunning = false
		s.mu.Unlock()
	}()

	values := url.Values{}
	values.Set("action", "session")
	values.Set("sid", s.sid)
	values.Set("iran_host", *publicHost)
	values.Set("iran_port", fmt.Sprintf("%d", *publicPort))
	values.Set("outer_host", *outerHost)
	values.Set("outer_port", fmt.Sprintf("%d", *outerPort))

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, *relayURL+"?"+values.Encode(), nil)
	if err != nil {
		logEvent("[IR] session=%s request_build_failed err=%v", s.sid, err)
		return
	}
	req.Header.Set("X-API-KEY", *apiKey)

	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		logEvent("[IR] session=%s request_failed dur_ms=%d err=%v", s.sid, time.Since(start).Milliseconds(), err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		logEvent("[IR] session=%s request_rejected status=%d body=%q", s.sid, resp.StatusCode, strings.TrimSpace(string(body)))
		return
	}

	logEvent("[IR] session=%s request_ok dur_ms=%d", s.sid, time.Since(start).Milliseconds())
	_, _ = io.Copy(io.Discard, resp.Body)
	logEvent("[IR] session=%s request_body_closed", s.sid)
}

func (s *MuxSession) loop() {
	for {
		s.mu.Lock()
		connected := s.conn != nil
		running := s.requestRunning
		s.mu.Unlock()

		if !connected && !running {
			go s.requestSession()
		}
		time.Sleep(sessionPollDelay)
	}
}

type SessionPool struct {
	sessions map[string]*MuxSession
	order    []*MuxSession
	rr       uint32
}

func newSessionPool(count int) *SessionPool {
	p := &SessionPool{
		sessions: make(map[string]*MuxSession, count),
		order:    make([]*MuxSession, 0, count),
	}
	for i := 0; i < count; i++ {
		sid := fmt.Sprintf("sess_%d", i+1)
		s := newMuxSession(p, i, sid)
		p.sessions[sid] = s
		p.order = append(p.order, s)
	}
	return p
}

func (p *SessionPool) start() {
	for _, s := range p.order {
		go s.loop()
		go s.statsLoop()
	}
}

func (p *SessionPool) totalActive() int {
	total := 0
	for _, s := range p.order {
		total += s.activeCount()
	}
	return total
}

func (p *SessionPool) getByID(sid string) *MuxSession {
	return p.sessions[sid]
}

func (p *SessionPool) chooseReadySession(ctx context.Context) (*MuxSession, error) {
	waitStart := time.Now()
	for {
		var ready []*MuxSession
		for _, s := range p.order {
			if s.isReady() {
				ready = append(ready, s)
			}
		}
		if len(ready) > 0 {
			idx := atomic.AddUint32(&p.rr, 1)
			chosen := ready[int(idx)%len(ready)]
			waited := time.Since(waitStart)
			if waited > time.Second {
				logEvent("[IR] choose_ready_session waited_ms=%d ready=%d chosen=%s total_active=%d", waited.Milliseconds(), len(ready), chosen.sid, p.totalActive())
			}
			return chosen, nil
		}

		select {
		case <-ctx.Done():
			logEvent("[IR] choose_ready_session failed waited_ms=%d err=%v total_active=%d", time.Since(waitStart).Milliseconds(), ctx.Err(), p.totalActive())
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (p *SessionPool) dial(ctx context.Context, host, port string) (net.Conn, error) {
	session, err := p.chooseReadySession(ctx)
	if err != nil {
		return nil, err
	}
	return session.dialStream(ctx, host, port)
}

func handleAgentConn(pool *SessionPool, conn net.Conn) {
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	setTCPOptions(conn)
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))

	line, err := readLine(conn, 1024)
	if err != nil {
		logEvent("[IR] attach handshake_read_failed remote=%s err=%v", conn.RemoteAddr(), err)
		return
	}

	parts := strings.Fields(line)
	if len(parts) != 3 || parts[0] != "SESSION" {
		_ = writeString(conn, "ERR bad-handshake\n")
		logEvent("[IR] attach bad_handshake remote=%s line=%q", conn.RemoteAddr(), line)
		return
	}
	if parts[1] != *apiKey {
		_ = writeString(conn, "ERR unauthorized\n")
		logEvent("[IR] attach unauthorized remote=%s", conn.RemoteAddr())
		return
	}

	session := pool.getByID(parts[2])
	if session == nil {
		_ = writeString(conn, "ERR unknown-session\n")
		logEvent("[IR] attach unknown_session sid=%s remote=%s", parts[2], conn.RemoteAddr())
		return
	}

	_ = conn.SetDeadline(time.Time{})
	if err := writeString(conn, "OK\n"); err != nil {
		logEvent("[IR] attach ack_failed sid=%s err=%v", session.sid, err)
		return
	}

	session.attachConn(conn)
	conn = nil
}

func runAgentListener(pool *SessionPool) error {
	ln, err := net.Listen("tcp", *agentListen)
	if err != nil {
		return err
	}
	log.Printf("[IR-SOCK] agent listener on %s", *agentListen)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[IR-SOCK] accept error: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		go handleAgentConn(pool, conn)
	}
}

func main() {
	flag.Parse()

	if *publicHost == "" {
		log.Fatal("--public-host is required")
	}
	if *outerHost == "" {
		log.Fatal("--outer-host is required")
	}
	if *sessionCount < 1 {
		log.Fatal("--session-count must be >= 1")
	}

	if *logFilePath != "" {
		logFile, err := os.OpenFile(*logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("failed to open log file %s: %v", *logFilePath, err)
		}
		logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
		log.Printf("[IR-SOCK] debug log file: %s", *logFilePath)
	}

	httpClient.Transport.(*http.Transport).ResponseHeaderTimeout = *openTimeout
	pool := newSessionPool(*sessionCount)
	logEvent("[IR] startup relay=%s listen=%s:%s agent=%s public=%s:%d outer=%s:%d open_timeout=%s session_count=%d max_relays_compat=%d", *relayURL, *listenIP, *listenPort, *agentListen, *publicHost, *publicPort, *outerHost, *outerPort, openTimeout.String(), *sessionCount, *maxRelays)

	go func() {
		if err := runAgentListener(pool); err != nil {
			log.Fatalf("[IR-SOCK] agent listener failed: %v", err)
		}
	}()
	pool.start()

	conf := &socks5.Config{
		Resolver: newUpstreamResolver(),
		Rewriter: preserveFQDNRewriter{},
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, targetPort, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			return pool.dial(ctx, host, targetPort)
		},
	}

	server, err := socks5.New(conf)
	if err != nil {
		log.Fatalf("failed to create SOCKS5 server: %v", err)
	}

	addr := net.JoinHostPort(*listenIP, *listenPort)
	log.Printf("[IR-SOCK] SOCKS5 listening on %s", addr)
	if err := server.ListenAndServe("tcp", addr); err != nil {
		log.Fatalf("SOCKS5 serve failed: %v", err)
	}
}
