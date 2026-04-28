package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
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
	framePing
	framePong
)

const (
	frameHeaderSize        = 9
	maxFramePayload        = 128 * 1024
	minAdaptiveFrameSize   = 32 * 1024
	midAdaptiveFrameSize   = 64 * 1024
	sessionPollDelay       = 1200 * time.Millisecond
	streamWriteQueueDepth  = 32
	controlWriteQueueDepth = 256
	slowWriteThreshold     = 200 * time.Millisecond
	sessionStatsInterval   = 15 * time.Second
	reconnectBackoffMin    = 1 * time.Second
	reconnectBackoffMax    = 30 * time.Second
	adminReadHeaderTimeout = 5 * time.Second
	pooledFrameThreshold   = 32 * 1024
	maxReservedSessions    = 2
	heartbeatInterval      = 15 * time.Second
	heartbeatTimeout       = 45 * time.Second
	recentSlowPenaltyAge   = 30 * time.Second
)

var (
	configPath      = flag.String("c", "config.client.json", "Path to client config JSON")
	showVersion     = flag.Bool("version", false, "Print version and exit")
	relayURL        string
	apiKey          string
	socksListen     string
	agentListen     string
	publicHost      string
	publicPort      int
	serverHost      string
	serverPort      int
	adminListen     string
	openTimeout     time.Duration
	keepalivePeriod time.Duration
	sessionCount    int
	logFilePath     string
)

var (
	appVersion   = "dev"
	appCommit    = "unknown"
	appBuildDate = "unknown"
)

type clientConfigFile struct {
	RelayURL     string `json:"relay_url"`
	APIKey       string `json:"api_key"`
	SOCKSListen  string `json:"socks_listen"`
	AgentListen  string `json:"agent_listen"`
	PublicHost   string `json:"public_host"`
	PublicPort   int    `json:"public_port"`
	ServerHost   string `json:"server_host"`
	ServerPort   int    `json:"server_port"`
	AdminListen  string `json:"admin_listen"`
	OpenTimeout  string `json:"open_timeout"`
	Keepalive    string `json:"keepalive"`
	SessionCount int    `json:"session_count"`
	LogFile      string `json:"log_file"`
}

func defaultClientConfig() clientConfigFile {
	return clientConfigFile{
		RelayURL:     "https://hidaco.site/tools/rel/soc/furo-relay.php",
		APIKey:       "my_super_secret_123456789",
		SOCKSListen:  "0.0.0.0:18713",
		AgentListen:  "0.0.0.0:28080",
		PublicPort:   28080,
		ServerPort:   28081,
		AdminListen:  "",
		OpenTimeout:  "45s",
		Keepalive:    "30s",
		SessionCount: 8,
		LogFile:      "",
	}
}

func loadClientConfig(path string) error {
	cfg := defaultClientConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	if cfg.RelayURL == "" {
		return errors.New("relay_url is required")
	}
	if cfg.APIKey == "" {
		return errors.New("api_key is required")
	}
	if cfg.SOCKSListen == "" {
		return errors.New("socks_listen is required")
	}
	if cfg.AgentListen == "" {
		return errors.New("agent_listen is required")
	}
	if cfg.PublicHost == "" {
		return errors.New("public_host is required")
	}
	if cfg.PublicPort < 1 || cfg.PublicPort > 65535 {
		return errors.New("public_port must be between 1 and 65535")
	}
	if cfg.ServerHost == "" {
		return errors.New("server_host is required")
	}
	if cfg.ServerPort < 1 || cfg.ServerPort > 65535 {
		return errors.New("server_port must be between 1 and 65535")
	}
	if cfg.SessionCount < 1 {
		return errors.New("session_count must be >= 1")
	}

	parsedOpenTimeout, err := time.ParseDuration(cfg.OpenTimeout)
	if err != nil {
		return fmt.Errorf("parse open_timeout: %w", err)
	}
	parsedKeepalive, err := time.ParseDuration(cfg.Keepalive)
	if err != nil {
		return fmt.Errorf("parse keepalive: %w", err)
	}

	relayURL = cfg.RelayURL
	apiKey = cfg.APIKey
	socksListen = cfg.SOCKSListen
	agentListen = cfg.AgentListen
	publicHost = cfg.PublicHost
	publicPort = cfg.PublicPort
	serverHost = cfg.ServerHost
	serverPort = cfg.ServerPort
	adminListen = cfg.AdminListen
	openTimeout = parsedOpenTimeout
	keepalivePeriod = parsedKeepalive
	sessionCount = cfg.SessionCount
	logFilePath = cfg.LogFile
	return nil
}

var (
	logMu           sync.Mutex
	logger          = log.New(io.Discard, "", log.LstdFlags|log.Lmicroseconds)
	clientStartedAt = time.Now()

	relayRequestsStarted   uint64
	relayRequestsSucceeded uint64
	relayRequestsFailed    uint64
	relayRequestsRejected  uint64

	httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   16,
			MaxConnsPerHost:       16,
			IdleConnTimeout:       30 * time.Second,
			DisableCompression:    true,
			DisableKeepAlives:     false,
			ForceAttemptHTTP2:     false,
			ResponseHeaderTimeout: 45 * time.Second,
			TLSHandshakeTimeout:   15 * time.Second,
		},
	}

	largeFramePool = sync.Pool{
		New: func() any {
			return make([]byte, maxFramePayload)
		},
	}
)

func logEvent(format string, args ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	logger.Printf(format, args...)
}

func clientVersionString() string {
	return fmt.Sprintf("furo-client version=%s commit=%s built=%s", appVersion, appCommit, appBuildDate)
}

func allocateFramePayload(src []byte) ([]byte, func()) {
	if len(src) >= pooledFrameThreshold {
		buf := largeFramePool.Get().([]byte)
		payload := buf[:len(src)]
		copy(payload, src)
		return payload, func() {
			largeFramePool.Put(buf)
		}
	}

	payload := make([]byte, len(src))
	copy(payload, src)
	return payload, nil
}

func adaptiveFramePayload(activeStreams int, pendingBytes int64) int {
	switch {
	case activeStreams >= 6 || pendingBytes >= 512*1024:
		return minAdaptiveFrameSize
	case activeStreams >= 2 || pendingBytes >= 128*1024:
		return midAdaptiveFrameSize
	default:
		return maxFramePayload
	}
}

func computeReconnectDelay(failures int) time.Duration {
	if failures <= 0 {
		return 0
	}

	delay := reconnectBackoffMin
	for attempt := 1; attempt < failures; attempt++ {
		if delay >= reconnectBackoffMax {
			return reconnectBackoffMax
		}
		delay *= 2
		if delay > reconnectBackoffMax {
			return reconnectBackoffMax
		}
	}
	return delay
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
		return ctx, net.IPv4zero, err
	}
	for _, addr := range ipAddrs {
		if v4 := addr.IP.To4(); v4 != nil {
			return ctx, v4, nil
		}
	}
	if len(ipAddrs) > 0 {
		return ctx, ipAddrs[0].IP, nil
	}
	return ctx, net.IPv4zero, fmt.Errorf("no addresses found for %s", name)
}

func setTCPOptions(conn net.Conn) {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcp.SetKeepAlive(true)
	_ = tcp.SetKeepAlivePeriod(keepalivePeriod)
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
	case framePing:
		return "ping"
	case framePong:
		return "pong"
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
	openSignal      chan struct{}
	openSignalOnce  sync.Once
	openReady       bool
	openErr         error
	readQ           [][]byte
	readBuf         []byte
	closed          bool
	remoteClosed    bool
	done            chan struct{}
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
	c.openSignal = make(chan struct{})
	c.done = make(chan struct{})
	c.startedAt = time.Now()
	return c
}

func (c *MuxConn) waitForOpen(ctx context.Context) error {
	for {
		c.mu.Lock()
		openSignal := c.openSignal
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
		case <-openSignal:
		}
	}
}

func (c *MuxConn) signalOpenState() {
	c.openSignalOnce.Do(func() {
		close(c.openSignal)
	})
}

func (c *MuxConn) markOpenReady() {
	c.mu.Lock()
	if !c.closed && c.openErr == nil {
		c.openReady = true
		c.cond.Broadcast()
	}
	c.mu.Unlock()
	c.signalOpenState()
}

func (c *MuxConn) markOpenErr(err error) {
	c.mu.Lock()
	if !c.closed && c.openErr == nil {
		c.openErr = err
		c.cond.Broadcast()
	}
	c.mu.Unlock()
	c.signalOpenState()
}

func (c *MuxConn) enqueueData(payload []byte) {
	if len(payload) == 0 {
		return
	}
	atomic.AddUint64(&c.bytesFromOuter, uint64(len(payload)))
	atomic.AddUint64(&c.framesFromOuter, 1)
	c.mu.Lock()
	if !c.closed {
		c.readQ = append(c.readQ, payload)
		c.cond.Broadcast()
		if depth := len(c.readQ); depth >= streamWriteQueueDepth/2 && (depth == streamWriteQueueDepth/2 || depth == streamWriteQueueDepth) {
			logEvent("[CLIENT] session=%s stream=%d readq_depth=%d bytes_from_outer=%d", c.session.sid, c.id, depth, atomic.LoadUint64(&c.bytesFromOuter))
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
	c.signalOpenState()
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
	c.signalOpenState()
	logEvent("[CLIENT] session=%s stream=%d fail err=%v", c.session.sid, c.id, err)
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
		chunkLen := c.session.recommendedChunkSize()
		if chunkLen > len(b) {
			chunkLen = len(b)
		}
		payload, release := allocateFramePayload(b[:chunkLen])
		if err := c.session.sendData(c.id, payload, release); err != nil {
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
	c.signalOpenState()
	c.session.removeStream(c.id)
	_ = c.session.sendControl(frameClose, c.id, nil)
	c.logSummary("local-close")
	return nil
}

func (c *MuxConn) logSummary(reason string) {
	c.summaryOnce.Do(func() {
		logEvent(
			"[CLIENT] session=%s stream=%d summary reason=%s age_ms=%d bytes_client=%d bytes_outer=%d frames_client=%d frames_outer=%d",
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

type outboundFrame struct {
	typ        byte
	streamID   uint32
	payload    []byte
	enqueuedAt time.Time
	result     chan error
	release    func()
}

func (f *outboundFrame) finish(err error) {
	if f.release != nil {
		f.release()
		f.release = nil
	}
	f.result <- err
}

type MuxSession struct {
	pool *SessionPool
	sid  string
	idx  int

	mu                sync.Mutex
	conn              net.Conn
	ready             bool
	requestRunning    bool
	streams           map[uint32]*MuxConn
	nextStreamID      uint32
	sessionGen        uint64
	loopWake          chan struct{}
	writeStop         chan struct{}
	controlQ          chan *outboundFrame
	wakeWriter        chan struct{}
	schedMu           sync.Mutex
	dataQueues        map[uint32][]*outboundFrame
	readyStreams      []uint32
	framesIn          uint64
	framesOut         uint64
	bytesIn           uint64
	bytesOut          uint64
	slowWrites        uint64
	lastSlowWriteUnix int64
	lastFrameInUnix   int64
	lastFrameOutUnix  int64
	pendingBytes      int64
	pendingFrames     int64
	requestFailures   int
	nextRetryAt       time.Time
	retryDelay        time.Duration
	lastRequestErr    string
}

func newMuxSession(pool *SessionPool, idx int, sid string) *MuxSession {
	return &MuxSession{
		pool:       pool,
		idx:        idx,
		sid:        sid,
		streams:    make(map[uint32]*MuxConn),
		loopWake:   make(chan struct{}, 1),
		controlQ:   make(chan *outboundFrame, controlWriteQueueDepth),
		wakeWriter: make(chan struct{}, 1),
		dataQueues: make(map[uint32][]*outboundFrame),
	}
}

func (s *MuxSession) activeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

func (s *MuxSession) notifyLoop() {
	select {
	case s.loopWake <- struct{}{}:
	default:
	}
}

func (s *MuxSession) recommendedChunkSize() int {
	s.mu.Lock()
	active := len(s.streams)
	s.mu.Unlock()
	return adaptiveFramePayload(active, atomic.LoadInt64(&s.pendingBytes))
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
	oldWriteStop := s.writeStop
	s.conn = conn
	s.ready = false
	s.sessionGen++
	gen := s.sessionGen
	s.writeStop = make(chan struct{})
	writeStop := s.writeStop
	s.mu.Unlock()
	now := time.Now().Unix()
	atomic.StoreInt64(&s.lastFrameInUnix, now)
	atomic.StoreInt64(&s.lastFrameOutUnix, now)

	if oldWriteStop != nil {
		close(oldWriteStop)
	}
	if oldConn != nil {
		_ = oldConn.Close()
	}
	if oldConn != nil || len(oldStreams) > 0 {
		s.failPending(errors.New("session replaced"))
	}
	for _, stream := range oldStreams {
		stream.fail(errors.New("session replaced"))
	}

	logEvent("[CLIENT] session=%s accepted remote=%s", s.sid, conn.RemoteAddr())
	go s.writeLoop(gen, conn, writeStop)
	go s.readLoop(gen, conn)
	go s.heartbeatLoop(gen, writeStop)
	if err := s.sendHello(gen); err != nil {
		logEvent("[CLIENT] session=%s hello_failed err=%v", s.sid, err)
		s.closeSession(gen, err)
	}
	s.pool.notifyStateChange()
	s.notifyLoop()
}

func (s *MuxSession) closeSession(gen uint64, err error) {
	s.mu.Lock()
	if gen != 0 && gen != s.sessionGen {
		s.mu.Unlock()
		return
	}
	conn := s.conn
	streams := s.snapshotAndResetStreamsLocked()
	writeStop := s.writeStop
	s.writeStop = nil
	s.conn = nil
	s.ready = false
	s.sessionGen++
	s.mu.Unlock()

	if writeStop != nil {
		close(writeStop)
	}
	if conn != nil {
		_ = conn.Close()
	}
	s.failPending(err)
	for _, stream := range streams {
		stream.fail(err)
	}
	logEvent("[CLIENT] session=%s closed err=%v", s.sid, err)
	s.pool.notifyStateChange()
	s.notifyLoop()
}

func (s *MuxSession) sendHello(gen uint64) error {
	s.mu.Lock()
	if gen != s.sessionGen || s.conn == nil {
		s.mu.Unlock()
		return errors.New("no live session")
	}
	s.mu.Unlock()

	return s.sendControl(frameHello, 0, []byte(apiKey))
}

func (s *MuxSession) sendControl(typ byte, streamID uint32, payload []byte) error {
	s.mu.Lock()
	ready := s.ready
	connected := s.conn != nil
	stop := s.writeStop
	s.mu.Unlock()

	if !connected {
		return errors.New("session not connected")
	}
	if typ != frameHello && !ready {
		return errors.New("session not ready")
	}

	req := &outboundFrame{
		typ:        typ,
		streamID:   streamID,
		payload:    payload,
		enqueuedAt: time.Now(),
		result:     make(chan error, 1),
	}

	select {
	case <-stop:
		return net.ErrClosed
	case s.controlQ <- req:
	}

	return <-req.result
}

func (s *MuxSession) sendData(streamID uint32, payload []byte, release func()) error {
	s.mu.Lock()
	ready := s.ready
	connected := s.conn != nil
	s.mu.Unlock()

	if !connected {
		if release != nil {
			release()
		}
		return errors.New("session not connected")
	}
	if !ready {
		if release != nil {
			release()
		}
		return errors.New("session not ready")
	}

	req := &outboundFrame{
		typ:        frameData,
		streamID:   streamID,
		payload:    payload,
		enqueuedAt: time.Now(),
		result:     make(chan error, 1),
		release:    release,
	}
	if err := s.enqueueDataFrame(req); err != nil {
		if release != nil {
			release()
		}
		return err
	}
	return <-req.result
}

func (s *MuxSession) enqueueDataFrame(req *outboundFrame) error {
	s.schedMu.Lock()
	if s.conn == nil {
		s.schedMu.Unlock()
		return net.ErrClosed
	}
	if len(s.dataQueues[req.streamID]) == 0 {
		s.readyStreams = append(s.readyStreams, req.streamID)
	}
	s.dataQueues[req.streamID] = append(s.dataQueues[req.streamID], req)
	atomic.AddInt64(&s.pendingBytes, int64(len(req.payload)))
	atomic.AddInt64(&s.pendingFrames, 1)
	depth := len(s.dataQueues[req.streamID])
	s.schedMu.Unlock()

	if depth >= streamWriteQueueDepth/2 && (depth == streamWriteQueueDepth/2 || depth == streamWriteQueueDepth) {
		logEvent("[CLIENT] session=%s stream=%d writeq_depth=%d pending_bytes=%d", s.sid, req.streamID, depth, atomic.LoadInt64(&s.pendingBytes))
	}
	select {
	case s.wakeWriter <- struct{}{}:
	default:
	}
	s.pool.notifyStateChange()
	return nil
}

func (s *MuxSession) popDataFrame() *outboundFrame {
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
	atomic.AddInt64(&s.pendingBytes, -int64(len(req.payload)))
	atomic.AddInt64(&s.pendingFrames, -1)
	return req
}

func (s *MuxSession) nextFrame(stop <-chan struct{}) (*outboundFrame, bool) {
	for {
		select {
		case req := <-s.controlQ:
			return req, true
		default:
		}
		if req := s.popDataFrame(); req != nil {
			s.pool.notifyStateChange()
			return req, true
		}

		select {
		case <-stop:
			return nil, false
		case req := <-s.controlQ:
			return req, true
		case <-s.wakeWriter:
		}
	}
}

func (s *MuxSession) writeLoop(gen uint64, conn net.Conn, stop <-chan struct{}) {
	for {
		req, ok := s.nextFrame(stop)
		if !ok {
			return
		}

		s.mu.Lock()
		currentConn := s.conn
		active := len(s.streams)
		currentGen := s.sessionGen
		s.mu.Unlock()

		if currentGen != gen || currentConn != conn || conn == nil {
			req.finish(net.ErrClosed)
			return
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
			atomic.StoreInt64(&s.lastSlowWriteUnix, time.Now().Unix())
			logEvent("[CLIENT] session=%s slow_send type=%s stream=%d bytes=%d queue_ms=%d write_ms=%d active=%d err=%v", s.sid, frameTypeName(req.typ), req.streamID, len(req.payload), queueWait.Milliseconds(), writeDur.Milliseconds(), active, err)
		}
		req.finish(err)
		if err != nil {
			s.closeSession(gen, err)
			return
		}
	}
}

func (s *MuxSession) failPending(err error) {
	for {
		select {
		case req := <-s.controlQ:
			req.finish(err)
		default:
			goto drainData
		}
	}

drainData:
	s.schedMu.Lock()
	defer s.schedMu.Unlock()
	for streamID, queue := range s.dataQueues {
		for _, req := range queue {
			req.finish(err)
		}
		delete(s.dataQueues, streamID)
	}
	s.readyStreams = nil
	atomic.StoreInt64(&s.pendingBytes, 0)
	atomic.StoreInt64(&s.pendingFrames, 0)
}

func (s *MuxSession) nextStream() uint32 {
	return atomic.AddUint32(&s.nextStreamID, 2)
}

func (s *MuxSession) registerStream(stream *MuxConn) {
	s.mu.Lock()
	s.streams[stream.id] = stream
	s.mu.Unlock()
	s.pool.notifyStateChange()
}

func (s *MuxSession) removeStream(streamID uint32) {
	s.mu.Lock()
	delete(s.streams, streamID)
	s.mu.Unlock()
	s.pool.notifyStateChange()
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
			logEvent("[CLIENT] session=%s ready", s.sid)
			s.pool.notifyStateChange()
		case frameOpenOK:
			stream := s.getStream(fr.streamID)
			if stream != nil {
				stream.markOpenReady()
				logEvent("[CLIENT] session=%s stream=%d open_ok active=%d", s.sid, fr.streamID, s.activeCount())
			}
		case frameOpenErr:
			stream := s.getStream(fr.streamID)
			if stream != nil {
				stream.markOpenErr(fmt.Errorf("open failed: %s", string(fr.payload)))
				s.removeStream(fr.streamID)
				logEvent("[CLIENT] session=%s stream=%d open_err detail=%q", s.sid, fr.streamID, string(fr.payload))
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
		case framePing:
			if err := s.sendControl(framePong, 0, nil); err != nil && !errors.Is(err, net.ErrClosed) {
				logEvent("[CLIENT] session=%s pong_failed err=%v", s.sid, err)
				s.closeSession(gen, err)
				return
			}
		case framePong:
		default:
			s.closeSession(gen, fmt.Errorf("unexpected frame type=%d", fr.typ))
			return
		}
	}
}

func (s *MuxSession) heartbeatLoop(gen uint64, stop <-chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval / 2)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}

		s.mu.Lock()
		connected := s.conn != nil
		currentGen := s.sessionGen
		s.mu.Unlock()
		if !connected || currentGen != gen {
			return
		}

		now := time.Now()
		lastIn := time.Unix(atomic.LoadInt64(&s.lastFrameInUnix), 0)
		if now.Sub(lastIn) > heartbeatTimeout {
			s.closeSession(gen, errors.New("heartbeat timeout"))
			return
		}

		lastOut := time.Unix(atomic.LoadInt64(&s.lastFrameOutUnix), 0)
		if now.Sub(lastOut) >= heartbeatInterval {
			if err := s.sendControl(framePing, 0, nil); err != nil && !errors.Is(err, net.ErrClosed) {
				logEvent("[CLIENT] session=%s heartbeat_ping_failed err=%v", s.sid, err)
				s.closeSession(gen, err)
				return
			}
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
			"[CLIENT] session=%s stats connected=%t ready=%t request_running=%t active=%d request_failures=%d retry_delay_ms=%d frames_in=%d frames_out=%d bytes_in=%d bytes_out=%d slow_writes=%d last_in=%d last_out=%d",
			s.sid,
			connected,
			ready,
			running,
			active,
			s.requestFailures,
			s.retryDelay.Milliseconds(),
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

func (s *MuxSession) markRequestSuccess() {
	s.mu.Lock()
	s.requestFailures = 0
	s.nextRetryAt = time.Time{}
	s.retryDelay = 0
	s.lastRequestErr = ""
	s.mu.Unlock()
	s.notifyLoop()
}

func (s *MuxSession) markRequestFailure(reason string) time.Duration {
	s.mu.Lock()
	s.requestFailures++
	s.retryDelay = computeReconnectDelay(s.requestFailures)
	s.nextRetryAt = time.Now().Add(s.retryDelay)
	s.lastRequestErr = reason
	delay := s.retryDelay
	s.mu.Unlock()
	s.notifyLoop()
	return delay
}

func (s *MuxSession) retryWait(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextRetryAt.IsZero() || !now.Before(s.nextRetryAt) {
		return 0
	}
	return s.nextRetryAt.Sub(now)
}

func (s *MuxSession) dialStream(ctx context.Context, host, port string) (net.Conn, error) {
	portNum, err := parsePort(port)
	if err != nil {
		return nil, err
	}

	streamID := s.nextStream()
	stream := newMuxConn(s, streamID)
	s.registerStream(stream)

	if err := s.sendControl(frameOpen, streamID, encodeOpenPayload(host, portNum)); err != nil {
		s.removeStream(streamID)
		return nil, err
	}

	waitCtx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()
	if err := stream.waitForOpen(waitCtx); err != nil {
		s.removeStream(streamID)
		return nil, err
	}

	logEvent("[CLIENT] session=%s stream=%d new_tunnel target=%s:%s active=%d", s.sid, streamID, host, port, s.activeCount())
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
		s.notifyLoop()
	}()

	values := url.Values{}
	values.Set("action", "session")
	values.Set("sid", s.sid)
	values.Set("client_host", publicHost)
	values.Set("client_port", fmt.Sprintf("%d", publicPort))
	values.Set("server_host", serverHost)
	values.Set("server_port", fmt.Sprintf("%d", serverPort))

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, relayURL+"?"+values.Encode(), nil)
	if err != nil {
		delay := s.markRequestFailure(err.Error())
		atomic.AddUint64(&relayRequestsFailed, 1)
		logEvent("[CLIENT] session=%s request_build_failed next_retry_ms=%d err=%v", s.sid, delay.Milliseconds(), err)
		return
	}
	req.Header.Set("X-API-KEY", apiKey)

	start := time.Now()
	atomic.AddUint64(&relayRequestsStarted, 1)
	resp, err := httpClient.Do(req)
	if err != nil {
		delay := s.markRequestFailure(err.Error())
		atomic.AddUint64(&relayRequestsFailed, 1)
		logEvent("[CLIENT] session=%s request_failed dur_ms=%d next_retry_ms=%d err=%v", s.sid, time.Since(start).Milliseconds(), delay.Milliseconds(), err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		delay := s.markRequestFailure(fmt.Sprintf("relay rejected with status %d", resp.StatusCode))
		atomic.AddUint64(&relayRequestsRejected, 1)
		logEvent("[CLIENT] session=%s request_rejected status=%d next_retry_ms=%d body=%q", s.sid, resp.StatusCode, delay.Milliseconds(), strings.TrimSpace(string(body)))
		return
	}

	s.markRequestSuccess()
	atomic.AddUint64(&relayRequestsSucceeded, 1)
	logEvent("[CLIENT] session=%s request_ok dur_ms=%d", s.sid, time.Since(start).Milliseconds())
	_, _ = io.Copy(io.Discard, resp.Body)
	logEvent("[CLIENT] session=%s request_body_closed", s.sid)
}

func (s *MuxSession) loop() {
	for {
		s.mu.Lock()
		connected := s.conn != nil
		running := s.requestRunning
		s.mu.Unlock()

		retryWait := s.retryWait(time.Now())
		if !connected && !running && retryWait == 0 {
			go s.requestSession()
		}

		sleepFor := sessionPollDelay
		if retryWait > 0 && retryWait < sleepFor {
			sleepFor = retryWait
		}

		timer := time.NewTimer(sleepFor)
		select {
		case <-timer.C:
		case <-s.loopWake:
			if !timer.Stop() {
				<-timer.C
			}
		}
	}
}

type SessionPool struct {
	sessions         map[string]*MuxSession
	order            []*MuxSession
	rr               uint32
	stateChange      chan struct{}
	reservedSessions int
}

type sessionLoadSnapshot struct {
	session           *MuxSession
	activeStreams     int
	pendingBytes      int64
	pendingFrames     int64
	requestFailures   int
	lastSlowWriteUnix int64
}

type clientRelayRequestStats struct {
	Started   uint64 `json:"started"`
	Succeeded uint64 `json:"succeeded"`
	Failed    uint64 `json:"failed"`
	Rejected  uint64 `json:"rejected"`
}

type clientTotals struct {
	ConnectedSessions int    `json:"connected_sessions"`
	ReadySessions     int    `json:"ready_sessions"`
	ActiveStreams     int    `json:"active_streams"`
	PendingFrames     int64  `json:"pending_frames"`
	PendingBytes      int64  `json:"pending_bytes"`
	FramesIn          uint64 `json:"frames_in"`
	FramesOut         uint64 `json:"frames_out"`
	BytesIn           uint64 `json:"bytes_in"`
	BytesOut          uint64 `json:"bytes_out"`
	SlowWrites        uint64 `json:"slow_writes"`
}

type clientSessionStatus struct {
	SessionID        string `json:"session_id"`
	Connected        bool   `json:"connected"`
	Ready            bool   `json:"ready"`
	RequestRunning   bool   `json:"request_running"`
	ActiveStreams    int    `json:"active_streams"`
	RequestFailures  int    `json:"request_failures"`
	RetryDelayMs     int64  `json:"retry_delay_ms"`
	NextRetryAt      string `json:"next_retry_at,omitempty"`
	LastRequestErr   string `json:"last_request_error,omitempty"`
	PendingFrames    int64  `json:"pending_frames"`
	PendingBytes     int64  `json:"pending_bytes"`
	FramesIn         uint64 `json:"frames_in"`
	FramesOut        uint64 `json:"frames_out"`
	BytesIn          uint64 `json:"bytes_in"`
	BytesOut         uint64 `json:"bytes_out"`
	SlowWrites       uint64 `json:"slow_writes"`
	LastFrameInUnix  int64  `json:"last_frame_in_unix"`
	LastFrameOutUnix int64  `json:"last_frame_out_unix"`
}

type clientStatusResponse struct {
	Service       string                  `json:"service"`
	Version       string                  `json:"version"`
	Commit        string                  `json:"commit"`
	BuildDate     string                  `json:"build_date"`
	StartedAt     string                  `json:"started_at"`
	UptimeSec     int64                   `json:"uptime_sec"`
	RelayURL      string                  `json:"relay_url"`
	SOCKSListen   string                  `json:"socks_listen"`
	AgentListen   string                  `json:"agent_listen"`
	AdminListen   string                  `json:"admin_listen,omitempty"`
	SessionCount  int                     `json:"session_count"`
	RelayRequests clientRelayRequestStats `json:"relay_requests"`
	Totals        clientTotals            `json:"totals"`
	Sessions      []clientSessionStatus   `json:"sessions"`
}

func newSessionPool(count int) *SessionPool {
	p := &SessionPool{
		sessions:         make(map[string]*MuxSession, count),
		order:            make([]*MuxSession, 0, count),
		stateChange:      make(chan struct{}, 1),
		reservedSessions: reserveSessionCount(count),
	}
	for i := 0; i < count; i++ {
		sid := fmt.Sprintf("sess_%d", i+1)
		s := newMuxSession(p, i, sid)
		p.sessions[sid] = s
		p.order = append(p.order, s)
	}
	return p
}

func reserveSessionCount(count int) int {
	var reserved int
	switch {
	case count >= 8:
		reserved = 2
	case count >= 4:
		reserved = 1
	}
	if reserved > maxReservedSessions {
		return maxReservedSessions
	}
	return reserved
}

func (p *SessionPool) notifyStateChange() {
	select {
	case p.stateChange <- struct{}{}:
	default:
	}
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

func (s *MuxSession) snapshot() clientSessionStatus {
	s.mu.Lock()
	connected := s.conn != nil
	ready := s.ready
	running := s.requestRunning
	active := len(s.streams)
	requestFailures := s.requestFailures
	retryDelay := s.retryDelay
	nextRetryAt := s.nextRetryAt
	lastRequestErr := s.lastRequestErr
	s.mu.Unlock()

	status := clientSessionStatus{
		SessionID:        s.sid,
		Connected:        connected,
		Ready:            ready,
		RequestRunning:   running,
		ActiveStreams:    active,
		RequestFailures:  requestFailures,
		RetryDelayMs:     retryDelay.Milliseconds(),
		LastRequestErr:   lastRequestErr,
		PendingFrames:    atomic.LoadInt64(&s.pendingFrames),
		PendingBytes:     atomic.LoadInt64(&s.pendingBytes),
		FramesIn:         atomic.LoadUint64(&s.framesIn),
		FramesOut:        atomic.LoadUint64(&s.framesOut),
		BytesIn:          atomic.LoadUint64(&s.bytesIn),
		BytesOut:         atomic.LoadUint64(&s.bytesOut),
		SlowWrites:       atomic.LoadUint64(&s.slowWrites),
		LastFrameInUnix:  atomic.LoadInt64(&s.lastFrameInUnix),
		LastFrameOutUnix: atomic.LoadInt64(&s.lastFrameOutUnix),
	}
	if !nextRetryAt.IsZero() {
		status.NextRetryAt = nextRetryAt.UTC().Format(time.RFC3339)
	}
	return status
}

func buildClientStatus(pool *SessionPool) clientStatusResponse {
	status := clientStatusResponse{
		Service:      "furo-client",
		Version:      appVersion,
		Commit:       appCommit,
		BuildDate:    appBuildDate,
		StartedAt:    clientStartedAt.UTC().Format(time.RFC3339),
		UptimeSec:    int64(time.Since(clientStartedAt).Seconds()),
		RelayURL:     relayURL,
		SOCKSListen:  socksListen,
		AgentListen:  agentListen,
		AdminListen:  adminListen,
		SessionCount: sessionCount,
		RelayRequests: clientRelayRequestStats{
			Started:   atomic.LoadUint64(&relayRequestsStarted),
			Succeeded: atomic.LoadUint64(&relayRequestsSucceeded),
			Failed:    atomic.LoadUint64(&relayRequestsFailed),
			Rejected:  atomic.LoadUint64(&relayRequestsRejected),
		},
		Sessions: make([]clientSessionStatus, 0, len(pool.order)),
	}

	for _, session := range pool.order {
		snapshot := session.snapshot()
		status.Sessions = append(status.Sessions, snapshot)
		if snapshot.Connected {
			status.Totals.ConnectedSessions++
		}
		if snapshot.Ready {
			status.Totals.ReadySessions++
		}
		status.Totals.ActiveStreams += snapshot.ActiveStreams
		status.Totals.PendingFrames += snapshot.PendingFrames
		status.Totals.PendingBytes += snapshot.PendingBytes
		status.Totals.FramesIn += snapshot.FramesIn
		status.Totals.FramesOut += snapshot.FramesOut
		status.Totals.BytesIn += snapshot.BytesIn
		status.Totals.BytesOut += snapshot.BytesOut
		status.Totals.SlowWrites += snapshot.SlowWrites
	}

	return status
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func runClientAdminServer(pool *SessionPool) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		status := buildClientStatus(pool)
		code := http.StatusOK
		if status.Totals.ReadySessions == 0 {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]any{
			"ok":                 code == http.StatusOK,
			"ready_sessions":     status.Totals.ReadySessions,
			"connected_sessions": status.Totals.ConnectedSessions,
		})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, buildClientStatus(pool))
	})

	server := &http.Server{
		Addr:              adminListen,
		Handler:           mux,
		ReadHeaderTimeout: adminReadHeaderTimeout,
	}

	log.Printf("[FURO-CLIENT] admin listener on %s", adminListen)
	return server.ListenAndServe()
}

func (p *SessionPool) getByID(sid string) *MuxSession {
	return p.sessions[sid]
}

func (s *MuxSession) loadSnapshot() sessionLoadSnapshot {
	s.mu.Lock()
	ready := s.ready && s.conn != nil
	active := len(s.streams)
	requestFailures := s.requestFailures
	s.mu.Unlock()
	if !ready {
		return sessionLoadSnapshot{}
	}

	return sessionLoadSnapshot{
		session:           s,
		activeStreams:     active,
		pendingBytes:      atomic.LoadInt64(&s.pendingBytes),
		pendingFrames:     atomic.LoadInt64(&s.pendingFrames),
		requestFailures:   requestFailures,
		lastSlowWriteUnix: atomic.LoadInt64(&s.lastSlowWriteUnix),
	}
}

func (snap sessionLoadSnapshot) isIdle() bool {
	return snap.activeStreams == 0 && snap.pendingBytes == 0 && snap.pendingFrames == 0
}

func (snap sessionLoadSnapshot) score() int64 {
	score := snap.pendingBytes + int64(snap.activeStreams*adaptiveFramePayload(snap.activeStreams, snap.pendingBytes)) + snap.pendingFrames*1024
	if snap.requestFailures > 0 {
		score += int64(snap.requestFailures) * 64 * 1024
	}
	if snap.lastSlowWriteUnix != 0 && time.Since(time.Unix(snap.lastSlowWriteUnix, 0)) < recentSlowPenaltyAge {
		score += 64 * 1024
	}
	return score
}

func (p *SessionPool) pickLeastLoaded(candidates []sessionLoadSnapshot) *MuxSession {
	if len(candidates) == 0 {
		return nil
	}

	start := 0
	if len(candidates) > 1 {
		start = int(atomic.AddUint32(&p.rr, 1)) % len(candidates)
	}
	best := candidates[start]
	for i := 1; i < len(candidates); i++ {
		candidate := candidates[(start+i)%len(candidates)]
		if candidate.score() < best.score() {
			best = candidate
			continue
		}
		if candidate.score() == best.score() && candidate.activeStreams < best.activeStreams {
			best = candidate
		}
	}
	return best.session
}

func (p *SessionPool) selectReadySession() (*MuxSession, int) {
	ready := make([]sessionLoadSnapshot, 0, len(p.order))
	idleCount := 0
	for _, session := range p.order {
		snapshot := session.loadSnapshot()
		if snapshot.session == nil {
			continue
		}
		if snapshot.isIdle() {
			idleCount++
		}
		ready = append(ready, snapshot)
	}
	if len(ready) == 0 {
		return nil, 0
	}

	if p.reservedSessions > 0 && idleCount > p.reservedSessions {
		warm := make([]sessionLoadSnapshot, 0, len(ready))
		for _, snapshot := range ready {
			if !snapshot.isIdle() {
				warm = append(warm, snapshot)
			}
		}
		if chosen := p.pickLeastLoaded(warm); chosen != nil {
			return chosen, len(ready)
		}
	}

	return p.pickLeastLoaded(ready), len(ready)
}

func (p *SessionPool) chooseReadySession(ctx context.Context) (*MuxSession, error) {
	waitStart := time.Now()
	for {
		chosen, readyCount := p.selectReadySession()
		if chosen != nil {
			waited := time.Since(waitStart)
			if waited > time.Second {
				logEvent("[CLIENT] choose_ready_session waited_ms=%d ready=%d chosen=%s total_active=%d", waited.Milliseconds(), readyCount, chosen.sid, p.totalActive())
			}
			return chosen, nil
		}

		select {
		case <-ctx.Done():
			logEvent("[CLIENT] choose_ready_session failed waited_ms=%d err=%v total_active=%d", time.Since(waitStart).Milliseconds(), ctx.Err(), p.totalActive())
			return nil, ctx.Err()
		case <-p.stateChange:
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
		logEvent("[CLIENT] attach handshake_read_failed remote=%s err=%v", conn.RemoteAddr(), err)
		return
	}

	parts := strings.Fields(line)
	if len(parts) != 3 || parts[0] != "SESSION" {
		_ = writeString(conn, "ERR bad-handshake\n")
		logEvent("[CLIENT] attach bad_handshake remote=%s line=%q", conn.RemoteAddr(), line)
		return
	}
	if parts[1] != apiKey {
		_ = writeString(conn, "ERR unauthorized\n")
		logEvent("[CLIENT] attach unauthorized remote=%s", conn.RemoteAddr())
		return
	}

	session := pool.getByID(parts[2])
	if session == nil {
		_ = writeString(conn, "ERR unknown-session\n")
		logEvent("[CLIENT] attach unknown_session sid=%s remote=%s", parts[2], conn.RemoteAddr())
		return
	}

	_ = conn.SetDeadline(time.Time{})
	if err := writeString(conn, "OK\n"); err != nil {
		logEvent("[CLIENT] attach ack_failed sid=%s err=%v", session.sid, err)
		return
	}

	session.attachConn(conn)
	conn = nil
}

func runAgentListener(pool *SessionPool) error {
	ln, err := net.Listen("tcp", agentListen)
	if err != nil {
		return err
	}
	log.Printf("[FURO-CLIENT] agent listener on %s", agentListen)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[FURO-CLIENT] accept error: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		go handleAgentConn(pool, conn)
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println(clientVersionString())
		return
	}

	if err := loadClientConfig(*configPath); err != nil {
		log.Fatalf("failed to load client config %s: %v", *configPath, err)
	}

	if logFilePath != "" {
		logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("failed to open log file %s: %v", logFilePath, err)
		}
		logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
		log.Printf("[FURO-CLIENT] debug log file: %s", logFilePath)
	}

	httpClient.Transport.(*http.Transport).ResponseHeaderTimeout = openTimeout
	pool := newSessionPool(sessionCount)
	logEvent("[CLIENT] startup version=%s relay=%s socks_listen=%s agent_listen=%s admin_listen=%s public=%s:%d server=%s:%d open_timeout=%s session_count=%d", appVersion, relayURL, socksListen, agentListen, adminListen, publicHost, publicPort, serverHost, serverPort, openTimeout.String(), sessionCount)

	go func() {
		if err := runAgentListener(pool); err != nil {
			log.Fatalf("[FURO-CLIENT] agent listener failed: %v", err)
		}
	}()
	if adminListen != "" {
		go func() {
			if err := runClientAdminServer(pool); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("[FURO-CLIENT] admin server failed: %v", err)
			}
		}()
	}
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

	log.Printf("[FURO-CLIENT] SOCKS5 listening on %s", socksListen)
	if err := server.ListenAndServe("tcp", socksListen); err != nil {
		log.Fatalf("SOCKS5 serve failed: %v", err)
	}
}
