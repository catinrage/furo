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
	"os"
	"sort"
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
	framePing
	framePong
)

const (
	frameHeaderSize        = 9
	defaultMaxFramePayload = 128 * 1024
	defaultMinFramePayload = 32 * 1024
	defaultMidFramePayload = 64 * 1024
	defaultMaxPendingBytes = 4 * 1024 * 1024
	defaultWriteTimeout    = 30 * time.Second
	streamWriteQueueDepth  = 32
	controlWriteQueueDepth = 256
	slowWriteThreshold     = 200 * time.Millisecond
	sessionStatsInterval   = 15 * time.Second
	adminReadHeaderTimeout = 5 * time.Second
	heartbeatInterval      = 15 * time.Second
	heartbeatTimeout       = 45 * time.Second
)

var (
	configPath      = flag.String("c", "config.server.json", "Path to server config JSON")
	showVersion     = flag.Bool("version", false, "Print version and exit")
	apiKey          string
	agentListen     string
	adminListen     string
	dialTimeout     time.Duration
	keepalivePeriod time.Duration
	writeTimeout    = defaultWriteTimeout
	maxSessions     int
	minFramePayload = defaultMinFramePayload
	midFramePayload = defaultMidFramePayload
	maxFramePayload = defaultMaxFramePayload
	maxPendingBytes = int64(defaultMaxPendingBytes)
	logFilePath     string
)

var (
	appVersion   = "dev"
	appCommit    = "unknown"
	appBuildDate = "unknown"
)

type serverConfigFile struct {
	APIKey          string `json:"api_key"`
	AgentListen     string `json:"agent_listen"`
	AdminListen     string `json:"admin_listen"`
	DialTimeout     string `json:"dial_timeout"`
	Keepalive       string `json:"keepalive"`
	WriteTimeout    string `json:"write_timeout"`
	FrameMinSize    int    `json:"frame_min_size"`
	FrameMidSize    int    `json:"frame_mid_size"`
	FrameMaxSize    int    `json:"frame_max_size"`
	MaxPendingBytes int64  `json:"max_pending_bytes"`
	MaxSessions     int    `json:"max_sessions"`
	LogFile         string `json:"log_file"`
}

func defaultServerConfig() serverConfigFile {
	return serverConfigFile{
		APIKey:          "my_super_secret_123456789",
		AgentListen:     "0.0.0.0:28081",
		AdminListen:     "",
		DialTimeout:     "10s",
		Keepalive:       "30s",
		WriteTimeout:    defaultWriteTimeout.String(),
		FrameMinSize:    defaultMinFramePayload,
		FrameMidSize:    defaultMidFramePayload,
		FrameMaxSize:    defaultMaxFramePayload,
		MaxPendingBytes: defaultMaxPendingBytes,
		MaxSessions:     10,
		LogFile:         "",
	}
}

func loadServerConfig(path string) error {
	cfg := defaultServerConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	if cfg.APIKey == "" {
		return errors.New("api_key is required")
	}
	if cfg.AgentListen == "" {
		return errors.New("agent_listen is required")
	}
	if cfg.MaxSessions < 0 {
		return errors.New("max_sessions must be >= 0")
	}

	parsedDialTimeout, err := time.ParseDuration(cfg.DialTimeout)
	if err != nil {
		return fmt.Errorf("parse dial_timeout: %w", err)
	}
	parsedKeepalive, err := time.ParseDuration(cfg.Keepalive)
	if err != nil {
		return fmt.Errorf("parse keepalive: %w", err)
	}
	parsedWriteTimeout, err := time.ParseDuration(cfg.WriteTimeout)
	if err != nil {
		return fmt.Errorf("parse write_timeout: %w", err)
	}
	if err := validateFrameConfig(cfg.FrameMinSize, cfg.FrameMidSize, cfg.FrameMaxSize, cfg.MaxPendingBytes); err != nil {
		return err
	}

	apiKey = cfg.APIKey
	agentListen = cfg.AgentListen
	adminListen = cfg.AdminListen
	dialTimeout = parsedDialTimeout
	keepalivePeriod = parsedKeepalive
	writeTimeout = parsedWriteTimeout
	minFramePayload = cfg.FrameMinSize
	midFramePayload = cfg.FrameMidSize
	maxFramePayload = cfg.FrameMaxSize
	maxPendingBytes = cfg.MaxPendingBytes
	maxSessions = cfg.MaxSessions
	logFilePath = cfg.LogFile
	return nil
}

func validateFrameConfig(minSize, midSize, maxSize int, pendingLimit int64) error {
	switch {
	case minSize < 4096:
		return errors.New("frame_min_size must be >= 4096")
	case midSize < minSize:
		return errors.New("frame_mid_size must be >= frame_min_size")
	case maxSize < midSize:
		return errors.New("frame_max_size must be >= frame_mid_size")
	case maxSize > 1024*1024:
		return errors.New("frame_max_size must be <= 1048576")
	case pendingLimit < int64(maxSize):
		return errors.New("max_pending_bytes must be >= frame_max_size")
	}
	return nil
}

var (
	logMu             sync.Mutex
	logger            = log.New(io.Discard, "", log.LstdFlags|log.Lmicroseconds)
	serverStartedAt   = time.Now()
	activeSessions    atomic.Int32
	acceptedSessions  uint64
	rejectedSessions  uint64
	closedSessions    uint64
	serverRegistry    = newServerSessionRegistry()
	framePayloadPools = []payloadPool{
		{size: 4 * 1024},
		{size: 16 * 1024},
		{size: 32 * 1024},
		{size: 64 * 1024},
		{size: 128 * 1024},
		{size: 256 * 1024},
		{size: 512 * 1024},
		{size: 1024 * 1024},
	}
	outboundFramePool = sync.Pool{
		New: func() any {
			return &outboundFrame{result: make(chan error, 1)}
		},
	}
)

type payloadPool struct {
	size int
	pool sync.Pool
}

func logEvent(format string, args ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	logger.Printf(format, args...)
}

func serverVersionString() string {
	return fmt.Sprintf("furo-server version=%s commit=%s built=%s", appVersion, appCommit, appBuildDate)
}

type serverSessionSnapshot struct {
	SessionID        string `json:"session_id"`
	Connected        bool   `json:"connected"`
	Authenticated    bool   `json:"authenticated"`
	ActiveStreams    int    `json:"active_streams"`
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

type serverStatusResponse struct {
	Service          string                  `json:"service"`
	Version          string                  `json:"version"`
	Commit           string                  `json:"commit"`
	BuildDate        string                  `json:"build_date"`
	StartedAt        string                  `json:"started_at"`
	UptimeSec        int64                   `json:"uptime_sec"`
	AgentListen      string                  `json:"agent_listen"`
	AdminListen      string                  `json:"admin_listen,omitempty"`
	DialTimeout      string                  `json:"dial_timeout"`
	WriteTimeout     string                  `json:"write_timeout"`
	FrameMinSize     int                     `json:"frame_min_size"`
	FrameMidSize     int                     `json:"frame_mid_size"`
	FrameMaxSize     int                     `json:"frame_max_size"`
	MaxPendingBytes  int64                   `json:"max_pending_bytes"`
	MaxSessions      int                     `json:"max_sessions"`
	ActiveSessions   int32                   `json:"active_sessions"`
	AcceptedSessions uint64                  `json:"accepted_sessions"`
	RejectedSessions uint64                  `json:"rejected_sessions"`
	ClosedSessions   uint64                  `json:"closed_sessions"`
	Sessions         []serverSessionSnapshot `json:"sessions"`
}

type serverSessionRegistry struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

func newServerSessionRegistry() *serverSessionRegistry {
	return &serverSessionRegistry{sessions: make(map[string]*Session)}
}

func (r *serverSessionRegistry) add(session *Session) {
	r.mu.Lock()
	r.sessions[session.sid] = session
	r.mu.Unlock()
}

func (r *serverSessionRegistry) remove(session *Session) {
	r.mu.Lock()
	if current := r.sessions[session.sid]; current == session {
		delete(r.sessions, session.sid)
	}
	r.mu.Unlock()
}

func (r *serverSessionRegistry) snapshots() []serverSessionSnapshot {
	r.mu.Lock()
	sessions := make([]*Session, 0, len(r.sessions))
	for _, session := range r.sessions {
		sessions = append(sessions, session)
	}
	r.mu.Unlock()

	snapshots := make([]serverSessionSnapshot, 0, len(sessions))
	for _, session := range sessions {
		snapshots = append(snapshots, session.snapshot())
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].SessionID < snapshots[j].SessionID
	})
	return snapshots
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

func setWriteDeadline(conn net.Conn) {
	if writeTimeout <= 0 {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
}

func clearWriteDeadline(conn net.Conn) {
	if writeTimeout <= 0 {
		return
	}
	_ = conn.SetWriteDeadline(time.Time{})
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

func getPooledPayload(size int) ([]byte, int, bool) {
	for idx := range framePayloadPools {
		if size <= framePayloadPools[idx].size {
			buf, ok := framePayloadPools[idx].pool.Get().([]byte)
			if !ok || cap(buf) < framePayloadPools[idx].size {
				buf = make([]byte, framePayloadPools[idx].size)
			}
			return buf[:size], idx, true
		}
	}
	return make([]byte, size), -1, false
}

func putPooledPayload(idx int, buf []byte) {
	if idx < 0 || idx >= len(framePayloadPools) {
		return
	}
	framePayloadPools[idx].pool.Put(buf[:framePayloadPools[idx].size])
}

func allocateFramePayload(src []byte) ([]byte, func()) {
	payload, poolIdx, pooled := getPooledPayload(len(src))
	copy(payload, src)
	if !pooled {
		return payload, nil
	}
	return payload, func() {
		putPooledPayload(poolIdx, payload)
	}
}

func adaptiveFramePayload(activeStreams int, pendingBytes int64) int {
	switch {
	case activeStreams >= 6 || pendingBytes >= 512*1024:
		return minFramePayload
	case activeStreams >= 2 || pendingBytes >= 128*1024:
		return midFramePayload
	default:
		return maxFramePayload
	}
}

func writeFrame(w io.Writer, typ byte, streamID uint32, payload []byte) error {
	var header [frameHeaderSize]byte
	header[0] = typ
	binary.BigEndian.PutUint32(header[1:5], streamID)
	binary.BigEndian.PutUint32(header[5:9], uint32(len(payload)))
	if err := writeFull(w, header[:]); err != nil {
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
	if n > uint32(maxFramePayload) {
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
		setWriteDeadline(conn)
		n, err := conn.Write(data)
		clearWriteDeadline(conn)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
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
	release    func()
}

func acquireOutboundFrame(typ byte, streamID uint32, payload []byte, release func()) *outboundFrame {
	req := outboundFramePool.Get().(*outboundFrame)
	select {
	case <-req.result:
	default:
	}
	req.typ = typ
	req.streamID = streamID
	req.payload = payload
	req.enqueuedAt = time.Now()
	req.release = release
	return req
}

func releaseOutboundFrame(req *outboundFrame) {
	req.typ = 0
	req.streamID = 0
	req.payload = nil
	req.enqueuedAt = time.Time{}
	req.release = nil
	outboundFramePool.Put(req)
}

func (f *outboundFrame) finish(err error) {
	if f.release != nil {
		f.release()
		f.release = nil
	}
	f.result <- err
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
			logEvent("[SERVER] stream=%d target_writeq_depth=%d bytes_from_relay=%d", t.id, depth, atomic.LoadUint64(&t.bytesFromRelay))
		}
		return nil
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
	pendingOpens     map[uint32]context.CancelFunc
	pendingFrames    int64
	pendingBytes     int64
	framesIn         uint64
	framesOut        uint64
	bytesIn          uint64
	bytesOut         uint64
	slowWrites       uint64
	lastFrameInUnix  int64
	lastFrameOutUnix int64
}

func (s *Session) snapshot() serverSessionSnapshot {
	s.mu.Lock()
	connected := s.conn != nil
	authOK := s.authOK
	active := len(s.streams)
	s.mu.Unlock()

	return serverSessionSnapshot{
		SessionID:        s.sid,
		Connected:        connected,
		Authenticated:    authOK,
		ActiveStreams:    active,
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
}

func newSession(sid string, conn net.Conn) *Session {
	setTCPOptions(conn)
	s := &Session{
		sid:          sid,
		conn:         conn,
		streams:      make(map[uint32]*TargetStream),
		controlQ:     make(chan *outboundFrame, controlWriteQueueDepth),
		wakeWriter:   make(chan struct{}, 1),
		closed:       make(chan struct{}),
		dataQueues:   make(map[uint32][]*outboundFrame),
		pendingOpens: make(map[uint32]context.CancelFunc),
	}
	now := time.Now().Unix()
	atomic.StoreInt64(&s.lastFrameInUnix, now)
	atomic.StoreInt64(&s.lastFrameOutUnix, now)
	go s.writeLoop()
	go s.heartbeatLoop()
	return s
}

func (s *Session) startOpen(streamID uint32) (context.Context, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil, false
	}
	if _, exists := s.streams[streamID]; exists {
		return nil, false
	}
	if _, exists := s.pendingOpens[streamID]; exists {
		return nil, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.pendingOpens[streamID] = cancel
	return ctx, true
}

func (s *Session) finishOpen(streamID uint32) {
	s.mu.Lock()
	cancel := s.pendingOpens[streamID]
	delete(s.pendingOpens, streamID)
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Session) cancelPendingOpen(streamID uint32) bool {
	s.mu.Lock()
	cancel := s.pendingOpens[streamID]
	delete(s.pendingOpens, streamID)
	s.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (s *Session) cancelAllPendingOpens() {
	s.mu.Lock()
	pending := make([]context.CancelFunc, 0, len(s.pendingOpens))
	for streamID, cancel := range s.pendingOpens {
		pending = append(pending, cancel)
		delete(s.pendingOpens, streamID)
	}
	s.mu.Unlock()
	for _, cancel := range pending {
		cancel()
	}
}

func (s *Session) sendFrame(typ byte, streamID uint32, payload []byte) error {
	return s.sendFrameWithRelease(typ, streamID, payload, nil)
}

func (s *Session) sendFrameWithRelease(typ byte, streamID uint32, payload []byte, release func()) error {
	s.mu.Lock()
	conn := s.conn
	authOK := s.authOK
	s.mu.Unlock()

	if conn == nil {
		if release != nil {
			release()
		}
		return errors.New("session not connected")
	}
	if typ != frameHelloAck && !authOK {
		if release != nil {
			release()
		}
		return errors.New("session not authenticated")
	}

	req := acquireOutboundFrame(typ, streamID, payload, release)

	if typ == frameData {
		if err := s.enqueueData(req); err != nil {
			if release != nil {
				release()
			}
			releaseOutboundFrame(req)
			return err
		}
	} else {
		select {
		case <-s.closed:
			if release != nil {
				release()
			}
			releaseOutboundFrame(req)
			return net.ErrClosed
		case s.controlQ <- req:
		}
	}

	err := <-req.result
	releaseOutboundFrame(req)
	return err
}

func (s *Session) enqueueData(req *outboundFrame) error {
	s.schedMu.Lock()
	if s.conn == nil {
		s.schedMu.Unlock()
		return net.ErrClosed
	}
	pendingAfter := atomic.LoadInt64(&s.pendingBytes) + int64(len(req.payload))
	if pendingAfter > maxPendingBytes {
		s.schedMu.Unlock()
		return fmt.Errorf("session pending bytes limit exceeded: %d > %d", pendingAfter, maxPendingBytes)
	}
	if len(s.dataQueues[req.streamID]) == 0 {
		s.readyStreams = append(s.readyStreams, req.streamID)
	}
	s.dataQueues[req.streamID] = append(s.dataQueues[req.streamID], req)
	atomic.AddInt64(&s.pendingBytes, int64(len(req.payload)))
	atomic.AddInt64(&s.pendingFrames, 1)
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
	atomic.AddInt64(&s.pendingBytes, -int64(len(req.payload)))
	atomic.AddInt64(&s.pendingFrames, -1)
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
			req.finish(net.ErrClosed)
			continue
		}

		writeStart := time.Now()
		setWriteDeadline(conn)
		err := writeFrame(conn, req.typ, req.streamID, req.payload)
		clearWriteDeadline(conn)
		writeDur := time.Since(writeStart)
		if err == nil {
			atomic.AddUint64(&s.framesOut, 1)
			atomic.AddUint64(&s.bytesOut, uint64(len(req.payload)))
			atomic.StoreInt64(&s.lastFrameOutUnix, time.Now().Unix())
		}
		if queueWait := writeStart.Sub(req.enqueuedAt); queueWait > slowWriteThreshold || writeDur > slowWriteThreshold {
			atomic.AddUint64(&s.slowWrites, 1)
			logEvent("[SERVER] session=%s slow_send type=%s stream=%d bytes=%d lock_ms=%d write_ms=%d active=%d err=%v", s.sid, frameTypeName(req.typ), req.streamID, len(req.payload), queueWait.Milliseconds(), writeDur.Milliseconds(), active, err)
		}
		req.finish(err)
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
			"[SERVER] session=%s stream=%d summary reason=%s age_ms=%d bytes_from_relay=%d bytes_to_relay=%d frames_from_relay=%d frames_to_relay=%d",
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

func (s *Session) recommendedChunkSize() int {
	s.mu.Lock()
	active := len(s.streams)
	s.mu.Unlock()
	return adaptiveFramePayload(active, atomic.LoadInt64(&s.pendingBytes))
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
		s.cancelAllPendingOpens()
		s.failPending(net.ErrClosed)
		for _, stream := range streams {
			stream.close()
		}
		serverRegistry.remove(s)
		activeSessions.Add(-1)
		atomic.AddUint64(&closedSessions, 1)
		logEvent("[SERVER] session=%s closed err=%v active_sessions=%d", s.sid, err, activeSessions.Load())
	})
}

func (s *Session) readLoop(conn net.Conn) {
	defer s.closeAll(io.EOF)

	for {
		fr, err := readFrame(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				logEvent("[SERVER] session=%s read_failed err=%v", s.sid, err)
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
			if fr.typ != frameHello || string(fr.payload) != apiKey {
				logEvent("[SERVER] session=%s bad_hello", s.sid)
				return
			}
			s.mu.Lock()
			s.authOK = true
			s.mu.Unlock()
			if err := s.sendFrame(frameHelloAck, 0, nil); err != nil {
				logEvent("[SERVER] session=%s hello_ack_failed err=%v", s.sid, err)
				return
			}
			logEvent("[SERVER] session=%s ready", s.sid)
			continue
		}

		switch fr.typ {
		case frameOpen:
			openCtx, ok := s.startOpen(fr.streamID)
			if !ok {
				_ = s.sendFrame(frameOpenErr, fr.streamID, []byte("open-already-pending"))
				continue
			}
			go s.handleOpen(openCtx, fr.streamID, fr.payload)
		case frameData:
			stream := s.getStream(fr.streamID)
			if stream == nil {
				_ = s.sendFrame(frameClose, fr.streamID, nil)
				continue
			}
			if err := stream.enqueue(fr.payload); err != nil {
				logEvent("[SERVER] session=%s stream=%d enqueue_target_failed err=%v", s.sid, fr.streamID, err)
				s.closeStream(fr.streamID, true)
			}
		case frameClose:
			if s.cancelPendingOpen(fr.streamID) {
				continue
			}
			s.closeStream(fr.streamID, false)
		case framePing:
			if err := s.sendFrame(framePong, 0, nil); err != nil && !errors.Is(err, net.ErrClosed) {
				logEvent("[SERVER] session=%s pong_failed err=%v", s.sid, err)
				return
			}
		case framePong:
		default:
			logEvent("[SERVER] session=%s unexpected_frame type=%d", s.sid, fr.typ)
			return
		}
	}
}

func (s *Session) handleOpen(ctx context.Context, streamID uint32, payload []byte) {
	defer s.finishOpen(streamID)

	host, port, err := decodeOpenPayload(payload)
	if err != nil {
		_ = s.sendFrame(frameOpenErr, streamID, []byte(err.Error()))
		return
	}

	targetAddr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	dialer := net.Dialer{Timeout: dialTimeout, KeepAlive: keepalivePeriod}
	start := time.Now()
	targetConn, err := dialer.DialContext(ctx, "tcp", targetAddr)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			logEvent("[SERVER] session=%s stream=%d dial_canceled target=%s", s.sid, streamID, targetAddr)
			return
		}
		logEvent("[SERVER] session=%s stream=%d dial_failed target=%s dur_ms=%d err=%v", s.sid, streamID, targetAddr, time.Since(start).Milliseconds(), err)
		_ = s.sendFrame(frameOpenErr, streamID, []byte("dial-failed"))
		return
	}
	if ctx.Err() != nil {
		_ = targetConn.Close()
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

	logEvent("[SERVER] session=%s stream=%d open_ok target=%s dur_ms=%d active=%d", s.sid, streamID, targetAddr, time.Since(start).Milliseconds(), s.activeCount())
	go s.pumpToTarget(stream, targetAddr)
	go s.pumpTarget(stream, targetAddr)
}

func (s *Session) pumpTarget(stream *TargetStream, targetAddr string) {
	buf, poolIdx, _ := getPooledPayload(maxFramePayload)
	defer putPooledPayload(poolIdx, buf)

	for {
		chunkSize := s.recommendedChunkSize()
		n, err := stream.conn.Read(buf[:chunkSize])
		if n > 0 {
			payload, release := allocateFramePayload(buf[:n])
			atomic.AddUint64(&stream.bytesToRelay, uint64(n))
			atomic.AddUint64(&stream.framesToRelay, 1)
			if err := s.sendFrameWithRelease(frameData, stream.id, payload, release); err != nil {
				logEvent("[SERVER] session=%s stream=%d send_data_failed err=%v", s.sid, stream.id, err)
				break
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				logEvent("[SERVER] session=%s stream=%d read_target_failed target=%s err=%v", s.sid, stream.id, targetAddr, err)
			}
			break
		}
	}

	s.closeStream(stream.id, true)
	logEvent("[SERVER] session=%s stream=%d closed target=%s active=%d", s.sid, stream.id, targetAddr, s.activeCount())
}

func (s *Session) pumpToTarget(stream *TargetStream, targetAddr string) {
	for {
		select {
		case <-stream.done:
			return
		case payload := <-stream.q:
			if err := writeAll(stream.conn, payload); err != nil {
				logEvent("[SERVER] session=%s stream=%d write_target_failed target=%s err=%v", s.sid, stream.id, targetAddr, err)
				s.closeStream(stream.id, true)
				return
			}
		}
	}
}

func (s *Session) statsLoop() {
	ticker := time.NewTicker(sessionStatsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-ticker.C:
		}
		s.mu.Lock()
		connected := s.conn != nil
		authOK := s.authOK
		active := len(s.streams)
		s.mu.Unlock()
		logEvent(
			"[SERVER] session=%s stats connected=%t auth=%t active=%d frames_in=%d frames_out=%d bytes_in=%d bytes_out=%d slow_writes=%d last_in=%d last_out=%d",
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

func (s *Session) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval / 2)
	defer ticker.Stop()

	for {
		select {
		case <-s.closed:
			return
		case <-ticker.C:
		}

		s.mu.Lock()
		connected := s.conn != nil
		authOK := s.authOK
		s.mu.Unlock()
		if !connected {
			return
		}

		now := time.Now()
		lastIn := time.Unix(atomic.LoadInt64(&s.lastFrameInUnix), 0)
		if now.Sub(lastIn) > heartbeatTimeout {
			logEvent("[SERVER] session=%s heartbeat_timeout last_in=%d", s.sid, atomic.LoadInt64(&s.lastFrameInUnix))
			s.closeAll(errors.New("heartbeat timeout"))
			return
		}

		if !authOK {
			continue
		}
		lastOut := time.Unix(atomic.LoadInt64(&s.lastFrameOutUnix), 0)
		if now.Sub(lastOut) >= heartbeatInterval {
			if err := s.sendFrame(framePing, 0, nil); err != nil && !errors.Is(err, net.ErrClosed) {
				logEvent("[SERVER] session=%s heartbeat_ping_failed err=%v", s.sid, err)
				return
			}
		}
	}
}

func buildServerStatus() serverStatusResponse {
	return serverStatusResponse{
		Service:          "furo-server",
		Version:          appVersion,
		Commit:           appCommit,
		BuildDate:        appBuildDate,
		StartedAt:        serverStartedAt.UTC().Format(time.RFC3339),
		UptimeSec:        int64(time.Since(serverStartedAt).Seconds()),
		AgentListen:      agentListen,
		AdminListen:      adminListen,
		DialTimeout:      dialTimeout.String(),
		WriteTimeout:     writeTimeout.String(),
		FrameMinSize:     minFramePayload,
		FrameMidSize:     midFramePayload,
		FrameMaxSize:     maxFramePayload,
		MaxPendingBytes:  maxPendingBytes,
		MaxSessions:      maxSessions,
		ActiveSessions:   activeSessions.Load(),
		AcceptedSessions: atomic.LoadUint64(&acceptedSessions),
		RejectedSessions: atomic.LoadUint64(&rejectedSessions),
		ClosedSessions:   atomic.LoadUint64(&closedSessions),
		Sessions:         serverRegistry.snapshots(),
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func runServerAdminServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		status := buildServerStatus()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":              true,
			"active_sessions": status.ActiveSessions,
			"saturated":       status.MaxSessions > 0 && int(status.ActiveSessions) >= status.MaxSessions,
		})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, buildServerStatus())
	})

	server := &http.Server{
		Addr:              adminListen,
		Handler:           mux,
		ReadHeaderTimeout: adminReadHeaderTimeout,
	}

	log.Printf("[FURO-SERVER] admin listener on %s", adminListen)
	return server.ListenAndServe()
}

func reserveSessionSlot() bool {
	if maxSessions <= 0 {
		return true
	}

	limit := int32(maxSessions)
	for {
		current := activeSessions.Load()
		if current >= limit {
			return false
		}
		if activeSessions.CompareAndSwap(current, current+1) {
			return true
		}
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
		logEvent("[SERVER] handshake_read_failed remote=%s err=%v", conn.RemoteAddr(), err)
		return
	}

	parts := strings.Fields(line)
	if len(parts) != 3 || parts[0] != "SESSION" {
		_ = writeString(conn, "ERR bad-handshake\n")
		atomic.AddUint64(&rejectedSessions, 1)
		logEvent("[SERVER] bad_handshake remote=%s line=%q", conn.RemoteAddr(), line)
		return
	}
	if parts[1] != apiKey {
		_ = writeString(conn, "ERR unauthorized\n")
		atomic.AddUint64(&rejectedSessions, 1)
		logEvent("[SERVER] unauthorized remote=%s sid=%s", conn.RemoteAddr(), parts[2])
		return
	}

	slotReserved := reserveSessionSlot()
	if !slotReserved {
		_ = writeString(conn, "ERR session-limit\n")
		atomic.AddUint64(&rejectedSessions, 1)
		logEvent("[SERVER] reject sid=%s reason=session-limit active_sessions=%d", parts[2], activeSessions.Load())
		return
	}
	defer func() {
		if slotReserved {
			activeSessions.Add(-1)
		}
	}()

	if err := writeString(conn, "OK\n"); err != nil {
		logEvent("[SERVER] handshake_ack_failed sid=%s err=%v", parts[2], err)
		return
	}
	_ = conn.SetDeadline(time.Time{})

	session := newSession(parts[2], conn)
	conn = nil
	serverRegistry.add(session)
	slotReserved = false
	atomic.AddUint64(&acceptedSessions, 1)
	logEvent("[SERVER] session=%s accepted remote=%s active_sessions=%d", session.sid, session.conn.RemoteAddr(), activeSessions.Load())
	go session.statsLoop()
	go session.readLoop(session.conn)
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println(serverVersionString())
		return
	}

	if err := loadServerConfig(*configPath); err != nil {
		log.Fatalf("failed to load server config %s: %v", *configPath, err)
	}

	if logFilePath != "" {
		logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("failed to open log file %s: %v", logFilePath, err)
		}
		logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
		log.Printf("[FURO-SERVER] debug log file: %s", logFilePath)
	}
	logEvent("[SERVER] startup version=%s agent_listen=%s admin_listen=%s dial_timeout=%s write_timeout=%s frame_min=%d frame_mid=%d frame_max=%d max_pending_bytes=%d max_sessions=%d", appVersion, agentListen, adminListen, dialTimeout.String(), writeTimeout.String(), minFramePayload, midFramePayload, maxFramePayload, maxPendingBytes, maxSessions)

	ln, err := net.Listen("tcp", agentListen)
	if err != nil {
		log.Fatalf("[FURO-SERVER] listen failed on %s: %v", agentListen, err)
	}

	if adminListen != "" {
		go func() {
			if err := runServerAdminServer(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("[FURO-SERVER] admin server failed: %v", err)
			}
		}()
	}

	log.Printf("[FURO-SERVER] agent listening on %s", agentListen)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[FURO-SERVER] accept error: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		go handleRelayConn(conn)
	}
}
