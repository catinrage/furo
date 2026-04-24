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
	frameHeaderSize  = 9
	maxFramePayload  = 64 * 1024
	sessionPollDelay = 1200 * time.Millisecond
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
	maxRelays       = flag.Int("max-relays", 0, "Compatibility flag; ignored in multiplex mode")
	logFilePath     = flag.String("log-file", "logs.txt", "Path to debug log file")
)

var (
	logMu  sync.Mutex
	logger = log.New(io.Discard, "", log.LstdFlags|log.Lmicroseconds)

	httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   8,
			MaxConnsPerHost:       8,
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

	return ctx, &socks5.AddrSpec{
		FQDN: dest.FQDN,
		Port: dest.Port,
	}
}

func newUpstreamResolver() *upstreamResolver {
	return &upstreamResolver{
		fallback: &net.Resolver{PreferGo: true},
	}
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
	_ = tcp.SetReadBuffer(256 * 1024)
	_ = tcp.SetWriteBuffer(256 * 1024)
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

func encodeOpenPayload(host string, port uint16) []byte {
	hostBytes := []byte(host)
	payload := make([]byte, 2+len(hostBytes)+2)
	binary.BigEndian.PutUint16(payload[0:2], uint16(len(hostBytes)))
	copy(payload[2:2+len(hostBytes)], hostBytes)
	binary.BigEndian.PutUint16(payload[2+len(hostBytes):], port)
	return payload
}

type MuxConn struct {
	id  uint32
	mgr *SessionManager

	mu           sync.Mutex
	cond         *sync.Cond
	openReady    bool
	openErr      error
	readQ        [][]byte
	readBuf      []byte
	closed       bool
	remoteClosed bool
}

func newMuxConn(mgr *SessionManager, id uint32) *MuxConn {
	c := &MuxConn{id: id, mgr: mgr}
	c.cond = sync.NewCond(&c.mu)
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
	if c.openErr == nil && !c.openReady && !c.closed {
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
	c.mu.Lock()
	if !c.closed {
		c.readQ = append(c.readQ, buf)
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

func (c *MuxConn) markRemoteClosed() {
	c.mu.Lock()
	c.remoteClosed = true
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *MuxConn) fail(err error) {
	c.mu.Lock()
	if c.openErr == nil && !c.closed {
		c.openErr = err
	}
	c.cond.Broadcast()
	c.mu.Unlock()
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
		if err := c.mgr.sendFrame(frameData, c.id, b[:chunkLen]); err != nil {
			c.fail(err)
			return total, err
		}
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

	c.mgr.removeStream(c.id)
	_ = c.mgr.sendFrame(frameClose, c.id, nil)
	return nil
}

func (c *MuxConn) LocalAddr() net.Addr  { return &net.TCPAddr{IP: net.IPv4zero, Port: 0} }
func (c *MuxConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.IPv4zero, Port: 0} }
func (c *MuxConn) SetDeadline(time.Time) error      { return nil }
func (c *MuxConn) SetReadDeadline(time.Time) error  { return nil }
func (c *MuxConn) SetWriteDeadline(time.Time) error { return nil }

type SessionManager struct {
	mu             sync.Mutex
	writerMu       sync.Mutex
	conn           net.Conn
	ready          bool
	streams        map[uint32]*MuxConn
	nextStreamID   uint32
	sessionGen     uint64
	requestRunning bool
}

func newSessionManager() *SessionManager {
	return &SessionManager{
		streams:      make(map[uint32]*MuxConn),
		nextStreamID: 1,
	}
}

func (m *SessionManager) activeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.streams)
}

func (m *SessionManager) waitReady(ctx context.Context) error {
	for {
		m.mu.Lock()
		ready := m.ready && m.conn != nil
		m.mu.Unlock()
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (m *SessionManager) setAcceptedConn(conn net.Conn) {
	setTCPOptions(conn)

	m.mu.Lock()
	oldConn := m.conn
	oldStreams := m.snapshotAndResetStreamsLocked()
	m.conn = conn
	m.ready = false
	m.sessionGen++
	gen := m.sessionGen
	m.mu.Unlock()

	if oldConn != nil {
		_ = oldConn.Close()
	}
	for _, stream := range oldStreams {
		stream.fail(errors.New("session replaced"))
	}

	logEvent("[IR] session accepted remote=%s", conn.RemoteAddr())
	go m.readLoop(gen, conn)
	if err := m.sendHello(gen); err != nil {
		logEvent("[IR] session hello_failed err=%v", err)
		m.closeSession(gen, err)
	}
}

func (m *SessionManager) snapshotAndResetStreamsLocked() []*MuxConn {
	streams := make([]*MuxConn, 0, len(m.streams))
	for id, stream := range m.streams {
		streams = append(streams, stream)
		delete(m.streams, id)
	}
	return streams
}

func (m *SessionManager) closeSession(gen uint64, err error) {
	m.mu.Lock()
	if gen != 0 && gen != m.sessionGen {
		m.mu.Unlock()
		return
	}
	conn := m.conn
	streams := m.snapshotAndResetStreamsLocked()
	m.conn = nil
	m.ready = false
	m.sessionGen++
	m.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	for _, stream := range streams {
		stream.fail(err)
	}
	logEvent("[IR] session closed err=%v", err)
}

func (m *SessionManager) sendHello(gen uint64) error {
	m.mu.Lock()
	if gen != m.sessionGen || m.conn == nil {
		m.mu.Unlock()
		return errors.New("no live session")
	}
	conn := m.conn
	m.mu.Unlock()

	m.writerMu.Lock()
	defer m.writerMu.Unlock()
	return writeFrame(conn, frameHello, 0, []byte(*apiKey))
}

func (m *SessionManager) sendFrame(typ byte, streamID uint32, payload []byte) error {
	m.mu.Lock()
	conn := m.conn
	ready := m.ready
	m.mu.Unlock()

	if conn == nil {
		return errors.New("session not connected")
	}
	if typ != frameHello && !ready {
		return errors.New("session not ready")
	}

	m.writerMu.Lock()
	defer m.writerMu.Unlock()
	return writeFrame(conn, typ, streamID, payload)
}

func (m *SessionManager) nextStream() uint32 {
	return atomic.AddUint32(&m.nextStreamID, 2)
}

func (m *SessionManager) registerStream(stream *MuxConn) {
	m.mu.Lock()
	m.streams[stream.id] = stream
	m.mu.Unlock()
}

func (m *SessionManager) removeStream(streamID uint32) {
	m.mu.Lock()
	delete(m.streams, streamID)
	m.mu.Unlock()
}

func (m *SessionManager) getStream(streamID uint32) *MuxConn {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[streamID]
}

func (m *SessionManager) readLoop(gen uint64, conn net.Conn) {
	for {
		fr, err := readFrame(conn)
		if err != nil {
			m.closeSession(gen, err)
			return
		}

		switch fr.typ {
		case frameHelloAck:
			m.mu.Lock()
			if gen == m.sessionGen && m.conn == conn {
				m.ready = true
			}
			m.mu.Unlock()
			logEvent("[IR] session ready")
		case frameOpenOK:
			stream := m.getStream(fr.streamID)
			if stream != nil {
				stream.markOpenReady()
				logEvent("[IR] stream=%d open_ok active=%d", fr.streamID, m.activeCount())
			}
		case frameOpenErr:
			stream := m.getStream(fr.streamID)
			if stream != nil {
				stream.markOpenErr(fmt.Errorf("open failed: %s", string(fr.payload)))
				m.removeStream(fr.streamID)
				logEvent("[IR] stream=%d open_err detail=%q", fr.streamID, string(fr.payload))
			}
		case frameData:
			stream := m.getStream(fr.streamID)
			if stream != nil {
				stream.enqueueData(fr.payload)
			}
		case frameClose:
			stream := m.getStream(fr.streamID)
			if stream != nil {
				stream.markRemoteClosed()
				m.removeStream(fr.streamID)
			}
		default:
			m.closeSession(gen, fmt.Errorf("unexpected frame type=%d", fr.typ))
			return
		}
	}
}

func (m *SessionManager) dialStream(ctx context.Context, host, port string) (net.Conn, error) {
	if err := m.waitReady(ctx); err != nil {
		return nil, err
	}

	portNum, err := parsePort(port)
	if err != nil {
		return nil, err
	}

	streamID := m.nextStream()
	stream := newMuxConn(m, streamID)
	m.registerStream(stream)

	if err := m.sendFrame(frameOpen, streamID, encodeOpenPayload(host, portNum)); err != nil {
		m.removeStream(streamID)
		return nil, err
	}

	waitCtx, cancel := context.WithTimeout(ctx, *openTimeout)
	defer cancel()
	if err := stream.waitForOpen(waitCtx); err != nil {
		m.removeStream(streamID)
		return nil, err
	}

	logEvent("[IR] stream=%d new_tunnel target=%s:%s active=%d", streamID, host, port, m.activeCount())
	return stream, nil
}

func parsePort(value string) (uint16, error) {
	var port int
	if _, err := fmt.Sscanf(value, "%d", &port); err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid port: %s", value)
	}
	return uint16(port), nil
}

func (m *SessionManager) requestSession() {
	m.mu.Lock()
	if m.requestRunning || m.conn != nil {
		m.mu.Unlock()
		return
	}
	m.requestRunning = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.requestRunning = false
		m.mu.Unlock()
	}()

	values := url.Values{}
	values.Set("action", "session")
	values.Set("iran_host", *publicHost)
	values.Set("iran_port", fmt.Sprintf("%d", *publicPort))
	values.Set("outer_host", *outerHost)
	values.Set("outer_port", fmt.Sprintf("%d", *outerPort))

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, *relayURL+"?"+values.Encode(), nil)
	if err != nil {
		logEvent("[IR] session_request_build_failed err=%v", err)
		return
	}
	req.Header.Set("X-API-KEY", *apiKey)

	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		logEvent("[IR] session_request_failed dur_ms=%d err=%v", time.Since(start).Milliseconds(), err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		logEvent("[IR] session_request_rejected status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
		return
	}

	logEvent("[IR] session_request_ok dur_ms=%d", time.Since(start).Milliseconds())
	_, _ = io.Copy(io.Discard, resp.Body)
	logEvent("[IR] session_request_body_closed")
}

func (m *SessionManager) sessionLoop() {
	for {
		m.mu.Lock()
		connected := m.conn != nil
		running := m.requestRunning
		m.mu.Unlock()

		if !connected && !running {
			go m.requestSession()
		}
		time.Sleep(sessionPollDelay)
	}
}

func runAgentListener(mgr *SessionManager) error {
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
		mgr.setAcceptedConn(conn)
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

	logFile, err := os.OpenFile(*logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("failed to open log file %s: %v", *logFilePath, err)
	}
	logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
	log.Printf("[IR-SOCK] debug log file: %s", *logFilePath)

	httpClient.Transport.(*http.Transport).ResponseHeaderTimeout = *openTimeout
	mgr := newSessionManager()
	logEvent("[IR] startup relay=%s listen=%s:%s agent=%s public=%s:%d outer=%s:%d open_timeout=%s max_relays_compat=%d", *relayURL, *listenIP, *listenPort, *agentListen, *publicHost, *publicPort, *outerHost, *outerPort, openTimeout.String(), *maxRelays)

	go func() {
		if err := runAgentListener(mgr); err != nil {
			log.Fatalf("[IR-SOCK] agent listener failed: %v", err)
		}
	}()
	go mgr.sessionLoop()

	conf := &socks5.Config{
		Resolver: newUpstreamResolver(),
		Rewriter: preserveFQDNRewriter{},
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, targetPort, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			return mgr.dialStream(ctx, host, targetPort)
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
