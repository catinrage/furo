package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	inspectFrameHello byte = iota + 1
	inspectFrameHelloAck
	inspectFrameOpen
	inspectFrameOpenOK
	inspectFrameOpenErr
	inspectFrameData
	inspectFrameClose
	inspectFramePing
	inspectFramePong
)

const (
	inspectFrameHeaderSize  = 9
	inspectMaxFramePayload  = 128 * 1024
	inspectLineLimit        = 2048
	inspectAcceptTimeout    = 20 * time.Second
	inspectRelayWaitTimeout = 20 * time.Second
	inspectHelloTimeout     = 15 * time.Second
	inspectPingCount        = 3
)

var (
	inspectConfigPath = flag.String("c", "config.client.json", "Path to client config JSON")
	inspectRouteID    = flag.String("route-id", "", "Route id to inspect when config.client.json contains multiple routes")
	inspectAllRoutes  = flag.Bool("all", false, "Inspect all enabled routes in config.client.json")
	speedTestEnabled  = flag.Bool("speed-test", false, "Run a download speed test through the relay")
	speedTestURL      = flag.String("speed-test-url", "https://nbg1-speed.hetzner.com/100MB.bin", "URL used for the optional speed test")
)

func init() {
	flag.Usage = func() {
		out := flag.CommandLine.Output()
		bin := os.Args[0]
		fmt.Fprintf(out, "Usage: %s [flags]\n\n", bin)
		fmt.Fprintln(out, "Inspect verifies the relay -> server path using config.client.json settings.")
		fmt.Fprintln(out, "It opens one temporary relay session, measures relay ping, and can optionally run")
		fmt.Fprintln(out, "a download speed test through the tunnel.")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Behavior:")
		fmt.Fprintln(out, "- Reads relay/server/api_key/public_host settings from the client config.")
		fmt.Fprintln(out, "- Tries agent_listen first; if furo-client is already using it, inspect falls back")
		fmt.Fprintln(out, "  to a temporary free port and advertises that port to the relay.")
		fmt.Fprintln(out, "- Reports the exact failure stage if setup breaks.")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Examples:")
		fmt.Fprintf(out, "  %s -c config.client.json\n", bin)
		fmt.Fprintf(out, "  %s -c config.client.json --route-id route_1\n", bin)
		fmt.Fprintf(out, "  %s -c config.client.json --all\n", bin)
		fmt.Fprintf(out, "  %s -c config.client.json --speed-test\n", bin)
		fmt.Fprintf(out, "  %s -c config.client.json --speed-test --speed-test-url https://example.com/test.bin\n", bin)
	}
}

type inspectClientConfig struct {
	Routes      []inspectRouteConfigFile `json:"routes"`
	RelayURL    string                   `json:"relay_url"`
	APIKey      string                   `json:"api_key"`
	AgentListen string                   `json:"agent_listen"`
	PublicHost  string                   `json:"public_host"`
	PublicPort  int                      `json:"public_port"`
	ServerHost  string                   `json:"server_host"`
	ServerPort  int                      `json:"server_port"`
	OpenTimeout string                   `json:"open_timeout"`
	Keepalive   string                   `json:"keepalive"`

	SelectedRouteID string `json:"-"`
}

type inspectRouteConfigFile struct {
	ID           string `json:"id"`
	RelayURL     string `json:"relay_url"`
	PublicHost   string `json:"public_host"`
	PublicPort   int    `json:"public_port"`
	ServerHost   string `json:"server_host"`
	ServerPort   int    `json:"server_port"`
	SessionCount int    `json:"session_count"`
	Enabled      *bool  `json:"enabled"`
}

type inspectRelayResult struct {
	body io.Closer
	err  error
}

type inspectAcceptResult struct {
	conn net.Conn
	err  error
}

type inspectDiagnostic struct {
	Stage string
	Err   error
}

func (d *inspectDiagnostic) Error() string {
	if d == nil {
		return ""
	}
	return fmt.Sprintf("%s: %v", d.Stage, d.Err)
}

func (d *inspectDiagnostic) Unwrap() error {
	if d == nil {
		return nil
	}
	return d.Err
}

func inspectStageError(stage string, err error) error {
	if err == nil {
		return nil
	}
	return &inspectDiagnostic{Stage: stage, Err: err}
}

type inspectStep struct {
	Title string
	Value string
}

type inspectReport struct {
	w               io.Writer
	mu              sync.Mutex
	steps           []inspectStep
	progressActive  bool
	lastProgressLen int
}

func newInspectReport(w io.Writer) *inspectReport {
	return &inspectReport{w: w}
}

func (r *inspectReport) start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Fprintln(r.w, "FURO inspect running")
	fmt.Fprintln(r.w)
}

func (r *inspectReport) clearProgressLocked() {
	if !r.progressActive {
		return
	}
	fmt.Fprint(r.w, "\n")
	r.progressActive = false
	r.lastProgressLen = 0
}

func (r *inspectReport) printStepLocked(prefix, title, value string) {
	fmt.Fprintf(r.w, "%s %s: %s\n", prefix, title, value)
}

func (r *inspectReport) add(title, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clearProgressLocked()
	r.steps = append(r.steps, inspectStep{Title: title, Value: value})
	r.printStepLocked("✓", title, value)
}

func (r *inspectReport) note(title, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clearProgressLocked()
	r.printStepLocked("•", title, value)
}

func (r *inspectReport) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clearProgressLocked()
	r.steps = nil
}

func (r *inspectReport) updateProgress(title string, downloaded, total int64, elapsed time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}

	mibPerSec := float64(downloaded) / elapsed.Seconds() / (1024 * 1024)
	mbps := float64(downloaded*8) / elapsed.Seconds() / 1_000_000

	var line string
	if total > 0 {
		ratio := float64(downloaded) / float64(total)
		if ratio > 1 {
			ratio = 1
		}
		line = fmt.Sprintf(
			"\r⏬ %s: [%s] %5.1f%% %s / %s  %.2f MiB/s (%.2f Mbps)",
			title,
			inspectRenderProgressBar(ratio, 24),
			ratio*100,
			inspectFormatBytes(downloaded),
			inspectFormatBytes(total),
			mibPerSec,
			mbps,
		)
	} else {
		line = fmt.Sprintf(
			"\r⏬ %s: %s downloaded  %.2f MiB/s (%.2f Mbps)",
			title,
			inspectFormatBytes(downloaded),
			mibPerSec,
			mbps,
		)
	}

	if pad := r.lastProgressLen - len([]rune(line)); pad > 0 {
		line += strings.Repeat(" ", pad)
	}
	fmt.Fprint(r.w, line)
	r.progressActive = true
	r.lastProgressLen = len([]rune(line))
}

func (r *inspectReport) printResult(failed error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clearProgressLocked()
	if failed == nil {
		fmt.Fprintln(r.w)
		fmt.Fprintln(r.w, "✓ FURO inspect succeeded")
	} else {
		fmt.Fprintln(r.w)
		fmt.Fprintln(r.w, "✗ FURO inspect failed")
		fmt.Fprintln(r.w)
		fmt.Fprintf(r.w, "Failure: %v\n", failed)
	}
}

func inspectRenderProgressBar(ratio float64, width int) string {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	filled := int(ratio * float64(width))
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func inspectFormatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for value := n / unit; value >= unit && exp < 5; value /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

type inspectFrame struct {
	typ      byte
	streamID uint32
	payload  []byte
}

type inspectStream struct {
	id      uint32
	session *inspectSession

	mu           sync.Mutex
	cond         *sync.Cond
	readQ        [][]byte
	readBuf      []byte
	openReady    bool
	openErr      error
	closed       bool
	remoteClosed bool
	done         chan struct{}
	closeOnce    sync.Once
}

func newInspectStream(session *inspectSession, id uint32) *inspectStream {
	stream := &inspectStream{
		id:      id,
		session: session,
		done:    make(chan struct{}),
	}
	stream.cond = sync.NewCond(&stream.mu)
	return stream
}

func (s *inspectStream) markOpenReady() {
	s.mu.Lock()
	if !s.closed && s.openErr == nil {
		s.openReady = true
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *inspectStream) markOpenErr(err error) {
	s.mu.Lock()
	if !s.closed && s.openErr == nil {
		s.openErr = err
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *inspectStream) enqueueData(payload []byte) {
	if len(payload) == 0 {
		return
	}
	buf := make([]byte, len(payload))
	copy(buf, payload)
	s.mu.Lock()
	if !s.closed {
		s.readQ = append(s.readQ, buf)
		s.cond.Broadcast()
	}
	s.mu.Unlock()
}

func (s *inspectStream) markRemoteClosed() {
	s.mu.Lock()
	s.remoteClosed = true
	s.cond.Broadcast()
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *inspectStream) fail(err error) {
	s.mu.Lock()
	if s.openErr == nil {
		s.openErr = err
	}
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *inspectStream) waitForOpen(ctx context.Context) error {
	for {
		s.mu.Lock()
		switch {
		case s.openReady:
			s.mu.Unlock()
			return nil
		case s.openErr != nil:
			err := s.openErr
			s.mu.Unlock()
			return err
		case s.closed:
			s.mu.Unlock()
			return net.ErrClosed
		}
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *inspectStream) Read(buf []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.readBuf) == 0 && len(s.readQ) == 0 && !s.remoteClosed && !s.closed && s.openErr == nil {
		s.cond.Wait()
	}
	if len(s.readBuf) == 0 && len(s.readQ) > 0 {
		s.readBuf = s.readQ[0]
		s.readQ = s.readQ[1:]
	}
	if len(s.readBuf) > 0 {
		n := copy(buf, s.readBuf)
		s.readBuf = s.readBuf[n:]
		return n, nil
	}
	if s.openErr != nil {
		return 0, s.openErr
	}
	if s.remoteClosed || s.closed {
		return 0, io.EOF
	}
	return 0, nil
}

func (s *inspectStream) Write(buf []byte) (int, error) {
	if err := s.waitForOpen(context.Background()); err != nil {
		return 0, err
	}

	total := 0
	for len(buf) > 0 {
		chunkSize := inspectMaxFramePayload
		if chunkSize > len(buf) {
			chunkSize = len(buf)
		}
		payload := make([]byte, chunkSize)
		copy(payload, buf[:chunkSize])
		if err := s.session.sendFrame(inspectFrameData, s.id, payload); err != nil {
			s.fail(err)
			return total, err
		}
		total += chunkSize
		buf = buf[chunkSize:]
	}
	return total, nil
}

func (s *inspectStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()

	s.closeOnce.Do(func() { close(s.done) })
	s.session.unregisterStream(s.id)
	return s.session.sendFrame(inspectFrameClose, s.id, nil)
}

func (s *inspectStream) LocalAddr() net.Addr              { return &net.TCPAddr{IP: net.IPv4zero, Port: 0} }
func (s *inspectStream) RemoteAddr() net.Addr             { return &net.TCPAddr{IP: net.IPv4zero, Port: 0} }
func (s *inspectStream) SetDeadline(time.Time) error      { return nil }
func (s *inspectStream) SetReadDeadline(time.Time) error  { return nil }
func (s *inspectStream) SetWriteDeadline(time.Time) error { return nil }

type inspectSession struct {
	conn net.Conn

	writeMu     sync.Mutex
	closeOnce   sync.Once
	done        chan struct{}
	streamMu    sync.Mutex
	streams     map[uint32]*inspectStream
	nextStream  uint32
	helloCh     chan error
	helloOnce   sync.Once
	pongCh      chan struct{}
	pingMu      sync.Mutex
	sessionErr  error
	sessionErrM sync.Mutex
}

func newInspectSession(conn net.Conn) *inspectSession {
	return &inspectSession{
		conn:    conn,
		done:    make(chan struct{}),
		streams: make(map[uint32]*inspectStream),
		helloCh: make(chan error, 1),
		pongCh:  make(chan struct{}, 1),
	}
}

func (s *inspectSession) setErr(err error) {
	s.sessionErrM.Lock()
	if s.sessionErr == nil {
		s.sessionErr = err
	}
	s.sessionErrM.Unlock()
}

func (s *inspectSession) err() error {
	s.sessionErrM.Lock()
	defer s.sessionErrM.Unlock()
	return s.sessionErr
}

func (s *inspectSession) close(err error) {
	s.closeOnce.Do(func() {
		s.setErr(err)
		if s.conn != nil {
			_ = s.conn.Close()
		}
		close(s.done)
		s.helloOnce.Do(func() {
			s.helloCh <- err
		})

		s.streamMu.Lock()
		streams := make([]*inspectStream, 0, len(s.streams))
		for id, stream := range s.streams {
			streams = append(streams, stream)
			delete(s.streams, id)
		}
		s.streamMu.Unlock()

		for _, stream := range streams {
			stream.fail(err)
		}
	})
}

func (s *inspectSession) registerStream(stream *inspectStream) {
	s.streamMu.Lock()
	s.streams[stream.id] = stream
	s.streamMu.Unlock()
}

func (s *inspectSession) unregisterStream(streamID uint32) {
	s.streamMu.Lock()
	delete(s.streams, streamID)
	s.streamMu.Unlock()
}

func (s *inspectSession) getStream(streamID uint32) *inspectStream {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	return s.streams[streamID]
}

func (s *inspectSession) sendFrame(typ byte, streamID uint32, payload []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.conn == nil {
		return net.ErrClosed
	}
	return inspectWriteFrame(s.conn, typ, streamID, payload)
}

func (s *inspectSession) waitHelloAck(ctx context.Context) error {
	select {
	case err := <-s.helloCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		if err := s.err(); err != nil {
			return err
		}
		return net.ErrClosed
	}
}

func (s *inspectSession) ping(ctx context.Context) (time.Duration, error) {
	s.pingMu.Lock()
	defer s.pingMu.Unlock()

	for {
		select {
		case <-s.pongCh:
		default:
			goto drained
		}
	}

drained:
	start := time.Now()
	if err := s.sendFrame(inspectFramePing, 0, nil); err != nil {
		return 0, err
	}

	select {
	case <-s.pongCh:
		return time.Since(start), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.done:
		if err := s.err(); err != nil {
			return 0, err
		}
		return 0, net.ErrClosed
	}
}

func (s *inspectSession) openStream(ctx context.Context, host string, port uint16) (*inspectStream, error) {
	s.streamMu.Lock()
	s.nextStream++
	streamID := s.nextStream
	stream := newInspectStream(s, streamID)
	s.streams[streamID] = stream
	s.streamMu.Unlock()

	if err := s.sendFrame(inspectFrameOpen, streamID, inspectEncodeOpenPayload(host, port)); err != nil {
		s.unregisterStream(streamID)
		return nil, err
	}

	if err := stream.waitForOpen(ctx); err != nil {
		s.unregisterStream(streamID)
		return nil, err
	}
	return stream, nil
}

func (s *inspectSession) readLoop() {
	defer s.close(io.EOF)

	for {
		frame, err := inspectReadFrame(s.conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.setErr(err)
			}
			return
		}

		switch frame.typ {
		case inspectFrameHelloAck:
			s.helloOnce.Do(func() {
				s.helloCh <- nil
			})
		case inspectFrameOpenOK:
			if stream := s.getStream(frame.streamID); stream != nil {
				stream.markOpenReady()
			}
		case inspectFrameOpenErr:
			if stream := s.getStream(frame.streamID); stream != nil {
				msg := strings.TrimSpace(string(frame.payload))
				if msg == "" {
					msg = "open rejected"
				}
				stream.markOpenErr(errors.New(msg))
				s.unregisterStream(frame.streamID)
			}
		case inspectFrameData:
			if stream := s.getStream(frame.streamID); stream != nil {
				stream.enqueueData(frame.payload)
			}
		case inspectFrameClose:
			if stream := s.getStream(frame.streamID); stream != nil {
				stream.markRemoteClosed()
				s.unregisterStream(frame.streamID)
			}
		case inspectFramePing:
			if err := s.sendFrame(inspectFramePong, 0, nil); err != nil {
				s.setErr(err)
				return
			}
		case inspectFramePong:
			select {
			case s.pongCh <- struct{}{}:
			default:
			}
		default:
			s.setErr(fmt.Errorf("unexpected frame type %d", frame.typ))
			return
		}
	}
}

func inspectDefaultClientConfig() inspectClientConfig {
	return inspectClientConfig{
		RelayURL:    "https://hidaco.site/tools/rel/soc/furo-relay.php",
		APIKey:      "my_super_secret_123456789",
		AgentListen: "0.0.0.0:28080",
		PublicPort:  28080,
		ServerPort:  28081,
		OpenTimeout: "45s",
		Keepalive:   "30s",
	}
}

func inspectLoadClientConfigFile(path string) (inspectClientConfig, time.Duration, error) {
	cfg := inspectDefaultClientConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return inspectClientConfig{}, 0, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return inspectClientConfig{}, 0, fmt.Errorf("parse config: %w", err)
	}

	if cfg.APIKey == "" {
		return inspectClientConfig{}, 0, errors.New("api_key is required")
	}
	if cfg.AgentListen == "" {
		return inspectClientConfig{}, 0, errors.New("agent_listen is required")
	}

	openTimeout, err := time.ParseDuration(cfg.OpenTimeout)
	if err != nil {
		return inspectClientConfig{}, 0, fmt.Errorf("parse open_timeout: %w", err)
	}
	if _, err := time.ParseDuration(cfg.Keepalive); err != nil {
		return inspectClientConfig{}, 0, fmt.Errorf("parse keepalive: %w", err)
	}

	return cfg, openTimeout, nil
}

func inspectValidateSelectedConfig(cfg inspectClientConfig) error {
	switch {
	case cfg.RelayURL == "":
		return errors.New("relay_url is required")
	case cfg.PublicHost == "":
		return errors.New("public_host is required")
	case cfg.PublicPort < 1 || cfg.PublicPort > 65535:
		return errors.New("public_port must be between 1 and 65535")
	case cfg.ServerHost == "":
		return errors.New("server_host is required")
	case cfg.ServerPort < 1 || cfg.ServerPort > 65535:
		return errors.New("server_port must be between 1 and 65535")
	}
	return nil
}

func inspectApplyRoute(cfg inspectClientConfig, route inspectRouteConfigFile) inspectClientConfig {
	cfg.SelectedRouteID = route.ID
	cfg.RelayURL = route.RelayURL
	cfg.PublicHost = route.PublicHost
	cfg.PublicPort = route.PublicPort
	cfg.ServerHost = route.ServerHost
	cfg.ServerPort = route.ServerPort
	return cfg
}

func inspectLoadClientConfig(path string) (inspectClientConfig, time.Duration, error) {
	cfg, openTimeout, err := inspectLoadClientConfigFile(path)
	if err != nil {
		return inspectClientConfig{}, 0, err
	}

	if len(cfg.Routes) > 0 {
		selected, err := inspectSelectRoute(cfg, *inspectRouteID)
		if err != nil {
			return inspectClientConfig{}, 0, err
		}
		cfg = inspectApplyRoute(cfg, selected)
	}

	if err := inspectValidateSelectedConfig(cfg); err != nil {
		return inspectClientConfig{}, 0, err
	}
	return cfg, openTimeout, nil
}

func inspectEnabledRoutes(cfg inspectClientConfig) []inspectRouteConfigFile {
	routes := make([]inspectRouteConfigFile, 0, len(cfg.Routes))
	for idx, route := range cfg.Routes {
		enabled := true
		if route.Enabled != nil {
			enabled = *route.Enabled
		}
		if !enabled {
			continue
		}
		if route.PublicHost == "" {
			route.PublicHost = cfg.PublicHost
		}
		if route.PublicPort == 0 {
			route.PublicPort = cfg.PublicPort
		}
		if route.ID == "" {
			route.ID = fmt.Sprintf("route_%d", idx+1)
		}
		routes = append(routes, route)
	}
	return routes
}

func inspectSelectRoute(cfg inspectClientConfig, requestedID string) (inspectRouteConfigFile, error) {
	routes := inspectEnabledRoutes(cfg)
	for _, route := range routes {
		if requestedID != "" && route.ID == requestedID {
			return route, nil
		}
	}
	if requestedID != "" {
		return inspectRouteConfigFile{}, fmt.Errorf("route_id %q not found or not enabled", requestedID)
	}
	if len(routes) == 0 {
		return inspectRouteConfigFile{}, errors.New("no enabled routes found in config")
	}
	return routes[0], nil
}

func inspectLoadAllClientConfigs(path string) ([]inspectClientConfig, time.Duration, error) {
	if *inspectRouteID != "" {
		return nil, 0, errors.New("--all cannot be combined with --route-id")
	}
	cfg, openTimeout, err := inspectLoadClientConfigFile(path)
	if err != nil {
		return nil, 0, err
	}
	if len(cfg.Routes) == 0 {
		if err := inspectValidateSelectedConfig(cfg); err != nil {
			return nil, 0, err
		}
		return []inspectClientConfig{cfg}, openTimeout, nil
	}

	routes := inspectEnabledRoutes(cfg)
	if len(routes) == 0 {
		return nil, 0, errors.New("no enabled routes found in config")
	}
	configs := make([]inspectClientConfig, 0, len(routes))
	for _, route := range routes {
		routeCfg := inspectApplyRoute(cfg, route)
		if err := inspectValidateSelectedConfig(routeCfg); err != nil {
			return nil, 0, fmt.Errorf("route %q: %w", route.ID, err)
		}
		configs = append(configs, routeCfg)
	}
	return configs, openTimeout, nil
}

func inspectReadLine(conn net.Conn, limit int) (string, error) {
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

func inspectWriteString(conn net.Conn, value string) error {
	_, err := io.WriteString(conn, value)
	return err
}

func inspectWriteFull(w io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := w.Write(payload)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func inspectWriteFrame(w io.Writer, typ byte, streamID uint32, payload []byte) error {
	header := make([]byte, inspectFrameHeaderSize)
	header[0] = typ
	binary.BigEndian.PutUint32(header[1:5], streamID)
	binary.BigEndian.PutUint32(header[5:9], uint32(len(payload)))
	if err := inspectWriteFull(w, header); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	return inspectWriteFull(w, payload)
}

func inspectReadFrame(r io.Reader) (inspectFrame, error) {
	var header [inspectFrameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return inspectFrame{}, err
	}
	n := binary.BigEndian.Uint32(header[5:9])
	if n > 16*1024*1024 {
		return inspectFrame{}, fmt.Errorf("frame too large: %d", n)
	}

	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return inspectFrame{}, err
	}
	return inspectFrame{
		typ:      header[0],
		streamID: binary.BigEndian.Uint32(header[1:5]),
		payload:  payload,
	}, nil
}

func inspectEncodeOpenPayload(host string, port uint16) []byte {
	hostBytes := []byte(host)
	payload := make([]byte, 2+len(hostBytes)+2)
	binary.BigEndian.PutUint16(payload[0:2], uint16(len(hostBytes)))
	copy(payload[2:2+len(hostBytes)], hostBytes)
	binary.BigEndian.PutUint16(payload[2+len(hostBytes):], port)
	return payload
}

func inspectStartRelayRequest(ctx context.Context, cfg inspectClientConfig, sid string, clientPort int) <-chan inspectRelayResult {
	results := make(chan inspectRelayResult, 1)
	go func() {
		values := url.Values{}
		values.Set("action", "session")
		values.Set("sid", sid)
		values.Set("client_host", cfg.PublicHost)
		values.Set("client_port", fmt.Sprintf("%d", clientPort))
		values.Set("server_host", cfg.ServerHost)
		values.Set("server_port", fmt.Sprintf("%d", cfg.ServerPort))

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.RelayURL+"?"+values.Encode(), nil)
		if err != nil {
			results <- inspectRelayResult{err: inspectStageError("relay request", fmt.Errorf("build request: %w", err))}
			return
		}
		req.Header.Set("X-API-KEY", cfg.APIKey)

		client := &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:          4,
				MaxIdleConnsPerHost:   1,
				MaxConnsPerHost:       1,
				IdleConnTimeout:       30 * time.Second,
				DisableCompression:    true,
				DisableKeepAlives:     false,
				ForceAttemptHTTP2:     false,
				ResponseHeaderTimeout: inspectRelayWaitTimeout,
				TLSHandshakeTimeout:   15 * time.Second,
			},
		}

		resp, err := client.Do(req)
		if err != nil {
			results <- inspectRelayResult{err: inspectStageError("relay request", err)}
			return
		}

		if resp.StatusCode != http.StatusOK {
			defer resp.Body.Close()
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			detail := strings.TrimSpace(string(body))

			var relayErr struct {
				Error  string `json:"error"`
				Detail string `json:"detail"`
			}
			if json.Unmarshal(body, &relayErr) == nil {
				switch {
				case relayErr.Detail != "":
					detail = relayErr.Detail
				case relayErr.Error != "":
					detail = relayErr.Error
				}
			}
			if detail == "" {
				detail = fmt.Sprintf("unexpected status %d", resp.StatusCode)
			}
			if strings.Contains(detail, "ERR session-limit") {
				detail = "server session limit reached; increase server max_sessions so it stays above client session_count when inspect runs alongside furo-client"
			}

			results <- inspectRelayResult{err: inspectStageError("relay request", fmt.Errorf("status %d: %s", resp.StatusCode, detail))}
			return
		}

		reader := bufio.NewReader(resp.Body)
		line, err := reader.ReadString('\n')
		if err != nil {
			resp.Body.Close()
			results <- inspectRelayResult{err: inspectStageError("relay request", fmt.Errorf("read relay confirmation: %w", err))}
			return
		}
		if strings.TrimSpace(line) != "OK" {
			resp.Body.Close()
			results <- inspectRelayResult{err: inspectStageError("relay request", fmt.Errorf("unexpected relay confirmation %q", strings.TrimSpace(line)))}
			return
		}

		results <- inspectRelayResult{body: resp.Body}
	}()
	return results
}

func inspectListen(cfg inspectClientConfig) (net.Listener, int, string, error) {
	ln, err := net.Listen("tcp", cfg.AgentListen)
	if err == nil {
		port := cfg.PublicPort
		if tcpAddr, ok := ln.Addr().(*net.TCPAddr); ok && tcpAddr.Port != 0 {
			port = tcpAddr.Port
		}
		return ln, port, fmt.Sprintf("listening on %s", ln.Addr().String()), nil
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		return nil, 0, "", err
	}

	host, _, splitErr := net.SplitHostPort(cfg.AgentListen)
	if splitErr != nil {
		return nil, 0, "", fmt.Errorf("split %s: %w", cfg.AgentListen, splitErr)
	}

	fallbackAddr := net.JoinHostPort(host, "0")
	ln, fallbackErr := net.Listen("tcp", fallbackAddr)
	if fallbackErr != nil {
		return nil, 0, "", fmt.Errorf("primary listen failed (%v), fallback listen on %s failed: %w", err, fallbackAddr, fallbackErr)
	}

	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok || tcpAddr.Port == 0 {
		_ = ln.Close()
		return nil, 0, "", fmt.Errorf("fallback listener returned unexpected addr %s", ln.Addr().String())
	}

	summary := fmt.Sprintf(
		"primary %s is busy; using temporary listener %s and advertising public port %d",
		cfg.AgentListen,
		ln.Addr().String(),
		tcpAddr.Port,
	)
	return ln, tcpAddr.Port, summary, nil
}

func inspectAttachRelay(conn net.Conn, apiKey, sid string) error {
	if err := conn.SetDeadline(time.Now().Add(inspectRelayWaitTimeout)); err != nil {
		return err
	}
	defer conn.SetDeadline(time.Time{})

	line, err := inspectReadLine(conn, inspectLineLimit)
	if err != nil {
		return err
	}
	parts := strings.Fields(line)
	if len(parts) != 3 || parts[0] != "SESSION" {
		return fmt.Errorf("unexpected relay greeting %q", line)
	}
	if parts[1] != apiKey {
		return errors.New("relay presented an unexpected api key")
	}
	if parts[2] != sid {
		return fmt.Errorf("relay presented wrong session id %q", parts[2])
	}
	return inspectWriteString(conn, "OK\n")
}

func inspectApplicationPings(ctx context.Context, session *inspectSession) (time.Duration, []time.Duration, error) {
	samples := make([]time.Duration, 0, inspectPingCount)
	var total time.Duration
	for i := 0; i < inspectPingCount; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		rtt, err := session.ping(pingCtx)
		cancel()
		if err != nil {
			return 0, samples, inspectStageError("ping", err)
		}
		samples = append(samples, rtt)
		total += rtt
	}
	return total / time.Duration(len(samples)), samples, nil
}

func inspectRunSpeedTest(ctx context.Context, session *inspectSession, report *inspectReport, rawURL string, openTimeout time.Duration) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", inspectStageError("speed test", fmt.Errorf("parse url: %w", err))
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return "", inspectStageError("speed test", fmt.Errorf("unsupported scheme %q", parsed.Scheme))
	}

	host := parsed.Hostname()
	port := parsed.Port()
	switch {
	case port != "":
	case parsed.Scheme == "https":
		port = "443"
	default:
		port = "80"
	}

	portNum, err := net.LookupPort("tcp", port)
	if err != nil {
		return "", inspectStageError("speed test", fmt.Errorf("parse port: %w", err))
	}

	openCtx, cancel := context.WithTimeout(ctx, openTimeout)
	stream, err := session.openStream(openCtx, host, uint16(portNum))
	cancel()
	if err != nil {
		return "", inspectStageError("speed test", fmt.Errorf("open stream to %s:%s: %w", host, port, err))
	}
	defer stream.Close()

	var transportConn net.Conn = stream
	if parsed.Scheme == "https" {
		tlsConn := tls.Client(stream, &tls.Config{ServerName: host})
		handshakeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err := tlsConn.HandshakeContext(handshakeCtx); err != nil {
			return "", inspectStageError("speed test", fmt.Errorf("tls handshake: %w", err))
		}
		defer tlsConn.Close()
		transportConn = tlsConn
	}

	targetPath := parsed.RequestURI()
	if targetPath == "" {
		targetPath = "/"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", inspectStageError("speed test", fmt.Errorf("build request: %w", err))
	}

	request := fmt.Sprintf(
		"GET %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: furo-inspect\r\nAccept: */*\r\nConnection: close\r\n\r\n",
		targetPath,
		parsed.Host,
	)
	if err := inspectWriteString(transportConn, request); err != nil {
		return "", inspectStageError("speed test", fmt.Errorf("send request: %w", err))
	}

	reader := bufio.NewReader(transportConn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		return "", inspectStageError("speed test", fmt.Errorf("read response: %w", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", inspectStageError("speed test", fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body))))
	}

	report.note("Speed test", fmt.Sprintf("downloading from %s", rawURL))

	start := time.Now()
	totalBytes := resp.ContentLength
	var downloaded int64
	buf := make([]byte, 32*1024)
	lastUpdate := time.Time{}
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			downloaded += int64(n)
			now := time.Now()
			if lastUpdate.IsZero() || now.Sub(lastUpdate) >= 200*time.Millisecond {
				report.updateProgress("Speed test", downloaded, totalBytes, now.Sub(start))
				lastUpdate = now
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return "", inspectStageError("speed test", fmt.Errorf("download: %w", readErr))
		}
	}
	elapsed := time.Since(start)
	if elapsed <= 0 {
		elapsed = time.Nanosecond
	}
	report.updateProgress("Speed test", downloaded, totalBytes, elapsed)

	mbps := float64(downloaded*8) / elapsed.Seconds() / 1_000_000
	mibps := float64(downloaded) / elapsed.Seconds() / (1024 * 1024)
	return fmt.Sprintf("%.2f Mbps (%.2f MiB/s over %s in %s)", mbps, mibps, inspectFormatBytes(downloaded), elapsed.Round(time.Millisecond)), nil
}

func inspectRun(ctx context.Context, cfg inspectClientConfig, openTimeout time.Duration, runSpeedTest bool, speedURL string, report *inspectReport) error {
	if cfg.SelectedRouteID != "" {
		report.add("Route", cfg.SelectedRouteID)
	}
	report.add("Client callback", fmt.Sprintf("%s:%d", cfg.PublicHost, cfg.PublicPort))
	report.add("Server agent", fmt.Sprintf("%s:%d", cfg.ServerHost, cfg.ServerPort))

	ln, listenPort, listenSummary, err := inspectListen(cfg)
	if err != nil {
		return inspectStageError("listener bind", fmt.Errorf("listen on %s: %w", cfg.AgentListen, err))
	}
	defer ln.Close()
	report.add("Listener", listenSummary)

	sid := fmt.Sprintf("inspect_%d", time.Now().UnixNano())
	report.add("Session", sid)

	relayCtx, relayCancel := context.WithCancel(ctx)
	defer relayCancel()
	relayResults := inspectStartRelayRequest(relayCtx, cfg, sid, listenPort)

	acceptCh := make(chan inspectAcceptResult, 1)
	go func() {
		conn, err := ln.Accept()
		acceptCh <- inspectAcceptResult{conn: conn, err: err}
	}()

	var relayBody io.Closer
	defer func() {
		if relayBody != nil {
			_ = relayBody.Close()
		}
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, inspectAcceptTimeout)
	defer waitCancel()

	var conn net.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	var relayReady bool
	var relayErr error

	for conn == nil || !relayReady {
		select {
		case result := <-acceptCh:
			if result.err != nil {
				if relayErr != nil {
					return relayErr
				}
				return inspectStageError("relay callback", result.err)
			}
			if conn == nil {
				conn = result.conn
				if err := inspectAttachRelay(conn, cfg.APIKey, sid); err != nil {
					conn.Close()
					return inspectStageError("relay callback", err)
				}
				report.add("Relay callback", "relay connected back and session attach succeeded")
			}
		case result := <-relayResults:
			if result.err != nil {
				relayErr = result.err
				if conn == nil {
					return relayErr
				}
				return relayErr
			}
			relayReady = true
			relayBody = result.body
			report.add("Relay request", "relay accepted the session and connected both sides")
		case <-waitCtx.Done():
			if relayErr != nil {
				return relayErr
			}
			if conn == nil {
				return inspectStageError("relay callback", errors.New("timed out waiting for relay to connect back to agent_listen"))
			}
			if !relayReady {
				return inspectStageError("relay request", errors.New("timed out waiting for relay to confirm the bridged session"))
			}
			return waitCtx.Err()
		}
	}

	session := newInspectSession(conn)
	conn = nil
	defer session.close(net.ErrClosed)
	go session.readLoop()

	if err := session.sendFrame(inspectFrameHello, 0, []byte(cfg.APIKey)); err != nil {
		return inspectStageError("server handshake", fmt.Errorf("send hello: %w", err))
	}
	helloCtx, cancel := context.WithTimeout(ctx, inspectHelloTimeout)
	err = session.waitHelloAck(helloCtx)
	cancel()
	if err != nil {
		return inspectStageError("server handshake", fmt.Errorf("wait hello ack: %w", err))
	}
	report.add("Server handshake", "received HELLO_ACK from the server agent")

	avgPing, samples, err := inspectApplicationPings(ctx, session)
	if err != nil {
		return err
	}
	parts := make([]string, 0, len(samples))
	for _, sample := range samples {
		parts = append(parts, sample.Round(time.Millisecond).String())
	}
	report.add("Ping", fmt.Sprintf("avg=%s samples=%s", avgPing.Round(time.Millisecond), strings.Join(parts, ", ")))

	if runSpeedTest {
		speedSummary, err := inspectRunSpeedTest(ctx, session, report, speedURL, openTimeout)
		if err != nil {
			return err
		}
		report.add("Speed test", speedSummary)
	}

	return nil
}

func main() {
	flag.Parse()
	report := newInspectReport(os.Stdout)
	report.start()

	if *inspectAllRoutes {
		cfgs, openTimeout, err := inspectLoadAllClientConfigs(*inspectConfigPath)
		if err != nil {
			report.printResult(inspectStageError("config", err))
			os.Exit(1)
		}
		var failed bool
		for idx, cfg := range cfgs {
			if idx > 0 {
				fmt.Fprintln(os.Stdout)
			}
			report.reset()
			err = inspectRun(context.Background(), cfg, openTimeout, *speedTestEnabled, *speedTestURL, report)
			report.printResult(err)
			if err != nil {
				failed = true
			}
		}
		if failed {
			os.Exit(1)
		}
		return
	}

	cfg, openTimeout, err := inspectLoadClientConfig(*inspectConfigPath)
	if err != nil {
		report.printResult(inspectStageError("config", err))
		os.Exit(1)
	}

	err = inspectRun(context.Background(), cfg, openTimeout, *speedTestEnabled, *speedTestURL, report)
	report.printResult(err)
	if err != nil {
		os.Exit(1)
	}
}
