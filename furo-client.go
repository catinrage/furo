package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
	defaultMaxFramePayload = 128 * 1024
	defaultMinFramePayload = 32 * 1024
	defaultMidFramePayload = 64 * 1024
	defaultWriteTimeout    = 30 * time.Second
	sessionPollDelay       = 1200 * time.Millisecond
	streamWriteQueueDepth  = 32
	controlWriteQueueDepth = 256
	slowWriteThreshold     = 200 * time.Millisecond
	sessionStatsInterval   = 15 * time.Second
	reconnectBackoffMin    = 1 * time.Second
	reconnectBackoffMax    = 30 * time.Second
	adminReadHeaderTimeout = 5 * time.Second
	maxReservedSessions    = 2
	heartbeatInterval      = 15 * time.Second
	heartbeatTimeout       = 45 * time.Second
	recentSlowPenaltyAge   = 30 * time.Second
	relayRequestTimeoutPad = 15 * time.Second
)

var (
	configPath       = flag.String("c", "config.client.json", "Path to client config JSON")
	showVersion      = flag.Bool("version", false, "Print version and exit")
	pingMaster       = flag.Bool("ping-master", false, "Ping configured master URL or relay route-map cache and exit")
	pingMasterURL    = flag.String("ping-master-url", "", "Override master URL for --ping-master")
	apiKey           string
	socksListen      string
	socksAuth        string
	agentListen      string
	publicHost       string
	publicPort       int
	serverHost       string
	serverPort       int
	adminListen      string
	openTimeout      time.Duration
	keepalivePeriod  time.Duration
	writeTimeout     = defaultWriteTimeout
	minFramePayload  = defaultMinFramePayload
	midFramePayload  = defaultMidFramePayload
	maxFramePayload  = defaultMaxFramePayload
	sessionCount     int
	logFilePath      string
	clientID         string
	routeSelection   routeSelectionStrategy
	relayURL         string
	clientRoutes     []clientRouteConfig
	airsSettings     clientAIRSConfig
	routeMapSettings clientRouteMapConfig
)

var (
	appVersion   = "dev"
	appCommit    = "unknown"
	appBuildDate = "unknown"
)

type clientConfigFile struct {
	ClientID           string                  `json:"client_id"`
	RouteSelection     string                  `json:"route_selection"`
	Routes             []clientRouteConfigFile `json:"routes"`
	RelayURL           string                  `json:"relay_url"`
	APIKey             string                  `json:"api_key"`
	SOCKSListen        string                  `json:"socks_listen"`
	SOCKSAuth          string                  `json:"socks_auth"`
	AgentListen        string                  `json:"agent_listen"`
	PublicHost         string                  `json:"public_host"`
	PublicPort         int                     `json:"public_port"`
	ServerHost         string                  `json:"server_host"`
	ServerPort         int                     `json:"server_port"`
	AdminListen        string                  `json:"admin_listen"`
	ControlPanelListen string                  `json:"control_panel_listen,omitempty"`
	OpenTimeout        string                  `json:"open_timeout"`
	Keepalive          string                  `json:"keepalive"`
	WriteTimeout       string                  `json:"write_timeout"`
	FrameMinSize       int                     `json:"frame_min_size"`
	FrameMidSize       int                     `json:"frame_mid_size"`
	FrameMaxSize       int                     `json:"frame_max_size"`
	SessionCount       int                     `json:"session_count"`
	LogFile            string                  `json:"log_file"`
	AIRS               clientAIRSConfig        `json:"airs"`
	MasterRoutes       clientRouteMapConfig    `json:"master_routes"`
}

type clientAIRSConfig struct {
	InspectBinary     string `json:"inspect_binary,omitempty"`
	ServiceScript     string `json:"service_script,omitempty"`
	ClientServiceRole string `json:"client_service_role,omitempty"`
}

type clientRouteConfigFile struct {
	ID           string `json:"id"`
	RelayURL     string `json:"relay_url"`
	PublicHost   string `json:"public_host"`
	PublicPort   int    `json:"public_port"`
	ServerHost   string `json:"server_host"`
	ServerPort   int    `json:"server_port"`
	SessionCount int    `json:"session_count"`
	Enabled      *bool  `json:"enabled"`
	ManagedBy    string `json:"managed_by,omitempty"`
	Role         string `json:"role,omitempty"`
	Generation   int64  `json:"generation,omitempty"`
}

type clientRouteMapConfig struct {
	Enabled              bool   `json:"enabled"`
	Namespace            string `json:"namespace,omitempty"`
	FleetID              string `json:"fleet_id,omitempty"`
	MasterURL            string `json:"master_url,omitempty"`
	PollIntervalSeconds  int    `json:"poll_interval_seconds,omitempty"`
	FailoverGraceSeconds int    `json:"failover_grace_seconds,omitempty"`
}

type routeSelectionStrategy string

const (
	routeSelectionRoundRobin routeSelectionStrategy = "round_robin"
	routeSelectionRandom     routeSelectionStrategy = "random"
	routeSelectionLeastLoad  routeSelectionStrategy = "least_load"
	routeSelectionLeastRTT   routeSelectionStrategy = "least_latency"
)

type clientRouteConfig struct {
	ID           string
	RelayURL     string
	PublicHost   string
	PublicPort   int
	ServerHost   string
	ServerPort   int
	SessionCount int
	Enabled      bool
	ManagedBy    string
	Role         string
	Generation   int64
}

type clientRouteState struct {
	cfg clientRouteConfig

	latencyMicros int64
}

func (r *clientRouteState) observeRequestSuccess(duration time.Duration) {
	if duration <= 0 {
		return
	}
	sample := duration.Microseconds()
	current := atomic.LoadInt64(&r.latencyMicros)
	if current == 0 {
		atomic.StoreInt64(&r.latencyMicros, sample)
		return
	}
	atomic.StoreInt64(&r.latencyMicros, ((current*7)+sample)/8)
}

func (r *clientRouteState) latency() time.Duration {
	micros := atomic.LoadInt64(&r.latencyMicros)
	if micros <= 0 {
		return 0
	}
	return time.Duration(micros) * time.Microsecond
}

func defaultClientConfig() clientConfigFile {
	return clientConfigFile{
		RouteSelection: string(routeSelectionLeastLoad),
		RelayURL:       "https://hidaco.site/tools/rel/soc/furo-relay.php",
		APIKey:         "my_super_secret_123456789",
		SOCKSListen:    "0.0.0.0:18713",
		SOCKSAuth:      "",
		AgentListen:    "0.0.0.0:28080",
		PublicPort:     28080,
		ServerPort:     28081,
		AdminListen:    "",
		OpenTimeout:    "45s",
		Keepalive:      "30s",
		WriteTimeout:   defaultWriteTimeout.String(),
		FrameMinSize:   defaultMinFramePayload,
		FrameMidSize:   defaultMidFramePayload,
		FrameMaxSize:   defaultMaxFramePayload,
		SessionCount:   8,
		LogFile:        "",
		AIRS: clientAIRSConfig{
			InspectBinary:     "inspect",
			ServiceScript:     "service.sh",
			ClientServiceRole: "client",
		},
		MasterRoutes: clientRouteMapConfig{
			Enabled:              false,
			Namespace:            "default",
			PollIntervalSeconds:  30,
			FailoverGraceSeconds: 5,
		},
	}
}

func validateFrameConfig(minSize, midSize, maxSize int) error {
	switch {
	case minSize < 4096:
		return errors.New("frame_min_size must be >= 4096")
	case midSize < minSize:
		return errors.New("frame_mid_size must be >= frame_min_size")
	case maxSize < midSize:
		return errors.New("frame_max_size must be >= frame_mid_size")
	case maxSize > 1024*1024:
		return errors.New("frame_max_size must be <= 1048576")
	}
	return nil
}

func validateSOCKSAuth(value string) error {
	user, pass, ok := strings.Cut(strings.TrimSpace(value), ":")
	if !ok || user == "" || pass == "" {
		return errors.New("socks_auth must use user:pass format or be empty")
	}
	if len(user) > 255 || len(pass) > 255 {
		return errors.New("socks_auth username and password must be <= 255 bytes")
	}
	return nil
}

func parseSOCKSAuth(value string) (string, string, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", "", false
	}
	user, pass, ok := strings.Cut(trimmed, ":")
	return user, pass, ok
}

func defaultClientID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "client"
	}
	return sanitizeSessionComponent(hostname)
}

func sanitizeSessionComponent(value string) string {
	if value == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(value))
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '-' || r == '.':
			b.WriteRune(r)
		}
	}
	return b.String()
}

func parseRouteSelection(value string) (routeSelectionStrategy, error) {
	switch routeSelectionStrategy(value) {
	case routeSelectionRoundRobin, routeSelectionRandom, routeSelectionLeastLoad, routeSelectionLeastRTT:
		return routeSelectionStrategy(value), nil
	default:
		return "", fmt.Errorf("unsupported route_selection %q", value)
	}
}

func buildClientRoutes(cfg clientConfigFile) ([]clientRouteConfig, int, error) {
	if len(cfg.Routes) == 0 {
		return []clientRouteConfig{{
			ID:           "primary",
			RelayURL:     cfg.RelayURL,
			PublicHost:   cfg.PublicHost,
			PublicPort:   cfg.PublicPort,
			ServerHost:   cfg.ServerHost,
			ServerPort:   cfg.ServerPort,
			SessionCount: cfg.SessionCount,
			Enabled:      true,
		}}, cfg.SessionCount, nil
	}

	routes := make([]clientRouteConfig, 0, len(cfg.Routes))
	totalSessions := 0
	seenIDs := make(map[string]struct{}, len(cfg.Routes))
	for idx, route := range cfg.Routes {
		routeID := sanitizeSessionComponent(route.ID)
		if routeID == "" {
			routeID = fmt.Sprintf("route_%d", idx+1)
		}
		if _, exists := seenIDs[routeID]; exists {
			return nil, 0, fmt.Errorf("duplicate route id %q", routeID)
		}
		seenIDs[routeID] = struct{}{}

		enabled := true
		if route.Enabled != nil {
			enabled = *route.Enabled
		}

		publicHostValue := route.PublicHost
		if publicHostValue == "" {
			publicHostValue = cfg.PublicHost
		}
		publicPortValue := route.PublicPort
		if publicPortValue == 0 {
			publicPortValue = cfg.PublicPort
		}

		switch {
		case route.RelayURL == "":
			return nil, 0, fmt.Errorf("routes[%d].relay_url is required", idx)
		case publicHostValue == "":
			return nil, 0, fmt.Errorf("routes[%d].public_host is required", idx)
		case publicPortValue < 1 || publicPortValue > 65535:
			return nil, 0, fmt.Errorf("routes[%d].public_port must be between 1 and 65535", idx)
		case route.ServerHost == "":
			return nil, 0, fmt.Errorf("routes[%d].server_host is required", idx)
		case route.ServerPort < 1 || route.ServerPort > 65535:
			return nil, 0, fmt.Errorf("routes[%d].server_port must be between 1 and 65535", idx)
		case route.SessionCount < 1:
			return nil, 0, fmt.Errorf("routes[%d].session_count must be >= 1", idx)
		}

		routeCfg := clientRouteConfig{
			ID:           routeID,
			RelayURL:     route.RelayURL,
			PublicHost:   publicHostValue,
			PublicPort:   publicPortValue,
			ServerHost:   route.ServerHost,
			ServerPort:   route.ServerPort,
			SessionCount: route.SessionCount,
			Enabled:      enabled,
			ManagedBy:    route.ManagedBy,
			Role:         route.Role,
			Generation:   route.Generation,
		}
		routes = append(routes, routeCfg)
		if routeCfg.Enabled {
			totalSessions += routeCfg.SessionCount
		}
	}

	if totalSessions < 1 {
		return nil, 0, errors.New("at least one enabled route with session_count >= 1 is required")
	}
	return routes, totalSessions, nil
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

	if cfg.APIKey == "" {
		return errors.New("api_key is required")
	}
	if cfg.SOCKSListen == "" {
		return errors.New("socks_listen is required")
	}
	if strings.TrimSpace(cfg.SOCKSAuth) != "" {
		if err := validateSOCKSAuth(cfg.SOCKSAuth); err != nil {
			return err
		}
	}
	if cfg.AgentListen == "" {
		return errors.New("agent_listen is required")
	}
	parsedOpenTimeout, err := time.ParseDuration(cfg.OpenTimeout)
	if err != nil {
		return fmt.Errorf("parse open_timeout: %w", err)
	}
	parsedKeepalive, err := time.ParseDuration(cfg.Keepalive)
	if err != nil {
		return fmt.Errorf("parse keepalive: %w", err)
	}
	parsedWriteTimeout, err := time.ParseDuration(cfg.WriteTimeout)
	if err != nil {
		return fmt.Errorf("parse write_timeout: %w", err)
	}
	if err := validateFrameConfig(cfg.FrameMinSize, cfg.FrameMidSize, cfg.FrameMaxSize); err != nil {
		return err
	}

	parsedRouteSelection, err := parseRouteSelection(cfg.RouteSelection)
	if err != nil {
		return err
	}
	parsedRoutes, totalSessions, err := buildClientRoutes(cfg)
	if err != nil {
		return err
	}
	resolvedClientID := sanitizeSessionComponent(cfg.ClientID)
	if resolvedClientID == "" {
		resolvedClientID = defaultClientID()
	}

	apiKey = cfg.APIKey
	socksListen = cfg.SOCKSListen
	socksAuth = strings.TrimSpace(cfg.SOCKSAuth)
	agentListen = cfg.AgentListen
	publicHost = parsedRoutes[0].PublicHost
	publicPort = parsedRoutes[0].PublicPort
	serverHost = parsedRoutes[0].ServerHost
	serverPort = parsedRoutes[0].ServerPort
	adminListen = cfg.AdminListen
	if adminListen == "" && cfg.ControlPanelListen != "" {
		adminListen = cfg.ControlPanelListen
	}
	openTimeout = parsedOpenTimeout
	keepalivePeriod = parsedKeepalive
	writeTimeout = parsedWriteTimeout
	minFramePayload = cfg.FrameMinSize
	midFramePayload = cfg.FrameMidSize
	maxFramePayload = cfg.FrameMaxSize
	sessionCount = totalSessions
	logFilePath = cfg.LogFile
	clientID = resolvedClientID
	routeSelection = parsedRouteSelection
	relayURL = parsedRoutes[0].RelayURL
	clientRoutes = parsedRoutes
	airsSettings = cfg.AIRS
	if airsSettings.InspectBinary == "" {
		airsSettings.InspectBinary = "inspect"
	}
	if airsSettings.ServiceScript == "" {
		airsSettings.ServiceScript = "service.sh"
	}
	if airsSettings.ClientServiceRole == "" {
		airsSettings.ClientServiceRole = "client"
	}
	routeMapSettings = cfg.MasterRoutes
	routeMapSettings.Namespace = sanitizeSessionComponent(routeMapSettings.Namespace)
	if routeMapSettings.Namespace == "" {
		routeMapSettings.Namespace = "default"
	}
	if routeMapSettings.PollIntervalSeconds == 0 {
		routeMapSettings.PollIntervalSeconds = 30
	}
	if routeMapSettings.FailoverGraceSeconds == 0 {
		routeMapSettings.FailoverGraceSeconds = 5
	}
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

func clientVersionString() string {
	return fmt.Sprintf("furo-client version=%s commit=%s built=%s", appVersion, appCommit, appBuildDate)
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

type MuxSession struct {
	pool  *SessionPool
	sid   string
	idx   int
	route *clientRouteState

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

func newMuxSession(pool *SessionPool, route *clientRouteState, idx int, sid string) *MuxSession {
	return &MuxSession{
		pool:       pool,
		route:      route,
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

	req := acquireOutboundFrame(typ, streamID, payload, nil)

	select {
	case <-stop:
		releaseOutboundFrame(req)
		return net.ErrClosed
	case s.controlQ <- req:
	}

	err := <-req.result
	releaseOutboundFrame(req)
	return err
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

	req := acquireOutboundFrame(frameData, streamID, payload, release)
	if err := s.enqueueDataFrame(req); err != nil {
		if release != nil {
			release()
		}
		releaseOutboundFrame(req)
		return err
	}
	err := <-req.result
	releaseOutboundFrame(req)
	return err
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
	values.Set("client_host", s.route.cfg.PublicHost)
	values.Set("client_port", fmt.Sprintf("%d", s.route.cfg.PublicPort))
	values.Set("server_host", s.route.cfg.ServerHost)
	values.Set("server_port", fmt.Sprintf("%d", s.route.cfg.ServerPort))

	requestCtx, cancel := context.WithTimeout(context.Background(), openTimeout+relayRequestTimeoutPad)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, s.route.cfg.RelayURL+"?"+values.Encode(), nil)
	if err != nil {
		delay := s.markRequestFailure(err.Error())
		atomic.AddUint64(&relayRequestsFailed, 1)
		logEvent("[CLIENT] route=%s session=%s request_build_failed next_retry_ms=%d err=%v", s.route.cfg.ID, s.sid, delay.Milliseconds(), err)
		return
	}
	req.Header.Set("X-API-KEY", apiKey)

	start := time.Now()
	atomic.AddUint64(&relayRequestsStarted, 1)
	resp, err := httpClient.Do(req)
	if err != nil {
		delay := s.markRequestFailure(err.Error())
		atomic.AddUint64(&relayRequestsFailed, 1)
		logEvent("[CLIENT] route=%s session=%s request_failed dur_ms=%d next_retry_ms=%d err=%v", s.route.cfg.ID, s.sid, time.Since(start).Milliseconds(), delay.Milliseconds(), err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		delay := s.markRequestFailure(fmt.Sprintf("relay rejected with status %d", resp.StatusCode))
		atomic.AddUint64(&relayRequestsRejected, 1)
		logEvent("[CLIENT] route=%s session=%s request_rejected status=%d next_retry_ms=%d body=%q", s.route.cfg.ID, s.sid, resp.StatusCode, delay.Milliseconds(), strings.TrimSpace(string(body)))
		return
	}

	s.markRequestSuccess()
	s.route.observeRequestSuccess(time.Since(start))
	atomic.AddUint64(&relayRequestsSucceeded, 1)
	logEvent("[CLIENT] route=%s session=%s request_ok dur_ms=%d", s.route.cfg.ID, s.sid, time.Since(start).Milliseconds())
	_, _ = io.Copy(io.Discard, resp.Body)
	logEvent("[CLIENT] route=%s session=%s request_body_closed", s.route.cfg.ID, s.sid)
}

func (s *MuxSession) loop() {
	for {
		s.mu.Lock()
		connected := s.conn != nil
		running := s.requestRunning
		s.mu.Unlock()

		if !s.route.cfg.Enabled {
			if connected {
				s.closeSession(0, errors.New("route disabled"))
			}
			timer := time.NewTimer(sessionPollDelay)
			select {
			case <-timer.C:
			case <-s.loopWake:
				if !timer.Stop() {
					<-timer.C
				}
			}
			continue
		}

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
	mu                      sync.RWMutex
	sessions                map[string]*MuxSession
	order                   []*MuxSession
	routes                  []*clientRouteState
	strategy                routeSelectionStrategy
	rr                      uint32
	stateChange             chan struct{}
	localFailoverGeneration int64
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
	RouteID          string `json:"route_id"`
}

type clientStatusResponse struct {
	Service        string                  `json:"service"`
	Version        string                  `json:"version"`
	Commit         string                  `json:"commit"`
	BuildDate      string                  `json:"build_date"`
	StartedAt      string                  `json:"started_at"`
	UptimeSec      int64                   `json:"uptime_sec"`
	RelayURL       string                  `json:"relay_url"`
	SOCKSListen    string                  `json:"socks_listen"`
	AgentListen    string                  `json:"agent_listen"`
	AdminListen    string                  `json:"admin_listen,omitempty"`
	ClientID       string                  `json:"client_id"`
	RouteSelection string                  `json:"route_selection"`
	SessionCount   int                     `json:"session_count"`
	WriteTimeout   string                  `json:"write_timeout"`
	FrameMinSize   int                     `json:"frame_min_size"`
	FrameMidSize   int                     `json:"frame_mid_size"`
	FrameMaxSize   int                     `json:"frame_max_size"`
	RelayRequests  clientRelayRequestStats `json:"relay_requests"`
	Totals         clientTotals            `json:"totals"`
	Routes         []clientRouteStatus     `json:"routes"`
	Sessions       []clientSessionStatus   `json:"sessions"`
}

type clientRouteStatus struct {
	RouteID            string `json:"route_id"`
	RelayURL           string `json:"relay_url"`
	ServerHost         string `json:"server_host"`
	ServerPort         int    `json:"server_port"`
	SessionCount       int    `json:"session_count"`
	Enabled            bool   `json:"enabled"`
	ManagedBy          string `json:"managed_by,omitempty"`
	Role               string `json:"role,omitempty"`
	Generation         int64  `json:"generation,omitempty"`
	ConnectedSessions  int    `json:"connected_sessions"`
	ReadySessions      int    `json:"ready_sessions"`
	ActiveStreams      int    `json:"active_streams"`
	PendingFrames      int64  `json:"pending_frames"`
	PendingBytes       int64  `json:"pending_bytes"`
	EstimatedLatencyMs int64  `json:"estimated_latency_ms"`
}

type relayRouteMap struct {
	Namespace  string           `json:"namespace"`
	FleetID    string           `json:"fleet_id"`
	Generation int64            `json:"generation"`
	Active     *relayRouteSpec  `json:"active"`
	Standby    []relayRouteSpec `json:"standby"`
	Retired    []string         `json:"retired"`
	UpdatedAt  string           `json:"updated_at"`
}

type relayRouteSpec struct {
	ID           string `json:"id"`
	RelayURL     string `json:"relay_url"`
	PublicHost   string `json:"public_host,omitempty"`
	PublicPort   int    `json:"public_port,omitempty"`
	ServerHost   string `json:"server_host"`
	ServerPort   int    `json:"server_port"`
	SessionCount int    `json:"session_count"`
}

func managedByForRouteMap(routeMap relayRouteMap) string {
	if strings.TrimSpace(routeMap.FleetID) != "" {
		return strings.TrimSpace(routeMap.FleetID)
	}
	return "server-master"
}

func routeConfigFromRelaySpec(spec relayRouteSpec, managedBy, role string, generation int64, enabled bool, fallbackPublicHost string, fallbackPublicPort int) clientRouteConfig {
	sessionCountValue := spec.SessionCount
	if sessionCountValue <= 0 {
		sessionCountValue = 1
	}
	relayURLValue := spec.RelayURL
	if relayURLValue == "" {
		relayURLValue = relayURL
	}
	publicHostValue := spec.PublicHost
	if publicHostValue == "" {
		publicHostValue = fallbackPublicHost
	}
	publicPortValue := spec.PublicPort
	if publicPortValue == 0 {
		publicPortValue = fallbackPublicPort
	}
	return clientRouteConfig{
		ID:           sanitizeSessionComponent(spec.ID),
		RelayURL:     relayURLValue,
		PublicHost:   publicHostValue,
		PublicPort:   publicPortValue,
		ServerHost:   spec.ServerHost,
		ServerPort:   spec.ServerPort,
		SessionCount: sessionCountValue,
		Enabled:      enabled,
		ManagedBy:    managedBy,
		Role:         role,
		Generation:   generation,
	}
}

func relayRouteMapURL(base string, namespace ...string) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	values := parsed.Query()
	values.Set("action", "route-map")
	if len(namespace) > 0 && strings.TrimSpace(namespace[0]) != "" {
		values.Set("namespace", sanitizeSessionComponent(namespace[0]))
	}
	parsed.RawQuery = values.Encode()
	return parsed.String(), nil
}

func fetchRelayRouteMap(ctx context.Context, base string) (relayRouteMap, error) {
	endpoint, err := relayRouteMapURL(base, routeMapSettings.Namespace)
	if err != nil {
		return relayRouteMap{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return relayRouteMap{}, err
	}
	req.Header.Set("X-API-KEY", apiKey)
	resp, err := httpClient.Do(req)
	if err != nil {
		return relayRouteMap{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return relayRouteMap{}, fmt.Errorf("route-map status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var routeMap relayRouteMap
	if err := json.NewDecoder(io.LimitReader(resp.Body, 256*1024)).Decode(&routeMap); err != nil {
		return relayRouteMap{}, err
	}
	return routeMap, nil
}

func routeConfigMap(cfg clientRouteConfig) map[string]any {
	route := map[string]any{
		"id":            cfg.ID,
		"relay_url":     cfg.RelayURL,
		"server_host":   cfg.ServerHost,
		"server_port":   cfg.ServerPort,
		"session_count": cfg.SessionCount,
		"enabled":       cfg.Enabled,
	}
	if cfg.PublicHost != "" {
		route["public_host"] = cfg.PublicHost
	}
	if cfg.PublicPort > 0 {
		route["public_port"] = cfg.PublicPort
	}
	if cfg.ManagedBy != "" {
		route["managed_by"] = cfg.ManagedBy
	}
	if cfg.Role != "" {
		route["role"] = cfg.Role
	}
	if cfg.Generation > 0 {
		route["generation"] = cfg.Generation
	}
	return route
}

func persistRelayRouteMap(routeMap relayRouteMap) error {
	data, err := os.ReadFile(*configPath)
	if err != nil {
		return err
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	managedBy := managedByForRouteMap(routeMap)
	fallbackPublicHost := stringFromMap(raw, "public_host")
	fallbackPublicPort := intFromAny(raw["public_port"])
	managedIDs := make(map[string]struct{})
	if routeMap.Active != nil {
		if id := sanitizeSessionComponent(routeMap.Active.ID); id != "" {
			managedIDs[id] = struct{}{}
		}
	}
	for _, standby := range routeMap.Standby {
		if id := sanitizeSessionComponent(standby.ID); id != "" {
			managedIDs[id] = struct{}{}
		}
	}
	retired := make(map[string]struct{})
	for _, id := range routeMap.Retired {
		if sanitized := sanitizeSessionComponent(id); sanitized != "" {
			retired[sanitized] = struct{}{}
		}
	}
	routes := make([]any, 0)
	if existing, ok := raw["routes"].([]any); ok {
		for _, value := range existing {
			route, ok := value.(map[string]any)
			if !ok {
				continue
			}
			routeID := sanitizeSessionComponent(stringFromMap(route, "id"))
			routeManagedBy := stringFromMap(route, "managed_by")
			if _, isRetired := retired[routeID]; isRetired {
				continue
			}
			if routeManagedBy == managedBy {
				continue
			}
			if _, isNowManaged := managedIDs[routeID]; isNowManaged {
				continue
			}
			routes = append(routes, route)
		}
	}
	if routeMap.Active != nil {
		cfg := routeConfigFromRelaySpec(*routeMap.Active, managedBy, "active", routeMap.Generation, true, fallbackPublicHost, fallbackPublicPort)
		routes = append(routes, routeConfigMap(cfg))
	}
	for _, standby := range routeMap.Standby {
		cfg := routeConfigFromRelaySpec(standby, managedBy, "standby", routeMap.Generation, false, fallbackPublicHost, fallbackPublicPort)
		routes = append(routes, routeConfigMap(cfg))
	}
	raw["routes"] = routes
	delete(raw, "relay_url")
	delete(raw, "server_host")
	delete(raw, "server_port")
	delete(raw, "session_count")
	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	out = append(out, '\n')
	return os.WriteFile(*configPath, out, 0644)
}

func persistPromotedManagedRoute(promoted clientRouteConfig) error {
	data, err := os.ReadFile(*configPath)
	if err != nil {
		return err
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	existing, _ := raw["routes"].([]any)
	routes := make([]any, 0, len(existing))
	found := false
	for _, value := range existing {
		route, ok := value.(map[string]any)
		if !ok {
			continue
		}
		if stringFromMap(route, "managed_by") == promoted.ManagedBy {
			if sanitizeSessionComponent(stringFromMap(route, "id")) == promoted.ID {
				routes = append(routes, routeConfigMap(promoted))
				found = true
				continue
			}
			if stringFromMap(route, "role") == "active" {
				route["enabled"] = false
				route["role"] = "retired"
			}
		}
		routes = append(routes, route)
	}
	if !found {
		routes = append(routes, routeConfigMap(promoted))
	}
	raw["routes"] = routes
	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	out = append(out, '\n')
	return os.WriteFile(*configPath, out, 0644)
}

func newSessionPool(count int) *SessionPool {
	defaultRoute := clientRouteConfig{
		ID:           "primary",
		RelayURL:     relayURL,
		PublicHost:   publicHost,
		PublicPort:   publicPort,
		ServerHost:   serverHost,
		ServerPort:   serverPort,
		SessionCount: count,
		Enabled:      true,
	}
	return newSessionPoolForRoutes([]clientRouteConfig{defaultRoute}, routeSelectionLeastLoad)
}

func newSessionPoolForRoutes(routeCfgs []clientRouteConfig, strategy routeSelectionStrategy) *SessionPool {
	resolvedClientID := clientID
	if resolvedClientID == "" {
		resolvedClientID = "client"
	}
	totalSessions := 0
	for _, routeCfg := range routeCfgs {
		if routeCfg.Enabled {
			totalSessions += routeCfg.SessionCount
		}
	}
	p := &SessionPool{
		sessions:    make(map[string]*MuxSession, totalSessions),
		order:       make([]*MuxSession, 0, totalSessions),
		routes:      make([]*clientRouteState, 0, len(routeCfgs)),
		strategy:    strategy,
		stateChange: make(chan struct{}, 1),
	}
	for _, routeCfg := range routeCfgs {
		routeState := &clientRouteState{cfg: routeCfg}
		p.routes = append(p.routes, routeState)
		if !routeCfg.Enabled {
			continue
		}
		for i := 0; i < routeCfg.SessionCount; i++ {
			sid := fmt.Sprintf("%s__%s__sess_%d", resolvedClientID, routeCfg.ID, i+1)
			s := newMuxSession(p, routeState, i, sid)
			p.sessions[sid] = s
			p.order = append(p.order, s)
		}
	}
	return p
}

func (p *SessionPool) findRouteLocked(id string) *clientRouteState {
	for _, route := range p.routes {
		if route.cfg.ID == id {
			return route
		}
	}
	return nil
}

func (p *SessionPool) ensureRouteSessionsLocked(route *clientRouteState) {
	if !route.cfg.Enabled {
		return
	}
	existing := 0
	for _, session := range p.order {
		if session.route == route {
			existing++
		}
	}
	for i := existing; i < route.cfg.SessionCount; i++ {
		sid := fmt.Sprintf("%s__%s__sess_%d", clientID, route.cfg.ID, i+1)
		if _, exists := p.sessions[sid]; exists {
			continue
		}
		session := newMuxSession(p, route, i, sid)
		p.sessions[sid] = session
		p.order = append(p.order, session)
		go session.loop()
		go session.statsLoop()
	}
}

func (p *SessionPool) upsertManagedRoute(cfg clientRouteConfig) {
	if cfg.ID == "" || cfg.ServerHost == "" || cfg.ServerPort <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	route := p.findRouteLocked(cfg.ID)
	if route == nil {
		route = &clientRouteState{cfg: cfg}
		p.routes = append(p.routes, route)
	} else {
		route.cfg = cfg
	}
	p.ensureRouteSessionsLocked(route)
	p.notifyStateChange()
}

func (p *SessionPool) disableRetiredManagedRoutes(managedBy string, generation int64, keep map[string]struct{}, retired map[string]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, route := range p.routes {
		if route.cfg.ManagedBy != managedBy {
			continue
		}
		_, isKeep := keep[route.cfg.ID]
		_, isRetired := retired[route.cfg.ID]
		if isRetired || (!isKeep && route.cfg.Generation < generation) {
			route.cfg.Enabled = false
			route.cfg.Role = "retired"
		}
	}
	p.notifyStateChange()
}

func (p *SessionPool) applyRelayRouteMap(routeMap relayRouteMap) {
	if routeMap.Generation <= 0 {
		return
	}
	if atomic.LoadInt64(&p.localFailoverGeneration) >= routeMap.Generation {
		return
	}
	managedBy := managedByForRouteMap(routeMap)
	keep := make(map[string]struct{})
	retired := make(map[string]struct{})
	for _, id := range routeMap.Retired {
		if sanitized := sanitizeSessionComponent(id); sanitized != "" {
			retired[sanitized] = struct{}{}
		}
	}
	if routeMap.Active != nil {
		cfg := routeConfigFromRelaySpec(*routeMap.Active, managedBy, "active", routeMap.Generation, true, publicHost, publicPort)
		if cfg.ID != "" {
			keep[cfg.ID] = struct{}{}
			p.upsertManagedRoute(cfg)
		}
	}
	for _, standby := range routeMap.Standby {
		cfg := routeConfigFromRelaySpec(standby, managedBy, "standby", routeMap.Generation, false, publicHost, publicPort)
		if cfg.ID != "" {
			keep[cfg.ID] = struct{}{}
			p.upsertManagedRoute(cfg)
		}
	}
	p.disableRetiredManagedRoutes(managedBy, routeMap.Generation, keep, retired)
}

func (p *SessionPool) managedActiveReady() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ready := 0
	for _, session := range p.order {
		if session.route.cfg.ManagedBy == "" || session.route.cfg.Role != "active" || !session.route.cfg.Enabled {
			continue
		}
		if session.isReady() {
			ready++
		}
	}
	return ready
}

func (p *SessionPool) promoteManagedStandby() (clientRouteConfig, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var chosen *clientRouteState
	for _, route := range p.routes {
		if route.cfg.ManagedBy == "" || route.cfg.Role != "standby" {
			continue
		}
		if chosen == nil || route.cfg.Generation > chosen.cfg.Generation {
			chosen = route
		}
	}
	if chosen == nil {
		return clientRouteConfig{}, false
	}
	for _, route := range p.routes {
		if route.cfg.ManagedBy == chosen.cfg.ManagedBy && route.cfg.Role == "active" {
			route.cfg.Enabled = false
			route.cfg.Role = "retired"
		}
	}
	chosen.cfg.Role = "active"
	chosen.cfg.Enabled = true
	atomic.StoreInt64(&p.localFailoverGeneration, chosen.cfg.Generation)
	p.ensureRouteSessionsLocked(chosen)
	p.notifyStateChange()
	return chosen.cfg, true
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
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, s := range p.order {
		go s.loop()
		go s.statsLoop()
	}
}

func (p *SessionPool) totalActive() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
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
		RouteID:          s.route.cfg.ID,
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
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	status := clientStatusResponse{
		Service:        "furo-client",
		Version:        appVersion,
		Commit:         appCommit,
		BuildDate:      appBuildDate,
		StartedAt:      clientStartedAt.UTC().Format(time.RFC3339),
		UptimeSec:      int64(time.Since(clientStartedAt).Seconds()),
		RelayURL:       relayURL,
		SOCKSListen:    socksListen,
		AgentListen:    agentListen,
		AdminListen:    adminListen,
		ClientID:       clientID,
		RouteSelection: string(pool.strategy),
		SessionCount:   sessionCount,
		WriteTimeout:   writeTimeout.String(),
		FrameMinSize:   minFramePayload,
		FrameMidSize:   midFramePayload,
		FrameMaxSize:   maxFramePayload,
		RelayRequests: clientRelayRequestStats{
			Started:   atomic.LoadUint64(&relayRequestsStarted),
			Succeeded: atomic.LoadUint64(&relayRequestsSucceeded),
			Failed:    atomic.LoadUint64(&relayRequestsFailed),
			Rejected:  atomic.LoadUint64(&relayRequestsRejected),
		},
		Routes:   make([]clientRouteStatus, len(pool.routes)),
		Sessions: make([]clientSessionStatus, 0, len(pool.order)),
	}

	routeStatusByID := make(map[string]*clientRouteStatus, len(pool.routes))
	for idx, route := range pool.routes {
		status.Routes[idx] = clientRouteStatus{
			RouteID:            route.cfg.ID,
			RelayURL:           route.cfg.RelayURL,
			ServerHost:         route.cfg.ServerHost,
			ServerPort:         route.cfg.ServerPort,
			SessionCount:       route.cfg.SessionCount,
			Enabled:            route.cfg.Enabled,
			ManagedBy:          route.cfg.ManagedBy,
			Role:               route.cfg.Role,
			Generation:         route.cfg.Generation,
			EstimatedLatencyMs: route.latency().Milliseconds(),
		}
		routeStatusByID[route.cfg.ID] = &status.Routes[idx]
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
		if routeStatus := routeStatusByID[snapshot.RouteID]; routeStatus != nil {
			if snapshot.Connected {
				routeStatus.ConnectedSessions++
			}
			if snapshot.Ready {
				routeStatus.ReadySessions++
			}
			routeStatus.ActiveStreams += snapshot.ActiveStreams
			routeStatus.PendingFrames += snapshot.PendingFrames
			routeStatus.PendingBytes += snapshot.PendingBytes
		}
	}

	return status
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

const adminCookieName = "furo_client_admin"

type adminPageData struct {
	Status             clientStatusResponse
	Config             adminConfigView
	ConfigPath         string
	Message            string
	Error              string
	AdminListen        string
	ClientServiceState string
	AIRSServiceState   string
	InspectOutput      string
	InspectSummary     adminInspectSummary
}

type adminConfigView struct {
	ClientID                 string
	RouteSelection           string
	APIKey                   string
	SOCKSListen              string
	SOCKSAuth                string
	AgentListen              string
	AdminListen              string
	PublicHost               string
	PublicPort               string
	OpenTimeout              string
	Keepalive                string
	WriteTimeout             string
	FrameMinSize             string
	FrameMidSize             string
	FrameMaxSize             string
	LogFile                  string
	AIRSArvanAPIKey          string
	AIRSArvanRegion          string
	AIRSArvanServerID        string
	AIRSFixedPublicIP        string
	AIRSAutoRenewSeconds     string
	AIRSCleanupMinutes       string
	AIRSCheckSeconds         string
	AIRSFailureAttempts      string
	AIRSFailureSeconds       string
	AIRSLogFile              string
	AIRSSwitchScript         string
	AIRSInspectBinary        string
	AIRSServiceScript        string
	AIRSClientServiceRole    string
	AIRSOutboundCheckURL     string
	AIRSOutboundRetrySeconds string
	AIRSPostAddWaitSeconds   string
	AIRSPostDetachSeconds    string
	Routes                   []adminRouteView
}

type adminRouteView struct {
	Index        int
	ID           string
	RelayURL     string
	PublicHost   string
	PublicPort   string
	ServerHost   string
	ServerPort   string
	SessionCount string
	Enabled      bool
}

type adminInspectSummary struct {
	HasOutput bool
	OK        bool
	Ping      string
	Routes    []string
}

func adminAuthToken() string {
	mac := hmac.New(sha256.New, []byte(apiKey))
	_, _ = mac.Write([]byte("furo-client-admin-panel-v1"))
	return hex.EncodeToString(mac.Sum(nil))
}

func isAdminAuthenticated(r *http.Request) bool {
	if r.Header.Get("X-API-KEY") == apiKey {
		return true
	}
	cookie, err := r.Cookie(adminCookieName)
	if err != nil {
		return false
	}
	return hmac.Equal([]byte(cookie.Value), []byte(adminAuthToken()))
}

func setAdminAuthCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     adminCookieName,
		Value:    adminAuthToken(),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil,
		MaxAge:   12 * 60 * 60,
	})
}

func clearAdminAuthCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     adminCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
}

func readClientConfigMapForPanel() (map[string]any, error) {
	data, err := os.ReadFile(*configPath)
	if err != nil {
		return nil, err
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func stringFromMap(raw map[string]any, key string) string {
	if value, ok := raw[key]; ok {
		switch typed := value.(type) {
		case string:
			return typed
		case float64:
			if typed == float64(int64(typed)) {
				return strconv.FormatInt(int64(typed), 10)
			}
			return strconv.FormatFloat(typed, 'f', -1, 64)
		case bool:
			return strconv.FormatBool(typed)
		}
	}
	return ""
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case string:
		parsed, _ := strconv.Atoi(strings.TrimSpace(typed))
		return parsed
	default:
		return 0
	}
}

func intStringFromMap(raw map[string]any, key string) string {
	return stringFromMap(raw, key)
}

func setStringField(raw map[string]any, key, value string) {
	raw[key] = strings.TrimSpace(value)
}

func setOptionalStringField(raw map[string]any, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		delete(raw, key)
		return
	}
	raw[key] = value
}

func setIntField(raw map[string]any, key, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		delete(raw, key)
		return nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("%s must be a number", key)
	}
	raw[key] = parsed
	return nil
}

func nestedMap(raw map[string]any, key string) map[string]any {
	if existing, ok := raw[key].(map[string]any); ok {
		return existing
	}
	created := make(map[string]any)
	raw[key] = created
	return created
}

func adminConfigViewFromMap(raw map[string]any) adminConfigView {
	view := adminConfigView{
		ClientID:       stringFromMap(raw, "client_id"),
		RouteSelection: stringFromMap(raw, "route_selection"),
		APIKey:         stringFromMap(raw, "api_key"),
		SOCKSListen:    stringFromMap(raw, "socks_listen"),
		SOCKSAuth:      stringFromMap(raw, "socks_auth"),
		AgentListen:    stringFromMap(raw, "agent_listen"),
		AdminListen:    stringFromMap(raw, "admin_listen"),
		PublicHost:     stringFromMap(raw, "public_host"),
		PublicPort:     intStringFromMap(raw, "public_port"),
		OpenTimeout:    stringFromMap(raw, "open_timeout"),
		Keepalive:      stringFromMap(raw, "keepalive"),
		WriteTimeout:   stringFromMap(raw, "write_timeout"),
		FrameMinSize:   intStringFromMap(raw, "frame_min_size"),
		FrameMidSize:   intStringFromMap(raw, "frame_mid_size"),
		FrameMaxSize:   intStringFromMap(raw, "frame_max_size"),
		LogFile:        stringFromMap(raw, "log_file"),
	}
	if view.AdminListen == "" {
		view.AdminListen = stringFromMap(raw, "control_panel_listen")
	}
	if airs, ok := raw["airs"].(map[string]any); ok {
		view.AIRSArvanAPIKey = stringFromMap(airs, "arvan_api_key")
		view.AIRSArvanRegion = stringFromMap(airs, "arvan_region")
		view.AIRSArvanServerID = stringFromMap(airs, "arvan_server_id")
		view.AIRSFixedPublicIP = stringFromMap(airs, "fixed_public_ip")
		view.AIRSAutoRenewSeconds = intStringFromMap(airs, "auto_renew_interval_seconds")
		view.AIRSCleanupMinutes = intStringFromMap(airs, "cleanup_interval_minutes")
		view.AIRSCheckSeconds = intStringFromMap(airs, "check_interval_seconds")
		view.AIRSFailureAttempts = intStringFromMap(airs, "failure_confirm_attempts")
		view.AIRSFailureSeconds = intStringFromMap(airs, "failure_confirm_interval_seconds")
		view.AIRSLogFile = stringFromMap(airs, "log_file")
		view.AIRSSwitchScript = stringFromMap(airs, "switch_script")
		view.AIRSInspectBinary = stringFromMap(airs, "inspect_binary")
		view.AIRSServiceScript = stringFromMap(airs, "service_script")
		view.AIRSClientServiceRole = stringFromMap(airs, "client_service_role")
		view.AIRSOutboundCheckURL = stringFromMap(airs, "outbound_check_url")
		view.AIRSOutboundRetrySeconds = intStringFromMap(airs, "outbound_check_retry_seconds")
		view.AIRSPostAddWaitSeconds = intStringFromMap(airs, "post_add_wait_seconds")
		view.AIRSPostDetachSeconds = intStringFromMap(airs, "post_detach_wait_seconds")
	}
	if routes, ok := raw["routes"].([]any); ok {
		for idx, routeValue := range routes {
			route, ok := routeValue.(map[string]any)
			if !ok {
				continue
			}
			enabled := true
			if value, exists := route["enabled"].(bool); exists {
				enabled = value
			}
			view.Routes = append(view.Routes, adminRouteView{
				Index:        idx,
				ID:           stringFromMap(route, "id"),
				RelayURL:     stringFromMap(route, "relay_url"),
				PublicHost:   stringFromMap(route, "public_host"),
				PublicPort:   intStringFromMap(route, "public_port"),
				ServerHost:   stringFromMap(route, "server_host"),
				ServerPort:   intStringFromMap(route, "server_port"),
				SessionCount: intStringFromMap(route, "session_count"),
				Enabled:      enabled,
			})
		}
	}
	if len(view.Routes) == 0 {
		view.Routes = []adminRouteView{{
			Index:        0,
			ID:           "primary",
			RelayURL:     stringFromMap(raw, "relay_url"),
			ServerHost:   stringFromMap(raw, "server_host"),
			ServerPort:   intStringFromMap(raw, "server_port"),
			SessionCount: intStringFromMap(raw, "session_count"),
			Enabled:      true,
		}}
	}
	return view
}

func saveClientConfigForm(r *http.Request) error {
	if err := r.ParseForm(); err != nil {
		return err
	}
	raw, err := readClientConfigMapForPanel()
	if err != nil {
		return err
	}

	setOptionalStringField(raw, "client_id", r.FormValue("client_id"))
	setStringField(raw, "route_selection", r.FormValue("route_selection"))
	setStringField(raw, "api_key", r.FormValue("api_key"))
	setStringField(raw, "socks_listen", r.FormValue("socks_listen"))
	setOptionalStringField(raw, "socks_auth", r.FormValue("socks_auth"))
	setStringField(raw, "agent_listen", r.FormValue("agent_listen"))
	setOptionalStringField(raw, "admin_listen", r.FormValue("admin_listen"))
	delete(raw, "control_panel_listen")
	setStringField(raw, "public_host", r.FormValue("public_host"))
	for _, key := range []string{"public_port", "frame_min_size", "frame_mid_size", "frame_max_size"} {
		if err := setIntField(raw, key, r.FormValue(key)); err != nil {
			return err
		}
	}
	setStringField(raw, "open_timeout", r.FormValue("open_timeout"))
	setStringField(raw, "keepalive", r.FormValue("keepalive"))
	setStringField(raw, "write_timeout", r.FormValue("write_timeout"))
	setOptionalStringField(raw, "log_file", r.FormValue("log_file"))

	airs := nestedMap(raw, "airs")
	for _, key := range []string{
		"arvan_api_key", "arvan_region", "arvan_server_id", "fixed_public_ip", "log_file",
		"switch_script", "inspect_binary", "service_script", "client_service_role", "outbound_check_url",
	} {
		setOptionalStringField(airs, key, r.FormValue("airs_"+key))
	}
	for _, key := range []string{
		"auto_renew_interval_seconds", "cleanup_interval_minutes", "check_interval_seconds",
		"failure_confirm_attempts", "failure_confirm_interval_seconds", "outbound_check_retry_seconds",
		"post_add_wait_seconds", "post_detach_wait_seconds",
	} {
		if err := setIntField(airs, key, r.FormValue("airs_"+key)); err != nil {
			return err
		}
	}

	count, err := strconv.Atoi(r.FormValue("routes_count"))
	if err != nil || count < 0 {
		return errors.New("routes_count must be a number")
	}
	routes := make([]any, 0, count)
	for idx := 0; idx < count; idx++ {
		prefix := fmt.Sprintf("routes_%d_", idx)
		if r.FormValue(prefix+"delete") == "1" {
			continue
		}
		route := make(map[string]any)
		setStringField(route, "id", r.FormValue(prefix+"id"))
		setStringField(route, "relay_url", r.FormValue(prefix+"relay_url"))
		setOptionalStringField(route, "public_host", r.FormValue(prefix+"public_host"))
		if err := setIntField(route, "public_port", r.FormValue(prefix+"public_port")); err != nil {
			return err
		}
		setStringField(route, "server_host", r.FormValue(prefix+"server_host"))
		if err := setIntField(route, "server_port", r.FormValue(prefix+"server_port")); err != nil {
			return err
		}
		if err := setIntField(route, "session_count", r.FormValue(prefix+"session_count")); err != nil {
			return err
		}
		route["enabled"] = r.FormValue(prefix+"enabled") == "on"
		routes = append(routes, route)
	}
	if len(routes) == 0 {
		return errors.New("at least one route is required")
	}
	raw["routes"] = routes
	delete(raw, "relay_url")
	delete(raw, "server_host")
	delete(raw, "server_port")
	delete(raw, "session_count")

	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(*configPath, data, 0644)
}

func serviceScriptPath() (string, error) {
	if airsSettings.ServiceScript != "" {
		if resolved, err := resolveClientHelperPath(airsSettings.ServiceScript); err == nil {
			return resolved, nil
		}
	}

	candidates := make([]string, 0, 3)
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(wd, "service.sh"))
	}
	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), "service.sh"))
	}
	candidates = append(candidates, filepath.Join(filepath.Dir(*configPath), "service.sh"))

	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", errors.New("service.sh not found next to working directory, binary, or config")
}

func resolveClientHelperPath(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", errors.New("empty helper path")
	}
	if filepath.IsAbs(value) {
		info, err := os.Stat(value)
		if err == nil && !info.IsDir() {
			return value, nil
		}
		return "", fmt.Errorf("helper not found: %s", value)
	}

	candidates := make([]string, 0, 3)
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(wd, value))
	}
	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), value))
	}
	candidates = append(candidates, filepath.Join(filepath.Dir(*configPath), value))

	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("helper not found: %s", value)
}

func adminPanelDir() (string, error) {
	candidates := make([]string, 0, 3)
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(wd, "web", "client-panel"))
	}
	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), "web", "client-panel"))
	}
	candidates = append(candidates, filepath.Join(filepath.Dir(*configPath), "web", "client-panel"))

	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && info.IsDir() {
			return candidate, nil
		}
	}
	return "", errors.New("client panel assets not found next to working directory, binary, or config")
}

func executeAdminTemplate(w http.ResponseWriter, name string, data any) error {
	panelDir, err := adminPanelDir()
	if err != nil {
		return err
	}
	tmpl, err := template.ParseFiles(filepath.Join(panelDir, name))
	if err != nil {
		return err
	}
	return tmpl.ExecuteTemplate(w, name, data)
}

func runServiceCommand(role, action string) (string, error) {
	switch role {
	case "client", "airs":
	default:
		return "", fmt.Errorf("unsupported service role %q", role)
	}
	switch action {
	case "start", "stop", "restart", "enable", "disable", "status", "stateus":
	default:
		return "", fmt.Errorf("unsupported service action %q", action)
	}

	script, err := serviceScriptPath()
	if err != nil {
		return "", err
	}

	if role == "client" && (action == "stop" || action == "restart") {
		cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("sleep 1; bash %s %s %s >/tmp/furo-client-service-control.log 2>&1", shellQuote(script), shellQuote(role), shellQuote(action)))
		if err := cmd.Start(); err != nil {
			return "", err
		}
		return "scheduled client " + action + "; output will be written to /tmp/furo-client-service-control.log", nil
	}

	cmd := exec.Command("bash", script, role, action)
	cmd.Dir = filepath.Dir(script)
	output, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

func runInspectCommand() (string, error) {
	inspectPath, err := resolveClientHelperPath(airsSettings.InspectBinary)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, inspectPath, "-c", *configPath, "--all")
	cmd.Dir = filepath.Dir(inspectPath)
	output, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return strings.TrimSpace(string(output)), ctx.Err()
	}
	return strings.TrimSpace(string(output)), err
}

func parseInspectSummary(output string, err error) adminInspectSummary {
	summary := adminInspectSummary{HasOutput: strings.TrimSpace(output) != "", OK: err == nil && !strings.Contains(output, "FURO inspect failed")}
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "Route:") {
			_, value, _ := strings.Cut(line, "Route:")
			value = strings.TrimSpace(value)
			if value != "" {
				summary.Routes = append(summary.Routes, value)
			}
		}
		if strings.Contains(line, "Ping:") {
			_, value, _ := strings.Cut(line, "Ping:")
			value = strings.TrimSpace(value)
			if value != "" {
				summary.Ping = value
			}
		}
	}
	return summary
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func renderAdminLogin(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := executeAdminTemplate(w, "login.html", map[string]string{"Message": message}); err != nil {
		logEvent("[CLIENT] render_login_failed err=%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func renderAdminPanel(w http.ResponseWriter, data adminPageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := executeAdminTemplate(w, "panel.html", data); err != nil {
		logEvent("[CLIENT] render_panel_failed err=%v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func buildAdminPageData(pool *SessionPool, message, errMessage, inspectOutput string) adminPageData {
	configMap, err := readClientConfigMapForPanel()
	if err != nil && errMessage == "" {
		errMessage = "read config: " + err.Error()
	}
	configView := adminConfigView{}
	if configMap != nil {
		configView = adminConfigViewFromMap(configMap)
	}
	clientState, clientErr := runServiceCommand("client", "status")
	if clientErr != nil {
		if clientState == "" {
			clientState = clientErr.Error()
		} else {
			clientState += "\n" + clientErr.Error()
		}
	}
	airsState, airsErr := runServiceCommand("airs", "status")
	if airsErr != nil {
		if airsState == "" {
			airsState = airsErr.Error()
		} else {
			airsState += "\n" + airsErr.Error()
		}
	}

	return adminPageData{
		Status:             buildClientStatus(pool),
		Config:             configView,
		ConfigPath:         *configPath,
		Message:            message,
		Error:              errMessage,
		AdminListen:        adminListen,
		ClientServiceState: clientState,
		AIRSServiceState:   airsState,
		InspectOutput:      inspectOutput,
		InspectSummary:     parseInspectSummary(inspectOutput, nil),
	}
}

func runClientAdminServer(pool *SessionPool) error {
	mux := http.NewServeMux()
	panelDir, err := adminPanelDir()
	if err != nil {
		return err
	}
	mux.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir(filepath.Join(panelDir, "assets")))))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if !isAdminAuthenticated(r) {
			renderAdminLogin(w, "")
			return
		}
		renderAdminPanel(w, buildAdminPageData(pool, "", "", ""))
	})
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		if r.FormValue("api_key") != apiKey {
			renderAdminLogin(w, "Invalid API key")
			return
		}
		setAdminAuthCookie(w, r)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	})
	mux.HandleFunc("/logout", func(w http.ResponseWriter, r *http.Request) {
		clearAdminAuthCookie(w)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	})
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
	mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		if !isAdminAuthenticated(r) {
			http.Error(w, "unauthorized", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		if err := saveClientConfigForm(r); err != nil {
			renderAdminPanel(w, buildAdminPageData(pool, "", err.Error(), ""))
			return
		}
		renderAdminPanel(w, buildAdminPageData(pool, "Config saved. Restart the affected services to apply it.", "", ""))
	})
	mux.HandleFunc("/service", func(w http.ResponseWriter, r *http.Request) {
		if !isAdminAuthenticated(r) {
			http.Error(w, "unauthorized", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		role := r.FormValue("role")
		if role == "" {
			role = "client"
		}
		action := r.FormValue("action")
		output, err := runServiceCommand(role, action)
		if err != nil {
			renderAdminPanel(w, buildAdminPageData(pool, output, err.Error(), ""))
			return
		}
		if output == "" {
			output = role + " service " + action + " completed"
		}
		renderAdminPanel(w, buildAdminPageData(pool, output, "", ""))
	})
	mux.HandleFunc("/inspect", func(w http.ResponseWriter, r *http.Request) {
		if !isAdminAuthenticated(r) {
			http.Error(w, "unauthorized", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		output, err := runInspectCommand()
		if err != nil {
			renderAdminPanel(w, buildAdminPageData(pool, output, err.Error(), output))
			return
		}
		renderAdminPanel(w, buildAdminPageData(pool, "Inspect completed", "", output))
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
	p.mu.RLock()
	defer p.mu.RUnlock()
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

func (p *SessionPool) readySnapshotsForRoute(route *clientRouteState) ([]sessionLoadSnapshot, int) {
	ready := make([]sessionLoadSnapshot, 0, route.cfg.SessionCount)
	idleCount := 0
	for _, session := range p.order {
		if session.route != route {
			continue
		}
		snapshot := session.loadSnapshot()
		if snapshot.session == nil {
			continue
		}
		if snapshot.isIdle() {
			idleCount++
		}
		ready = append(ready, snapshot)
	}
	return ready, idleCount
}

func (p *SessionPool) pickLeastLoadedSnapshot(candidates []sessionLoadSnapshot) (sessionLoadSnapshot, bool) {
	if len(candidates) == 0 {
		return sessionLoadSnapshot{}, false
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
	return best, true
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

func (p *SessionPool) selectReadySnapshotForRoute(route *clientRouteState) (sessionLoadSnapshot, int, bool) {
	ready, idleCount := p.readySnapshotsForRoute(route)
	if len(ready) == 0 {
		return sessionLoadSnapshot{}, 0, false
	}

	reservedSessions := reserveSessionCount(route.cfg.SessionCount)
	if reservedSessions > 0 && idleCount > reservedSessions {
		warm := make([]sessionLoadSnapshot, 0, len(ready))
		for _, snapshot := range ready {
			if !snapshot.isIdle() {
				warm = append(warm, snapshot)
			}
		}
		if chosen, ok := p.pickLeastLoadedSnapshot(warm); ok {
			return chosen, len(ready), true
		}
	}

	chosen, ok := p.pickLeastLoadedSnapshot(ready)
	return chosen, len(ready), ok
}

type routeCandidate struct {
	route      *clientRouteState
	snapshot   sessionLoadSnapshot
	readyCount int
}

func (p *SessionPool) selectReadySession() (*MuxSession, int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	candidates := make([]routeCandidate, 0, len(p.routes))
	totalReady := 0
	for _, route := range p.routes {
		if !route.cfg.Enabled {
			continue
		}
		snapshot, readyCount, ok := p.selectReadySnapshotForRoute(route)
		totalReady += readyCount
		if !ok {
			continue
		}
		candidates = append(candidates, routeCandidate{route: route, snapshot: snapshot, readyCount: readyCount})
	}
	if len(candidates) == 0 {
		return nil, totalReady
	}

	var chosen routeCandidate
	switch p.strategy {
	case routeSelectionRoundRobin:
		chosen = candidates[int(atomic.AddUint32(&p.rr, 1))%len(candidates)]
	case routeSelectionRandom:
		chosen = candidates[rand.Intn(len(candidates))]
	case routeSelectionLeastRTT:
		chosen = candidates[0]
		for _, candidate := range candidates[1:] {
			chosenLatency := chosen.route.latency()
			candidateLatency := candidate.route.latency()
			switch {
			case chosenLatency == 0 && candidateLatency > 0:
				chosen = candidate
			case candidateLatency > 0 && candidateLatency < chosenLatency:
				chosen = candidate
			case candidateLatency == chosenLatency && candidate.snapshot.score() < chosen.snapshot.score():
				chosen = candidate
			}
		}
	default:
		chosen = candidates[0]
		for _, candidate := range candidates[1:] {
			if candidate.snapshot.score() < chosen.snapshot.score() {
				chosen = candidate
				continue
			}
			if candidate.snapshot.score() == chosen.snapshot.score() && candidate.snapshot.activeStreams < chosen.snapshot.activeStreams {
				chosen = candidate
			}
		}
	}
	return chosen.snapshot.session, totalReady
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

func buildSOCKSConfig(pool *SessionPool) *socks5.Config {
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
	if user, pass, ok := parseSOCKSAuth(socksAuth); ok {
		conf.Credentials = socks5.StaticCredentials{user: pass}
	}
	return conf
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

func runManagedRouteMapLoop(pool *SessionPool) {
	if !routeMapSettings.Enabled {
		return
	}
	pollInterval := time.Duration(routeMapSettings.PollIntervalSeconds) * time.Second
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}
	failoverGrace := time.Duration(routeMapSettings.FailoverGraceSeconds) * time.Second
	if failoverGrace <= 0 {
		failoverGrace = 5 * time.Second
	}
	var activeEmptySince time.Time
	fetchAndApply := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		routeMap, err := fetchRelayRouteMap(ctx, relayURL)
		if err != nil {
			logEvent("[CLIENT] route_map_fetch_failed err=%v", err)
			return
		}
		if routeMapSettings.FleetID != "" && routeMap.FleetID != "" && routeMap.FleetID != routeMapSettings.FleetID {
			logEvent("[CLIENT] route_map_ignored fleet_id=%s want=%s", routeMap.FleetID, routeMapSettings.FleetID)
			return
		}
		if routeMap.Namespace != "" && routeMap.Namespace != routeMapSettings.Namespace {
			logEvent("[CLIENT] route_map_ignored namespace=%s want=%s", routeMap.Namespace, routeMapSettings.Namespace)
			return
		}
		pool.applyRelayRouteMap(routeMap)
		if err := persistRelayRouteMap(routeMap); err != nil {
			logEvent("[CLIENT] route_map_persist_failed err=%v", err)
		}
	}
	fetchAndApply()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	healthTicker := time.NewTicker(time.Second)
	defer healthTicker.Stop()
	for {
		select {
		case <-ticker.C:
			fetchAndApply()
		case <-healthTicker.C:
			if pool.managedActiveReady() > 0 {
				activeEmptySince = time.Time{}
				continue
			}
			if activeEmptySince.IsZero() {
				activeEmptySince = time.Now()
				continue
			}
			if time.Since(activeEmptySince) < failoverGrace {
				continue
			}
			promoted, ok := pool.promoteManagedStandby()
			if !ok {
				continue
			}
			logEvent("[CLIENT] managed_failover_promoted route=%s generation=%d", promoted.ID, promoted.Generation)
			if err := persistPromotedManagedRoute(promoted); err != nil {
				logEvent("[CLIENT] managed_failover_persist_failed err=%v", err)
			}
			activeEmptySince = time.Time{}
		}
	}
}

func pingMasterFromClient() error {
	target := strings.TrimSpace(*pingMasterURL)
	if target == "" {
		target = strings.TrimSpace(routeMapSettings.MasterURL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if target != "" {
		parsed, err := url.Parse(target)
		if err != nil {
			return err
		}
		if parsed.Path == "" || parsed.Path == "/" {
			parsed.Path = "/healthz"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-API-KEY", apiKey)
		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			return fmt.Errorf("master status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		fmt.Printf("ping master ok: %s\n%s\n", parsed.String(), strings.TrimSpace(string(body)))
		return nil
	}
	routeMap, err := fetchRelayRouteMap(ctx, relayURL)
	if err != nil {
		return err
	}
	fmt.Printf("ping master route-map ok: namespace=%s fleet=%s generation=%d active=%t standby=%d updated_at=%s\n", routeMap.Namespace, routeMap.FleetID, routeMap.Generation, routeMap.Active != nil, len(routeMap.Standby), routeMap.UpdatedAt)
	return nil
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
	if *pingMaster {
		if err := pingMasterFromClient(); err != nil {
			log.Fatalf("ping master failed: %v", err)
		}
		return
	}

	if logFilePath != "" {
		logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("failed to open log file %s: %v", logFilePath, err)
		}
		logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
		log.Printf("[FURO-CLIENT] debug log file: %s", logFilePath)
	}

	rand.Seed(time.Now().UnixNano())
	httpClient.Transport.(*http.Transport).ResponseHeaderTimeout = openTimeout
	pool := newSessionPoolForRoutes(clientRoutes, routeSelection)
	logEvent("[CLIENT] startup version=%s client_id=%s route_selection=%s routes=%d socks_listen=%s agent_listen=%s admin_listen=%s open_timeout=%s session_count=%d", appVersion, clientID, routeSelection, len(clientRoutes), socksListen, agentListen, adminListen, openTimeout.String(), sessionCount)

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
	go runManagedRouteMapLoop(pool)

	conf := buildSOCKSConfig(pool)

	server, err := socks5.New(conf)
	if err != nil {
		log.Fatalf("failed to create SOCKS5 server: %v", err)
	}

	log.Printf("[FURO-CLIENT] SOCKS5 listening on %s", socksListen)
	if err := server.ListenAndServe("tcp", socksListen); err != nil {
		log.Fatalf("SOCKS5 serve failed: %v", err)
	}
}
