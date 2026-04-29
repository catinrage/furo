package main

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientWriteReadFrameRoundTrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	payload := []byte("hello")
	if err := writeFrame(&buf, frameData, 42, payload); err != nil {
		t.Fatalf("writeFrame() error = %v", err)
	}

	got, err := readFrame(&buf)
	if err != nil {
		t.Fatalf("readFrame() error = %v", err)
	}

	if got.typ != frameData || got.streamID != 42 || !bytes.Equal(got.payload, payload) {
		t.Fatalf("unexpected frame = %#v", got)
	}
}

func TestClientReadFrameRejectsOversizedPayload(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	header := make([]byte, frameHeaderSize)
	header[0] = frameData
	binary.BigEndian.PutUint32(header[5:9], uint32(maxFramePayload+1))
	if _, err := buf.Write(header); err != nil {
		t.Fatalf("write header: %v", err)
	}

	if _, err := readFrame(&buf); err == nil {
		t.Fatal("readFrame() error = nil, want oversized frame error")
	}
}

func TestClientEncodeOpenPayload(t *testing.T) {
	t.Parallel()

	payload := encodeOpenPayload("example.com", 443)
	hostLen := int(binary.BigEndian.Uint16(payload[0:2]))
	if got := string(payload[2 : 2+hostLen]); got != "example.com" {
		t.Fatalf("encoded host = %q, want %q", got, "example.com")
	}
	if got := binary.BigEndian.Uint16(payload[2+hostLen:]); got != 443 {
		t.Fatalf("encoded port = %d, want 443", got)
	}
}

func TestComputeReconnectDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		failures int
		want     time.Duration
	}{
		{failures: 0, want: 0},
		{failures: 1, want: 1 * time.Second},
		{failures: 2, want: 2 * time.Second},
		{failures: 3, want: 4 * time.Second},
		{failures: 4, want: 8 * time.Second},
		{failures: 5, want: 16 * time.Second},
		{failures: 6, want: 30 * time.Second},
		{failures: 7, want: 30 * time.Second},
	}

	for _, tc := range tests {
		if got := computeReconnectDelay(tc.failures); got != tc.want {
			t.Fatalf("computeReconnectDelay(%d) = %s, want %s", tc.failures, got, tc.want)
		}
	}
}

func TestReserveSessionCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		count int
		want  int
	}{
		{count: 1, want: 0},
		{count: 3, want: 0},
		{count: 4, want: 1},
		{count: 7, want: 1},
		{count: 8, want: 2},
	}

	for _, tc := range tests {
		if got := reserveSessionCount(tc.count); got != tc.want {
			t.Fatalf("reserveSessionCount(%d) = %d, want %d", tc.count, got, tc.want)
		}
	}
}

func TestSelectReadySessionPrefersLeastLoaded(t *testing.T) {
	t.Parallel()

	pool := newSessionPool(3)
	markSessionReady(pool.order[0], 3, 3*maxFramePayload)
	markSessionReady(pool.order[1], 1, maxFramePayload/2)
	markSessionReady(pool.order[2], 0, 0)

	chosen, readyCount := pool.selectReadySession()
	if readyCount != 3 {
		t.Fatalf("readyCount = %d, want 3", readyCount)
	}
	if chosen != pool.order[2] {
		t.Fatalf("selected %s, want %s", chosen.sid, pool.order[2].sid)
	}
}

func TestSelectReadySessionPreservesReserveCapacity(t *testing.T) {
	t.Parallel()

	pool := newSessionPool(4)
	markSessionReady(pool.order[0], 2, maxFramePayload)
	markSessionReady(pool.order[1], 0, 0)
	markSessionReady(pool.order[2], 0, 0)
	markSessionReady(pool.order[3], 0, 0)

	chosen, readyCount := pool.selectReadySession()
	if readyCount != 4 {
		t.Fatalf("readyCount = %d, want 4", readyCount)
	}
	if chosen != pool.order[0] {
		t.Fatalf("selected %s, want warm session %s", chosen.sid, pool.order[0].sid)
	}
}

func TestMuxSessionPopDataFrameFairness(t *testing.T) {
	t.Parallel()

	pool := newSessionPool(1)
	session := newMuxSession(pool, pool.routes[0], 0, "sess_1")
	session.mu.Lock()
	session.conn = &stubConn{}
	session.ready = true
	session.mu.Unlock()

	for _, item := range []struct {
		streamID uint32
		payload  []byte
	}{
		{streamID: 1, payload: []byte("first")},
		{streamID: 1, payload: []byte("second")},
		{streamID: 3, payload: []byte("third")},
	} {
		if err := session.enqueueDataFrame(&outboundFrame{
			typ:      frameData,
			streamID: item.streamID,
			payload:  item.payload,
			result:   make(chan error, 1),
		}); err != nil {
			t.Fatalf("enqueueDataFrame(%d) error = %v", item.streamID, err)
		}
	}

	got := []uint32{
		session.popDataFrame().streamID,
		session.popDataFrame().streamID,
		session.popDataFrame().streamID,
	}
	want := []uint32{1, 3, 1}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pop order = %v, want %v", got, want)
		}
	}
	if pending := atomic.LoadInt64(&session.pendingFrames); pending != 0 {
		t.Fatalf("pendingFrames = %d, want 0", pending)
	}
}

func TestBuildClientStatusAggregatesSessions(t *testing.T) {
	originalClientID := clientID
	originalRouteSelection := routeSelection
	originalRelayURL := relayURL
	originalSocksListen := socksListen
	originalAgentListen := agentListen
	originalAdminListen := adminListen
	originalSessionCount := sessionCount
	originalStartedAt := clientStartedAt
	originalStarted := atomic.LoadUint64(&relayRequestsStarted)
	originalSucceeded := atomic.LoadUint64(&relayRequestsSucceeded)
	originalFailed := atomic.LoadUint64(&relayRequestsFailed)
	originalRejected := atomic.LoadUint64(&relayRequestsRejected)
	t.Cleanup(func() {
		clientID = originalClientID
		routeSelection = originalRouteSelection
		relayURL = originalRelayURL
		socksListen = originalSocksListen
		agentListen = originalAgentListen
		adminListen = originalAdminListen
		sessionCount = originalSessionCount
		clientStartedAt = originalStartedAt
		atomic.StoreUint64(&relayRequestsStarted, originalStarted)
		atomic.StoreUint64(&relayRequestsSucceeded, originalSucceeded)
		atomic.StoreUint64(&relayRequestsFailed, originalFailed)
		atomic.StoreUint64(&relayRequestsRejected, originalRejected)
	})

	clientID = "client-test"
	routeSelection = routeSelectionLeastLoad
	relayURL = "https://relay.example/furo-relay.php"
	socksListen = "127.0.0.1:18713"
	agentListen = "0.0.0.0:28080"
	adminListen = "127.0.0.1:19080"
	sessionCount = 2
	clientStartedAt = time.Now().Add(-5 * time.Second)
	atomic.StoreUint64(&relayRequestsStarted, 4)
	atomic.StoreUint64(&relayRequestsSucceeded, 3)
	atomic.StoreUint64(&relayRequestsFailed, 1)
	atomic.StoreUint64(&relayRequestsRejected, 0)

	pool := newSessionPool(2)

	first := pool.order[0]
	first.mu.Lock()
	first.conn = &stubConn{}
	first.ready = true
	first.requestFailures = 1
	first.retryDelay = 2 * time.Second
	first.nextRetryAt = time.Now().Add(2 * time.Second)
	first.lastRequestErr = "relay rejected"
	atomic.StoreInt64(&first.pendingFrames, 2)
	atomic.StoreInt64(&first.pendingBytes, 2048)
	first.streams[1] = newMuxConn(first, 1)
	first.mu.Unlock()
	atomic.StoreUint64(&first.framesIn, 11)
	atomic.StoreUint64(&first.framesOut, 9)
	atomic.StoreUint64(&first.bytesIn, 110)
	atomic.StoreUint64(&first.bytesOut, 90)

	second := pool.order[1]
	second.mu.Lock()
	second.requestFailures = 2
	second.retryDelay = 4 * time.Second
	second.nextRetryAt = time.Now().Add(4 * time.Second)
	second.lastRequestErr = "dial tcp: timeout"
	second.mu.Unlock()

	status := buildClientStatus(pool)

	if status.Totals.ConnectedSessions != 1 {
		t.Fatalf("connected sessions = %d, want 1", status.Totals.ConnectedSessions)
	}
	if status.Totals.ReadySessions != 1 {
		t.Fatalf("ready sessions = %d, want 1", status.Totals.ReadySessions)
	}
	if status.Totals.ActiveStreams != 1 {
		t.Fatalf("active streams = %d, want 1", status.Totals.ActiveStreams)
	}
	if status.Totals.PendingFrames != 2 || status.Totals.PendingBytes != 2048 {
		t.Fatalf("pending totals = (%d, %d), want (2, 2048)", status.Totals.PendingFrames, status.Totals.PendingBytes)
	}
	if status.RelayRequests.Started != 4 || status.RelayRequests.Succeeded != 3 || status.RelayRequests.Failed != 1 {
		t.Fatalf("unexpected relay request stats = %#v", status.RelayRequests)
	}
	if len(status.Sessions) != 2 {
		t.Fatalf("session count = %d, want 2", len(status.Sessions))
	}
	if status.Sessions[0].LastRequestErr != "relay rejected" {
		t.Fatalf("first session last error = %q", status.Sessions[0].LastRequestErr)
	}
	if status.Sessions[1].RetryDelayMs != 4000 {
		t.Fatalf("second session retry delay = %d, want 4000", status.Sessions[1].RetryDelayMs)
	}
	if status.RouteSelection != string(routeSelectionLeastLoad) {
		t.Fatalf("route selection = %q, want %q", status.RouteSelection, routeSelectionLeastLoad)
	}
	if len(status.Routes) != 1 || status.Routes[0].RouteID != "primary" {
		t.Fatalf("unexpected route status = %#v", status.Routes)
	}
}

func TestLoadClientConfigRoutes(t *testing.T) {
	originalClientID := clientID
	originalRouteSelection := routeSelection
	originalRoutes := clientRoutes
	originalRelayURL := relayURL
	originalAPIKey := apiKey
	originalSocksListen := socksListen
	originalAgentListen := agentListen
	originalPublicHost := publicHost
	originalPublicPort := publicPort
	originalServerHost := serverHost
	originalServerPort := serverPort
	originalAdminListen := adminListen
	originalOpenTimeout := openTimeout
	originalKeepalive := keepalivePeriod
	originalSessionCount := sessionCount
	originalLogFilePath := logFilePath
	t.Cleanup(func() {
		clientID = originalClientID
		routeSelection = originalRouteSelection
		clientRoutes = originalRoutes
		relayURL = originalRelayURL
		apiKey = originalAPIKey
		socksListen = originalSocksListen
		agentListen = originalAgentListen
		publicHost = originalPublicHost
		publicPort = originalPublicPort
		serverHost = originalServerHost
		serverPort = originalServerPort
		adminListen = originalAdminListen
		openTimeout = originalOpenTimeout
		keepalivePeriod = originalKeepalive
		sessionCount = originalSessionCount
		logFilePath = originalLogFilePath
	})

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.client.json")
	payload := `{
  "client_id": "client-a",
  "route_selection": "least_latency",
  "api_key": "secret",
  "socks_listen": "127.0.0.1:18713",
  "agent_listen": "0.0.0.0:28080",
  "public_host": "198.51.100.10",
  "public_port": 28080,
  "admin_listen": "127.0.0.1:19080",
  "open_timeout": "12s",
  "keepalive": "7s",
  "log_file": "",
  "routes": [
    {
      "id": "relay_a",
      "relay_url": "https://relay-a.example/furo.php",
      "server_host": "203.0.113.10",
      "server_port": 8443,
      "session_count": 2
    },
    {
      "id": "relay_b",
      "relay_url": "https://relay-b.example/furo.php",
      "public_host": "198.51.100.11",
      "public_port": 29080,
      "server_host": "203.0.113.11",
      "server_port": 9443,
      "session_count": 3
    }
  ]
}`
	if err := os.WriteFile(configPath, []byte(payload), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := loadClientConfig(configPath); err != nil {
		t.Fatalf("loadClientConfig() error = %v", err)
	}

	if clientID != "client-a" {
		t.Fatalf("clientID = %q, want %q", clientID, "client-a")
	}
	if routeSelection != routeSelectionLeastRTT {
		t.Fatalf("routeSelection = %q, want %q", routeSelection, routeSelectionLeastRTT)
	}
	if sessionCount != 5 {
		t.Fatalf("sessionCount = %d, want 5", sessionCount)
	}
	if len(clientRoutes) != 2 {
		t.Fatalf("clientRoutes count = %d, want 2", len(clientRoutes))
	}
	if clientRoutes[0].PublicHost != "198.51.100.10" || clientRoutes[0].PublicPort != 28080 {
		t.Fatalf("first route public = %s:%d, want 198.51.100.10:28080", clientRoutes[0].PublicHost, clientRoutes[0].PublicPort)
	}
	if clientRoutes[1].PublicHost != "198.51.100.11" || clientRoutes[1].PublicPort != 29080 {
		t.Fatalf("second route public = %s:%d, want 198.51.100.11:29080", clientRoutes[1].PublicHost, clientRoutes[1].PublicPort)
	}
}

func TestSelectReadySessionRoundRobinAcrossRoutes(t *testing.T) {
	originalClientID := clientID
	t.Cleanup(func() { clientID = originalClientID })
	clientID = "client-test"
	routes := []clientRouteConfig{
		{ID: "route_a", RelayURL: "https://relay-a.example", PublicHost: "198.51.100.10", PublicPort: 28080, ServerHost: "203.0.113.10", ServerPort: 8443, SessionCount: 1, Enabled: true},
		{ID: "route_b", RelayURL: "https://relay-b.example", PublicHost: "198.51.100.10", PublicPort: 28080, ServerHost: "203.0.113.11", ServerPort: 8443, SessionCount: 1, Enabled: true},
	}
	pool := newSessionPoolForRoutes(routes, routeSelectionRoundRobin)
	markSessionReady(pool.order[0], 0, 0)
	markSessionReady(pool.order[1], 0, 0)

	first, _ := pool.selectReadySession()
	second, _ := pool.selectReadySession()
	if first == nil || second == nil {
		t.Fatal("expected ready sessions")
	}
	if first.route.cfg.ID == second.route.cfg.ID {
		t.Fatalf("round robin selected same route twice: %s then %s", first.route.cfg.ID, second.route.cfg.ID)
	}
}

func TestSelectReadySessionPrefersLowestLatencyRoute(t *testing.T) {
	originalClientID := clientID
	t.Cleanup(func() { clientID = originalClientID })
	clientID = "client-test"
	routes := []clientRouteConfig{
		{ID: "slow", RelayURL: "https://relay-slow.example", PublicHost: "198.51.100.10", PublicPort: 28080, ServerHost: "203.0.113.10", ServerPort: 8443, SessionCount: 1, Enabled: true},
		{ID: "fast", RelayURL: "https://relay-fast.example", PublicHost: "198.51.100.10", PublicPort: 28080, ServerHost: "203.0.113.11", ServerPort: 8443, SessionCount: 1, Enabled: true},
	}
	pool := newSessionPoolForRoutes(routes, routeSelectionLeastRTT)
	markSessionReady(pool.order[0], 0, 0)
	markSessionReady(pool.order[1], 0, 0)
	pool.routes[0].observeRequestSuccess(40 * time.Millisecond)
	pool.routes[1].observeRequestSuccess(10 * time.Millisecond)

	chosen, _ := pool.selectReadySession()
	if chosen == nil {
		t.Fatal("expected ready session")
	}
	if chosen.route.cfg.ID != "fast" {
		t.Fatalf("selected route %s, want fast", chosen.route.cfg.ID)
	}
}

func markSessionReady(session *MuxSession, activeStreams int, pendingBytes int64) {
	session.mu.Lock()
	session.conn = &stubConn{}
	session.ready = true
	for i := 0; i < activeStreams; i++ {
		session.streams[uint32(i+1)] = newMuxConn(session, uint32(i+1))
	}
	session.mu.Unlock()
	atomic.StoreInt64(&session.pendingBytes, pendingBytes)
	if pendingBytes > 0 {
		atomic.StoreInt64(&session.pendingFrames, 1)
	}
}

type stubConn struct{}

func (*stubConn) Read([]byte) (int, error)         { return 0, nil }
func (*stubConn) Write(b []byte) (int, error)      { return len(b), nil }
func (*stubConn) Close() error                     { return nil }
func (*stubConn) LocalAddr() net.Addr              { return nil }
func (*stubConn) RemoteAddr() net.Addr             { return nil }
func (*stubConn) SetDeadline(time.Time) error      { return nil }
func (*stubConn) SetReadDeadline(time.Time) error  { return nil }
func (*stubConn) SetWriteDeadline(time.Time) error { return nil }
