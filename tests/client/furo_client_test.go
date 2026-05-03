package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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

func TestAdminAuthCookieAllowsPanelAccess(t *testing.T) {
	originalAPIKey := apiKey
	t.Cleanup(func() { apiKey = originalAPIKey })
	apiKey = "panel-secret"

	req := httptest.NewRequest("GET", "/", nil)
	if isAdminAuthenticated(req) {
		t.Fatal("isAdminAuthenticated() = true without API key or cookie")
	}

	req.Header.Set("X-API-KEY", apiKey)
	if !isAdminAuthenticated(req) {
		t.Fatal("isAdminAuthenticated() = false with X-API-KEY")
	}

	req = httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	setAdminAuthCookie(rec, req)
	for _, cookie := range rec.Result().Cookies() {
		req.AddCookie(cookie)
	}
	if !isAdminAuthenticated(req) {
		t.Fatal("isAdminAuthenticated() = false with auth cookie")
	}
}

func TestAdminPanelTemplateIncludesAIRSAndInspect(t *testing.T) {
	rec := httptest.NewRecorder()
	renderAdminPanel(rec, adminPageData{
		Status: clientStatusResponse{
			Service:        "furo-client",
			ClientID:       "client-test",
			RouteSelection: string(routeSelectionLeastLoad),
		},
		ConfigPath:         "/tmp/config.client.json",
		Config:             adminConfigView{AdminListen: "127.0.0.1:19080"},
		AdminListen:        "127.0.0.1:19080",
		ClientServiceState: "client ok",
		AIRSServiceState:   "airs ok",
		InspectOutput:      "✓ Ping: avg=12ms samples=10ms, 14ms",
		InspectSummary:     parseInspectSummary("✓ Ping: avg=12ms samples=10ms, 14ms", nil),
	})

	body := rec.Body.String()
	for _, token := range []string{"AIRS", `action="/inspect"`, "avg=12ms", "127.0.0.1:19080", "Save config"} {
		if !strings.Contains(body, token) {
			t.Fatalf("panel output missing %q\n%s", token, body)
		}
	}
}

func TestParseInspectSummaryExtractsPing(t *testing.T) {
	t.Parallel()

	summary := parseInspectSummary("✓ Route: relay-a\n✓ Ping: avg=42ms samples=40ms, 44ms\n✓ FURO inspect succeeded", nil)
	if !summary.OK {
		t.Fatal("summary.OK = false, want true")
	}
	if summary.Ping != "avg=42ms samples=40ms, 44ms" {
		t.Fatalf("Ping = %q, want parsed ping", summary.Ping)
	}
	if len(summary.Routes) != 1 || summary.Routes[0] != "relay-a" {
		t.Fatalf("Routes = %#v, want relay-a", summary.Routes)
	}
}

func TestSaveClientConfigFormWritesAdminListenAndPreservesUnknownAIRS(t *testing.T) {
	originalConfigPath := *configPath
	t.Cleanup(func() { *configPath = originalConfigPath })

	path := filepath.Join(t.TempDir(), "config.client.json")
	payload := `{
  "client_id": "old",
  "route_selection": "least_load",
  "api_key": "secret",
  "socks_listen": "127.0.0.1:18713",
  "agent_listen": "0.0.0.0:28080",
  "public_host": "198.51.100.10",
  "public_port": 28080,
  "control_panel_listen": "127.0.0.1:19080",
  "open_timeout": "10s",
  "keepalive": "5s",
  "write_timeout": "9s",
  "frame_min_size": 16384,
  "frame_mid_size": 32768,
  "frame_max_size": 262144,
  "airs": {"custom_future_field": "keep-me"},
  "routes": [{"id":"old","relay_url":"https://old.example/furo.php","server_host":"203.0.113.1","server_port":8443,"session_count":1,"enabled":true}]
}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	*configPath = path

	form := url.Values{
		"client_id":                             {"client-new"},
		"route_selection":                       {"least_latency"},
		"api_key":                               {"secret"},
		"socks_listen":                          {"127.0.0.1:18713"},
		"socks_auth":                            {"alice:secret"},
		"agent_listen":                          {"0.0.0.0:28080"},
		"admin_listen":                          {"127.0.0.1:29080"},
		"public_host":                           {"198.51.100.20"},
		"public_port":                           {"29080"},
		"open_timeout":                          {"12s"},
		"keepalive":                             {"7s"},
		"write_timeout":                         {"9s"},
		"frame_min_size":                        {"16384"},
		"frame_mid_size":                        {"32768"},
		"frame_max_size":                        {"262144"},
		"log_file":                              {""},
		"airs_arvan_api_key":                    {"apikey test"},
		"airs_arvan_region":                     {"ir-thr-fr1"},
		"airs_arvan_server_id":                  {"server-1"},
		"airs_fixed_public_ip":                  {"198.51.100.1"},
		"airs_auto_renew_interval_seconds":      {"1800"},
		"airs_cleanup_interval_minutes":         {"60"},
		"airs_check_interval_seconds":           {"10"},
		"airs_failure_confirm_attempts":         {"5"},
		"airs_failure_confirm_interval_seconds": {"4"},
		"airs_log_file":                         {"./airs.log"},
		"airs_switch_script":                    {"./switch-outbound-ip.sh"},
		"airs_inspect_binary":                   {"./inspect"},
		"airs_service_script":                   {"./service.sh"},
		"airs_client_service_role":              {"client"},
		"airs_outbound_check_url":               {"https://example.com/ip"},
		"airs_outbound_check_retry_seconds":     {"480"},
		"airs_post_add_wait_seconds":            {"5"},
		"airs_post_detach_wait_seconds":         {"5"},
		"routes_count":                          {"1"},
		"routes_0_id":                           {"relay-a"},
		"routes_0_relay_url":                    {"https://relay.example/furo.php"},
		"routes_0_server_host":                  {"203.0.113.10"},
		"routes_0_server_port":                  {"9443"},
		"routes_0_session_count":                {"3"},
		"routes_0_public_host":                  {""},
		"routes_0_public_port":                  {""},
		"routes_0_enabled":                      {"on"},
	}
	req := httptest.NewRequest(http.MethodPost, "/config", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := saveClientConfigForm(req); err != nil {
		t.Fatalf("saveClientConfigForm() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if got["admin_listen"] != "127.0.0.1:29080" {
		t.Fatalf("admin_listen = %v, want new admin listen", got["admin_listen"])
	}
	if _, exists := got["control_panel_listen"]; exists {
		t.Fatal("control_panel_listen still exists after structured save")
	}
	airs := got["airs"].(map[string]any)
	if airs["custom_future_field"] != "keep-me" {
		t.Fatalf("custom_future_field = %v, want preserved", airs["custom_future_field"])
	}
	routes := got["routes"].([]any)
	if routes[0].(map[string]any)["id"] != "relay-a" {
		t.Fatalf("route id = %v, want relay-a", routes[0].(map[string]any)["id"])
	}
}

func TestValidateSOCKSAuth(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"user:pass", "user:p:a:s:s"} {
		if err := validateSOCKSAuth(value); err != nil {
			t.Fatalf("validateSOCKSAuth(%q) error = %v", value, err)
		}
	}
	for _, value := range []string{"", "user", ":pass", "user:"} {
		if err := validateSOCKSAuth(value); err == nil {
			t.Fatalf("validateSOCKSAuth(%q) error = nil, want error", value)
		}
	}
}

func TestBuildSOCKSConfigEnablesAuthWhenConfigured(t *testing.T) {
	originalSOCKSAuth := socksAuth
	originalClientID := clientID
	t.Cleanup(func() {
		socksAuth = originalSOCKSAuth
		clientID = originalClientID
	})
	clientID = "client-test"
	socksAuth = "alice:secret"

	conf := buildSOCKSConfig(newSessionPool(1))
	if conf.Credentials == nil {
		t.Fatal("Credentials = nil, want username/password auth")
	}
	if !conf.Credentials.Valid("alice", "secret") {
		t.Fatal("configured credentials were not accepted")
	}
	if conf.Credentials.Valid("alice", "wrong") {
		t.Fatal("wrong password was accepted")
	}
}

func TestRunServiceCommandSupportsAIRSRole(t *testing.T) {
	originalAIRS := airsSettings
	t.Cleanup(func() { airsSettings = originalAIRS })

	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "service.sh")
	if err := os.WriteFile(scriptPath, []byte("#!/usr/bin/env bash\necho \"$1 $2\"\n"), 0755); err != nil {
		t.Fatalf("write service script: %v", err)
	}
	airsSettings.ServiceScript = scriptPath

	output, err := runServiceCommand("airs", "status")
	if err != nil {
		t.Fatalf("runServiceCommand() error = %v", err)
	}
	if output != "airs status" {
		t.Fatalf("service output = %q, want airs status", output)
	}
}

func TestRunInspectCommandUsesConfiguredInspect(t *testing.T) {
	originalAIRS := airsSettings
	originalConfigPath := *configPath
	t.Cleanup(func() {
		airsSettings = originalAIRS
		*configPath = originalConfigPath
	})

	tmpDir := t.TempDir()
	inspectPath := filepath.Join(tmpDir, "inspect")
	configFile := filepath.Join(tmpDir, "config.client.json")
	if err := os.WriteFile(inspectPath, []byte("#!/usr/bin/env bash\necho \"inspect $@\"\n"), 0755); err != nil {
		t.Fatalf("write inspect script: %v", err)
	}
	if err := os.WriteFile(configFile, []byte(`{"api_key":"secret"}`), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	airsSettings.InspectBinary = inspectPath
	*configPath = configFile

	output, err := runInspectCommand()
	if err != nil {
		t.Fatalf("runInspectCommand() error = %v", err)
	}
	want := "inspect -c " + configFile + " --all"
	if output != want {
		t.Fatalf("inspect output = %q, want %q", output, want)
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
	markSessionReady(pool.order[0], 3, int64(3*maxFramePayload))
	markSessionReady(pool.order[1], 1, int64(maxFramePayload/2))
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
	markSessionReady(pool.order[0], 2, int64(maxFramePayload))
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
	originalSOCKSAuth := socksAuth
	originalAgentListen := agentListen
	originalPublicHost := publicHost
	originalPublicPort := publicPort
	originalServerHost := serverHost
	originalServerPort := serverPort
	originalAdminListen := adminListen
	originalOpenTimeout := openTimeout
	originalKeepalive := keepalivePeriod
	originalWriteTimeout := writeTimeout
	originalMinFramePayload := minFramePayload
	originalMidFramePayload := midFramePayload
	originalMaxFramePayload := maxFramePayload
	originalSessionCount := sessionCount
	originalLogFilePath := logFilePath
	originalAIRSSettings := airsSettings
	t.Cleanup(func() {
		clientID = originalClientID
		routeSelection = originalRouteSelection
		clientRoutes = originalRoutes
		relayURL = originalRelayURL
		apiKey = originalAPIKey
		socksListen = originalSocksListen
		socksAuth = originalSOCKSAuth
		agentListen = originalAgentListen
		publicHost = originalPublicHost
		publicPort = originalPublicPort
		serverHost = originalServerHost
		serverPort = originalServerPort
		adminListen = originalAdminListen
		openTimeout = originalOpenTimeout
		keepalivePeriod = originalKeepalive
		writeTimeout = originalWriteTimeout
		minFramePayload = originalMinFramePayload
		midFramePayload = originalMidFramePayload
		maxFramePayload = originalMaxFramePayload
		sessionCount = originalSessionCount
		logFilePath = originalLogFilePath
		airsSettings = originalAIRSSettings
	})

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.client.json")
	payload := `{
  "client_id": "client-a",
  "route_selection": "least_latency",
  "api_key": "secret",
  "socks_listen": "127.0.0.1:18713",
  "socks_auth": "alice:secret",
  "agent_listen": "0.0.0.0:28080",
  "public_host": "198.51.100.10",
  "public_port": 28080,
  "admin_listen": "127.0.0.1:19080",
  "control_panel_listen": "127.0.0.1:29080",
  "open_timeout": "12s",
  "keepalive": "7s",
  "write_timeout": "9s",
  "frame_min_size": 16384,
  "frame_mid_size": 32768,
  "frame_max_size": 262144,
  "log_file": "",
  "airs": {
    "inspect_binary": "./custom-inspect",
    "service_script": "./custom-service.sh",
    "client_service_role": "client"
  },
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
	if writeTimeout != 9*time.Second {
		t.Fatalf("writeTimeout = %s, want 9s", writeTimeout)
	}
	if adminListen != "127.0.0.1:19080" {
		t.Fatalf("adminListen = %q, want configured admin listen", adminListen)
	}
	if socksAuth != "alice:secret" {
		t.Fatalf("socksAuth = %q, want configured auth", socksAuth)
	}
	if airsSettings.InspectBinary != "./custom-inspect" || airsSettings.ServiceScript != "./custom-service.sh" {
		t.Fatalf("airs settings = %#v, want configured inspect and service scripts", airsSettings)
	}
	if minFramePayload != 16384 || midFramePayload != 32768 || maxFramePayload != 262144 {
		t.Fatalf("frame sizes = %d/%d/%d, want 16384/32768/262144", minFramePayload, midFramePayload, maxFramePayload)
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

func TestManagedRouteMapPersistAddsActiveAndStandby(t *testing.T) {
	originalConfigPath := *configPath
	t.Cleanup(func() { *configPath = originalConfigPath })

	configFile := filepath.Join(t.TempDir(), "config.client.json")
	*configPath = configFile
	if err := os.WriteFile(configFile, []byte(`{
  "api_key": "secret",
  "socks_listen": "127.0.0.1:0",
  "agent_listen": "127.0.0.1:0",
  "public_host": "198.51.100.10",
  "public_port": 28080,
  "routes": [
    {"id": "user", "relay_url": "https://relay.example/furo.php", "server_host": "203.0.113.1", "server_port": 8443, "session_count": 1, "enabled": true}
  ]
}`), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	err := persistRelayRouteMap(relayRouteMap{
		Namespace:  "fleet-a",
		FleetID:    "fleet-a",
		Generation: 3,
		Active:     &relayRouteSpec{ID: "active-a", RelayURL: "https://relay.example/furo.php", ServerHost: "203.0.113.10", ServerPort: 8443, SessionCount: 2},
		Standby:    []relayRouteSpec{{ID: "standby-a", RelayURL: "https://relay.example/furo.php", ServerHost: "203.0.113.11", ServerPort: 8443, SessionCount: 2}},
	})
	if err != nil {
		t.Fatalf("persistRelayRouteMap() error = %v", err)
	}
	data, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var got struct {
		Routes []clientRouteConfigFile `json:"routes"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("config json: %v", err)
	}
	if len(got.Routes) != 3 {
		t.Fatalf("routes count = %d, want user + active + standby", len(got.Routes))
	}
	if got.Routes[1].Role != "active" || got.Routes[1].ManagedBy != "fleet-a" || got.Routes[1].Enabled == nil || !*got.Routes[1].Enabled {
		t.Fatalf("active route = %#v", got.Routes[1])
	}
	if got.Routes[2].Role != "standby" || got.Routes[2].Enabled == nil || *got.Routes[2].Enabled {
		t.Fatalf("standby route = %#v", got.Routes[2])
	}
}

func TestPromoteManagedStandbyDisablesOldActive(t *testing.T) {
	trueValue := true
	falseValue := false
	routes, _, err := buildClientRoutes(clientConfigFile{
		APIKey:     "secret",
		PublicHost: "198.51.100.10",
		PublicPort: 28080,
		Routes: []clientRouteConfigFile{
			{ID: "active-a", RelayURL: "https://relay.example/furo.php", ServerHost: "203.0.113.10", ServerPort: 8443, SessionCount: 1, Enabled: &trueValue, ManagedBy: "fleet-a", Role: "active", Generation: 3},
			{ID: "standby-a", RelayURL: "https://relay.example/furo.php", ServerHost: "203.0.113.11", ServerPort: 8443, SessionCount: 1, Enabled: &falseValue, ManagedBy: "fleet-a", Role: "standby", Generation: 3},
		},
	})
	if err != nil {
		t.Fatalf("buildClientRoutes() error = %v", err)
	}
	pool := newSessionPoolForRoutes(routes, routeSelectionLeastLoad)
	promoted, ok := pool.promoteManagedStandby()
	if !ok {
		t.Fatal("promoteManagedStandby() ok = false")
	}
	if promoted.ID != "standby-a" || promoted.Role != "active" || !promoted.Enabled {
		t.Fatalf("promoted = %#v", promoted)
	}
	if pool.routes[0].cfg.Role != "retired" || pool.routes[0].cfg.Enabled {
		t.Fatalf("old active route = %#v", pool.routes[0].cfg)
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
