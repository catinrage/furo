package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeCaasifyAPI struct {
	mu           sync.Mutex
	createCount  int
	deleteCount  int
	createInputs []caasifyCreateRequest
	orders       []caasifyListedOrder
	ready        map[string]caasifyOrderInfo
}

func (f *fakeCaasifyAPI) createVPS(ctx context.Context, input caasifyCreateRequest) (caasifyCreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCount++
	f.createInputs = append(f.createInputs, input)
	orderID := input.Note + "-order"
	if f.ready == nil {
		f.ready = map[string]caasifyOrderInfo{}
	}
	f.ready[orderID] = caasifyOrderInfo{IP: "203.0.113.10", Password: "root-password"}
	return caasifyCreateResult{OrderID: orderID}, nil
}

func (f *fakeCaasifyAPI) waitForOrderReady(ctx context.Context, orderID string) (caasifyOrderInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if info, ok := f.ready[orderID]; ok {
		return info, nil
	}
	return caasifyOrderInfo{}, errors.New("order not ready")
}

func (f *fakeCaasifyAPI) deleteOrder(ctx context.Context, orderID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCount++
	return nil
}

func (f *fakeCaasifyAPI) listOrders(ctx context.Context) ([]caasifyListedOrder, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]caasifyListedOrder(nil), f.orders...), nil
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func TestMasterPromotesStandbyOnDeadActive(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := defaultMasterConfig()
	cfg.StateFile = filepath.Join(tmpDir, "state.json")
	cfg.RelayURL = "http://127.0.0.1/relay.php"
	cfg.CaasifyToken = "token"
	cfg.BackupCount = 0
	app := newMasterApp(cfg)
	app.statePath = cfg.StateFile
	app.state = masterState{
		Namespace:  "default",
		FleetID:    "fleet-a",
		Generation: 7,
		ActiveID:   "active-1",
		Nodes: []masterNode{
			{Namespace: "default", ID: "active-1", IP: "192.0.2.1", Role: "active", Status: "ready"},
			{Namespace: "default", ID: "standby-1", OrderID: "standby-order", IP: "192.0.2.2", Role: "standby", Status: "ready"},
		},
	}
	if err := app.saveStateLocked(); err != nil {
		t.Fatalf("save state: %v", err)
	}

	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("action") != "route-map" {
			t.Fatalf("unexpected relay action %q", r.URL.RawQuery)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer relay.Close()
	app.cfg.RelayURL = relay.URL + "/relay.php"
	app.caasify = &fakeCaasifyAPI{}

	if err := app.handleDeadNode(context.Background(), "active-1", "test failure"); err != nil {
		t.Fatalf("handleDeadNode() error = %v", err)
	}
	if app.state.ActiveID != "standby-1" {
		t.Fatalf("active id = %q, want standby-1", app.state.ActiveID)
	}
	if app.state.Generation != 8 {
		t.Fatalf("generation = %d, want 8", app.state.Generation)
	}
}

func TestMasterReplacesDeadStandby(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := defaultMasterConfig()
	cfg.StateFile = filepath.Join(tmpDir, "state.json")
	cfg.CaasifyToken = "token"
	cfg.BackupCount = 1
	app := newMasterApp(cfg)
	app.statePath = cfg.StateFile
	app.state = masterState{
		Namespace:  "default",
		FleetID:    "fleet-a",
		Generation: 7,
		ActiveID:   "active-1",
		Nodes: []masterNode{
			{Namespace: "default", ID: "active-1", IP: "192.0.2.1", Role: "active", Status: "ready"},
			{Namespace: "default", ID: "standby-1", OrderID: "standby-order", IP: "192.0.2.2", Role: "standby", Status: "ready"},
		},
	}
	if err := app.saveStateLocked(); err != nil {
		t.Fatalf("save state: %v", err)
	}

	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("action") != "route-map" {
			t.Fatalf("unexpected relay action %q", r.URL.RawQuery)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer relay.Close()
	app.cfg.RelayURL = relay.URL + "/relay.php"
	fake := &fakeCaasifyAPI{}
	app.caasify = fake
	app.bootstrapNodeFunc = func(context.Context, masterNode) error {
		return nil
	}
	app.verifyNodeRelayAccessFunc = func(context.Context, masterNode) error {
		return nil
	}

	if err := app.handleDeadNode(context.Background(), "standby-1", "relay health check failed"); err != nil {
		t.Fatalf("handleDeadNode() error = %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		fake.mu.Lock()
		deleteCount := fake.deleteCount
		fake.mu.Unlock()
		if deleteCount > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	fake.mu.Lock()
	deleteCount := fake.deleteCount
	createCount := fake.createCount
	fake.mu.Unlock()
	if deleteCount != 1 {
		t.Fatalf("delete count = %d, want 1", deleteCount)
	}
	if createCount != 1 {
		t.Fatalf("create count = %d, want 1", createCount)
	}
	if app.state.ActiveID != "active-1" {
		t.Fatalf("active id = %q, want active-1", app.state.ActiveID)
	}
	if !containsString(app.state.Retired, "standby-1") {
		t.Fatalf("retired = %#v, want standby-1", app.state.Retired)
	}
	readyStandbys := 0
	for _, node := range app.state.Nodes {
		if node.Role == "standby" && node.Status == "ready" {
			if node.ID == "standby-1" {
				t.Fatal("dead standby is still ready")
			}
			readyStandbys++
		}
	}
	if readyStandbys != 1 {
		t.Fatalf("ready standbys = %d, want 1", readyStandbys)
	}
}

func TestMasterStateRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "state.json")
	cfg := defaultMasterConfig()
	cfg.StateFile = path
	cfg.CaasifyToken = "token"
	cfg.RelayURL = "http://relay.test/furo.php"
	app := newMasterApp(cfg)
	app.statePath = path
	if err := app.loadState(); err != nil {
		t.Fatalf("loadState() create error = %v", err)
	}
	app.state.ActiveID = "node-a"
	app.state.Nodes = append(app.state.Nodes, masterNode{Namespace: "default", ID: "node-a", Role: "active", IP: "192.0.2.10"})
	if err := app.saveStateLocked(); err != nil {
		t.Fatalf("saveStateLocked() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var got masterState
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("state json: %v", err)
	}
	if got.ActiveID != "node-a" || len(got.Nodes) != 1 {
		t.Fatalf("state = %#v, want saved node", got)
	}
}

func TestMasterRouteMapUsesConfiguredSessionCount(t *testing.T) {
	cfg := defaultMasterConfig()
	cfg.CaasifyToken = "token"
	cfg.RelayURL = "http://relay.test/furo.php"
	cfg.RouteSessionCount = 9
	app := newMasterApp(cfg)
	app.state = masterState{
		Namespace:  "default",
		FleetID:    "furo-default",
		Generation: 2,
		ActiveID:   "active-a",
		Nodes: []masterNode{
			{Namespace: "default", ID: "active-a", Role: "active", Status: "ready", IP: "192.0.2.10"},
			{Namespace: "default", ID: "standby-a", Role: "standby", Status: "ready", IP: "192.0.2.11"},
		},
	}

	routeMap := app.routeMapLocked()
	if routeMap.Active == nil || routeMap.Active.SessionCount != 9 {
		t.Fatalf("active route = %#v, want session_count=9", routeMap.Active)
	}
	if len(routeMap.Standby) != 1 || routeMap.Standby[0].SessionCount != 9 {
		t.Fatalf("standby routes = %#v, want session_count=9", routeMap.Standby)
	}
}

func TestMasterPersistsNodeBeforeBootstrapFailure(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := defaultMasterConfig()
	cfg.Namespace = "sky-01"
	cfg.StateFile = filepath.Join(tmpDir, "state.json")
	cfg.CaasifyToken = "token"
	cfg.RelayURL = "http://relay.test/furo.php"
	app := newMasterApp(cfg)
	app.statePath = cfg.StateFile
	fake := &fakeCaasifyAPI{}
	app.caasify = fake
	app.bootstrapNodeFunc = func(context.Context, masterNode) error {
		return errors.New("ssh not ready")
	}
	if err := app.loadState(); err != nil {
		t.Fatalf("loadState() error = %v", err)
	}

	if _, err := app.createAndBootstrapNode(context.Background(), "active"); err == nil {
		t.Fatal("createAndBootstrapNode() error = nil, want bootstrap failure")
	}
	if fake.createCount != 1 {
		t.Fatalf("create count after first failure = %d, want 1", fake.createCount)
	}
	if app.state.ActiveID == "" || len(app.state.Nodes) != 1 {
		t.Fatalf("state active=%q nodes=%d, want persisted active node", app.state.ActiveID, len(app.state.Nodes))
	}
	node := app.state.Nodes[0]
	if node.Status != "bootstrap_failed" || node.OrderID == "" || node.IP == "" || node.Password == "" {
		t.Fatalf("node after bootstrap failure = %#v, want saved bootstrap_failed with order/ip/password", node)
	}

	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer relay.Close()
	app.cfg.RelayURL = relay.URL + "/relay.php"
	if err := app.ensureFleet(context.Background()); err == nil {
		t.Fatal("ensureFleet() error = nil, want repeated bootstrap failure")
	}
	if fake.createCount != 1 {
		t.Fatalf("create count after reconcile = %d, want no duplicate active", fake.createCount)
	}
	if len(app.state.Nodes) != 1 || app.state.ActiveID != node.ID {
		t.Fatalf("state after reconcile active=%q nodes=%d, want same active only", app.state.ActiveID, len(app.state.Nodes))
	}
}

func TestMasterCreateUsesConfiguredProviderPool(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := defaultMasterConfig()
	cfg.Namespace = "sky-01"
	cfg.StateFile = filepath.Join(tmpDir, "state.json")
	cfg.CaasifyToken = "token"
	cfg.RelayURL = "http://relay.test/furo.php"
	cfg.ProviderPool = []caasifyProviderOption{
		{Name: "test-region", Note: "VPS-Test", ProductID: 4242, Template: "test-template", IPv4: 1, IPv6: 0},
	}
	app := newMasterApp(cfg)
	app.statePath = cfg.StateFile
	fake := &fakeCaasifyAPI{}
	app.caasify = fake
	app.bootstrapNodeFunc = func(context.Context, masterNode) error {
		return nil
	}
	app.verifyNodeRelayAccessFunc = func(context.Context, masterNode) error {
		return nil
	}
	if err := app.loadState(); err != nil {
		t.Fatalf("loadState() error = %v", err)
	}

	if _, err := app.createAndBootstrapNode(context.Background(), "standby"); err != nil {
		t.Fatalf("createAndBootstrapNode() error = %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.createInputs) != 1 {
		t.Fatalf("create inputs = %d, want 1", len(fake.createInputs))
	}
	input := fake.createInputs[0]
	if input.ProductID != 4242 || input.Template != "test-template" || input.IPv4 != 1 || input.IPv6 != 0 {
		t.Fatalf("create input = %#v, want configured provider", input)
	}
}

func TestMasterReadyReportClearsBootstrapFailure(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := defaultMasterConfig()
	cfg.Namespace = "sky-01"
	cfg.StateFile = filepath.Join(tmpDir, "state.json")
	cfg.CaasifyToken = "token"
	cfg.RelayURL = "http://relay.test/furo.php"
	app := newMasterApp(cfg)
	app.statePath = cfg.StateFile
	app.state = masterState{
		Namespace:  "sky-01",
		FleetID:    "furo-sky-01",
		Generation: 6,
		Nodes: []masterNode{
			{Namespace: "sky-01", ID: "standby-a", OrderID: "order-a", IP: "192.0.2.10", Role: "standby", Status: "bootstrap_failed", LastError: "text file busy"},
		},
	}
	if err := app.saveStateLocked(); err != nil {
		t.Fatalf("save state: %v", err)
	}

	role := app.recordNodeReport(nodeReport{Namespace: "sky-01", NodeID: "standby-a", Role: "standby", Status: "ready"})
	if role != "standby" {
		t.Fatalf("role = %q, want standby", role)
	}
	node := app.findNodeLocked("standby-a")
	if node == nil {
		t.Fatal("node not found after report")
	}
	if node.Status != "ready" || node.LastError != "" || node.ReadyAt == "" {
		t.Fatalf("node after ready report = %#v, want ready with cleared error", node)
	}
	if app.state.Generation != 7 {
		t.Fatalf("generation = %d, want 7", app.state.Generation)
	}
}

func TestMasterDiscardsStandbyWhenRelayCheckFails(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := defaultMasterConfig()
	cfg.Namespace = "sky-01"
	cfg.StateFile = filepath.Join(tmpDir, "state.json")
	cfg.CaasifyToken = "token"
	cfg.RelayURL = "http://relay.test/furo.php"
	app := newMasterApp(cfg)
	app.statePath = cfg.StateFile
	fake := &fakeCaasifyAPI{}
	app.caasify = fake
	app.bootstrapNodeFunc = func(context.Context, masterNode) error {
		return nil
	}
	app.verifyNodeRelayAccessFunc = func(context.Context, masterNode) error {
		return errors.New("relay blocked")
	}
	if err := app.loadState(); err != nil {
		t.Fatalf("loadState() error = %v", err)
	}

	if _, err := app.createAndBootstrapNode(context.Background(), "standby"); err == nil {
		t.Fatal("createAndBootstrapNode() error = nil, want relay verification failure")
	}

	deadline := time.Now().Add(time.Second)
	for {
		fake.mu.Lock()
		deleteCount := fake.deleteCount
		fake.mu.Unlock()
		if deleteCount > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	fake.mu.Lock()
	deleteCount := fake.deleteCount
	fake.mu.Unlock()
	if deleteCount != 1 {
		t.Fatalf("delete count = %d, want 1", deleteCount)
	}

	app.mu.Lock()
	defer app.mu.Unlock()
	if len(app.state.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(app.state.Nodes))
	}
	node := app.state.Nodes[0]
	if node.Status != "deleted" && node.Status != "deleting" {
		t.Fatalf("node status = %q, want deleting/deleted", node.Status)
	}
	if !containsString(app.state.Retired, node.ID) {
		t.Fatalf("retired = %#v, want %s", app.state.Retired, node.ID)
	}
	routeMap := app.routeMapLocked()
	if len(routeMap.Standby) != 0 {
		t.Fatalf("standby routes = %#v, want none", routeMap.Standby)
	}
}

func TestMasterReadyReportDoesNotReviveDeletingStandby(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := defaultMasterConfig()
	cfg.Namespace = "sky-01"
	cfg.StateFile = filepath.Join(tmpDir, "state.json")
	cfg.CaasifyToken = "token"
	cfg.RelayURL = "http://relay.test/furo.php"
	app := newMasterApp(cfg)
	app.statePath = cfg.StateFile
	app.state = masterState{
		Namespace:  "sky-01",
		FleetID:    "furo-sky-01",
		Generation: 6,
		Nodes: []masterNode{
			{Namespace: "sky-01", ID: "standby-a", OrderID: "order-a", IP: "192.0.2.10", Role: "standby", Status: "deleting", LastError: "relay blocked"},
		},
		Retired: []string{"standby-a"},
	}
	if err := app.saveStateLocked(); err != nil {
		t.Fatalf("save state: %v", err)
	}

	role := app.recordNodeReport(nodeReport{Namespace: "sky-01", NodeID: "standby-a", Role: "standby", Status: "ready"})
	if role != "standby" {
		t.Fatalf("role = %q, want standby", role)
	}
	node := app.findNodeLocked("standby-a")
	if node == nil {
		t.Fatal("node not found after report")
	}
	if node.Status != "deleting" {
		t.Fatalf("node status = %q, want deleting", node.Status)
	}
	if app.state.Generation != 6 {
		t.Fatalf("generation = %d, want 6", app.state.Generation)
	}
}

func TestMasterImportsProviderNodeBeforeCreatingActive(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := defaultMasterConfig()
	cfg.Namespace = "sky-01"
	cfg.StateFile = filepath.Join(tmpDir, "state.json")
	cfg.CaasifyToken = "token"
	cfg.BackupCount = 0
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer relay.Close()
	cfg.RelayURL = relay.URL + "/relay.php"
	app := newMasterApp(cfg)
	app.statePath = cfg.StateFile
	fake := &fakeCaasifyAPI{
		orders: []caasifyListedOrder{
			{OrderID: "old-order", Note: "furo-server-sky-01-active-100", IP: "203.0.113.100", Status: "active"},
			{OrderID: "new-order", Note: "furo-server-sky-01-active-200", IP: "203.0.113.200", Status: "active"},
		},
		ready: map[string]caasifyOrderInfo{
			"old-order": {IP: "203.0.113.100", Password: "old-password"},
			"new-order": {IP: "203.0.113.200", Password: "new-password"},
		},
	}
	app.caasify = fake
	app.bootstrapNodeFunc = func(context.Context, masterNode) error { return nil }
	if err := app.loadState(); err != nil {
		t.Fatalf("loadState() error = %v", err)
	}

	if err := app.ensureFleet(context.Background()); err != nil {
		t.Fatalf("ensureFleet() error = %v", err)
	}
	if fake.createCount != 0 {
		t.Fatalf("create count = %d, want imported provider node instead of new VPS", fake.createCount)
	}
	if app.state.ActiveID != "furo-server-sky-01-active-200" {
		t.Fatalf("active id = %q, want newest imported active", app.state.ActiveID)
	}
	active := app.findNodeLocked(app.state.ActiveID)
	if active == nil || active.Status != "ready" || active.IP != "203.0.113.200" {
		t.Fatalf("active = %#v, want imported ready node", active)
	}
}

func TestRelayRouteMapURLAddsAction(t *testing.T) {
	got, err := relayRouteMapURL("https://example.com/furo-relay.php?x=1", "fleet-a")
	if err != nil {
		t.Fatalf("relayRouteMapURL() error = %v", err)
	}
	if !strings.Contains(got, "action=route-map") || !strings.Contains(got, "namespace=fleet-a") || !strings.Contains(got, "x=1") {
		t.Fatalf("route map url = %q", got)
	}
}

func TestCaasifyShowOrderParsesScriptShape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"secret": "root-password",
				"view": map[string]any{
					"references": []any{
						map[string]any{"reference": map[string]any{"type": "ipv4"}, "value": "203.0.113.20"},
					},
				},
			},
		})
	}))
	defer server.Close()

	client := newCaasifyClient("token")
	client.client = server.Client()
	// Exercise the JSON extraction helpers directly; the real method uses the same response shape.
	body, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(body.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	root := decoded["data"].(map[string]any)
	if got := firstIPv4(root); got != "203.0.113.20" {
		t.Fatalf("firstIPv4() = %q", got)
	}
	if got := jsonString(root, "secret"); got != "root-password" {
		t.Fatalf("secret = %q", got)
	}
}

func TestDopraxClientCreateListDelete(t *testing.T) {
	var sawCreate bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/account/login-doprax-123321/":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"cca":     "bearer-token",
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/services/instances/":
			if got := r.Header.Get("Authorization"); got != "Bearer bearer-token" {
				t.Fatalf("Authorization = %q, want bearer", got)
			}
			if !strings.Contains(r.Header.Get("Cookie"), "cca=bearer-token") {
				t.Fatalf("Cookie = %q, want cca token", r.Header.Get("Cookie"))
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("create body decode: %v", err)
			}
			if body["name"] != "furo-node-a" {
				t.Fatalf("create name = %#v, want furo-node-a", body["name"])
			}
			sawCreate = true
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"service_id": "svc-1",
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/services/instances/svc-1/":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"service": map[string]any{
						"status": "running",
					},
					"access": map[string]any{
						"public_ipv4": "203.0.113.50",
					},
					"links": map[string]any{
						"vm_code": "vm-1",
					},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/vms/vm-1/actions/access/":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"vmUsername": "root",
					"tempPass":   "root-password",
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/vms/":
			if got := r.Header.Get("X-API-Key"); got != "v1-key" {
				t.Fatalf("X-API-Key = %q, want v1-key", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": []map[string]any{
					{"vmCode": "vm-1", "name": "furo-node-a", "status": "running", "ipv4": "203.0.113.50"},
				},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/vms/vm-1/":
			if got := r.Header.Get("X-API-Key"); got != "v1-key" {
				t.Fatalf("X-API-Key = %q, want v1-key", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newDopraxClient(masterConfigFile{
		DopraxAPIKey:           "v1-key",
		DopraxUsername:         "user@example.com",
		DopraxPassword:         "secret",
		DopraxBaseURL:          server.URL,
		DopraxProductVersionID: "product-version",
		DopraxLocationOptionID: "location-option",
		DopraxOSOptionID:       "os-option",
		DopraxAccessMethod:     "password",
	})
	client.client = server.Client()

	created, err := client.createVPS(context.Background(), caasifyCreateRequest{Note: "furo-node-a"})
	if err != nil {
		t.Fatalf("createVPS() error = %v", err)
	}
	if !sawCreate {
		t.Fatal("create endpoint was not called")
	}
	if created.OrderID != "vm-1" || created.IP != "203.0.113.50" || created.Password != "root-password" {
		t.Fatalf("created = %#v, want vm/ip/password", created)
	}

	orders, err := client.listOrders(context.Background())
	if err != nil {
		t.Fatalf("listOrders() error = %v", err)
	}
	if len(orders) != 1 || orders[0].OrderID != "vm-1" || orders[0].Note != "furo-node-a" {
		t.Fatalf("orders = %#v, want listed VM", orders)
	}
	if err := client.deleteOrder(context.Background(), "vm-1"); err != nil {
		t.Fatalf("deleteOrder() error = %v", err)
	}
}

func TestDopraxClientRefreshesTokenAfterAuthFailure(t *testing.T) {
	var loginCount int
	var createCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/account/login-doprax-123321/":
			loginCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"cca":     map[bool]string{true: "fresh-token", false: "expired-token"}[loginCount > 1],
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/services/instances/":
			createCount++
			if createCount == 1 {
				http.Error(w, `{"success":false,"error":"token expired"}`, http.StatusUnauthorized)
				return
			}
			if got := r.Header.Get("Authorization"); got != "Bearer fresh-token" {
				t.Fatalf("Authorization after refresh = %q, want fresh token", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"service_id": "svc-1"}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/services/instances/svc-1/":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"service": map[string]any{"status": "running"},
					"access":  map[string]any{"public_ipv4": "203.0.113.50"},
					"links":   map[string]any{"vm_code": "vm-1"},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/vms/vm-1/actions/access/":
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"tempPass": "root-password"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := newDopraxClient(masterConfigFile{
		DopraxAPIKey:             "v1-key",
		DopraxUsername:           "user@example.com",
		DopraxPassword:           "secret",
		DopraxBaseURL:            server.URL,
		DopraxProductVersionID:   "product-version",
		DopraxLocationOptionID:   "location-option",
		DopraxOSOptionID:         "os-option",
		DopraxAccessMethod:       "password",
		DopraxLoginRetryAttempts: 1,
		DopraxLoginRetryDelaySec: 1,
	})
	client.client = server.Client()

	if _, err := client.createVPS(context.Background(), caasifyCreateRequest{Note: "furo-node-a"}); err != nil {
		t.Fatalf("createVPS() error = %v", err)
	}
	if loginCount != 2 {
		t.Fatalf("login count = %d, want 2", loginCount)
	}
	if createCount != 2 {
		t.Fatalf("create count = %d, want 2", createCount)
	}
}

func TestMasterStaticEgressStatePersistsNodeKeys(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := defaultMasterConfig()
	cfg.Namespace = "sky-01"
	cfg.StateFile = filepath.Join(tmpDir, "state.json")
	cfg.CaasifyToken = "token"
	cfg.ProviderBackend = "caasify"
	cfg.StaticEgress.Enabled = true
	app := newMasterApp(cfg)
	app.statePath = cfg.StateFile
	if err := app.loadState(); err != nil {
		t.Fatalf("loadState() error = %v", err)
	}

	app.mu.Lock()
	node := masterNode{Namespace: "sky-01", ID: "node-a", Role: "standby", Status: "created"}
	if err := app.ensureNodeStaticEgressLocked(&node); err != nil {
		app.mu.Unlock()
		t.Fatalf("ensureNodeStaticEgressLocked() error = %v", err)
	}
	app.state.Nodes = append(app.state.Nodes, node)
	if err := app.saveStateLocked(); err != nil {
		app.mu.Unlock()
		t.Fatalf("saveStateLocked() error = %v", err)
	}
	app.mu.Unlock()

	data, err := os.ReadFile(cfg.StateFile)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var saved masterState
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("state json: %v", err)
	}
	if saved.StaticEgress.PrivateKey == "" || saved.StaticEgress.PublicKey == "" {
		t.Fatalf("static egress master keys were not saved: %#v", saved.StaticEgress)
	}
	if len(saved.Nodes) != 1 || saved.Nodes[0].EgressTunnelIP == "" || saved.Nodes[0].WireGuardPrivateKey == "" || saved.Nodes[0].WireGuardPublicKey == "" {
		t.Fatalf("node static egress fields not saved: %#v", saved.Nodes)
	}
}

func TestRenderMasterWireGuardConfigAllowsForwarding(t *testing.T) {
	cfg := defaultMasterConfig()
	cfg.StaticEgress.Enabled = true
	cfg.StaticEgress.Interface = "wg-furo"
	cfg.StaticEgress.Subnet = "10.66.0.0/24"
	cfg.StaticEgress.MasterTunnelIP = "10.66.0.1"
	app := newMasterApp(cfg)
	state := masterState{
		StaticEgress: masterStaticEgressState{
			PrivateKey: "master-private",
		},
		Nodes: []masterNode{{
			EgressEnabled:      true,
			EgressTunnelIP:     "10.66.0.2",
			WireGuardPublicKey: "node-public",
			Status:             "ready",
		}},
	}
	rendered, err := app.renderMasterWireGuardConfig(state)
	if err != nil {
		t.Fatalf("renderMasterWireGuardConfig() error = %v", err)
	}
	for _, want := range []string{
		"PostUp = iptables -t nat -A POSTROUTING -s 10.66.0.0/24 -j MASQUERADE",
		"PostUp = iptables -I FORWARD 1 -i %i -j ACCEPT",
		"PostUp = iptables -I FORWARD 1 -o %i -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT",
		"PostDown = iptables -D FORWARD -i %i -j ACCEPT",
		"AllowedIPs = 10.66.0.2/32",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered WireGuard config missing %q:\n%s", want, rendered)
		}
	}
}

func TestRenderBootstrapScriptUsesRoutePriorityBeforeMain(t *testing.T) {
	cfg := defaultMasterConfig()
	cfg.StaticEgress.Enabled = true
	cfg.StaticEgress.NodeRouteTable = 51820
	cfg.StaticEgress.NodeRoutePriority = 1000
	cfg.NodeMaxSessions = 120
	cfg.PublicURL = "http://203.0.113.1:19082"
	app := newMasterApp(cfg)
	app.staticEgressAvailable = true
	app.state.StaticEgress.PublicKey = "master-public"
	node := masterNode{
		EgressEnabled:       true,
		EgressTunnelIP:      "10.66.0.2",
		WireGuardPrivateKey: "node-private",
	}
	rendered, err := app.renderBootstrapScript("priority={{wg_node_route_priority}} table={{wg_node_route_table}} max={{node_max_sessions}} enabled={{static_egress_enabled}}", node)
	if err != nil {
		t.Fatalf("renderBootstrapScript() error = %v", err)
	}
	if rendered != "priority=1000 table=51820 max=120 enabled=true" {
		t.Fatalf("rendered = %q, want route priority before main table", rendered)
	}
}

func TestRunSSHScriptHonorsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	if err := runSSHScript(ctx, "127.0.0.1", "bad", "true"); err == nil {
		t.Fatal("runSSHScript() error = nil, want context or dial failure")
	}
}
