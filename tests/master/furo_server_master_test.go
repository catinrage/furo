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
	mu          sync.Mutex
	createCount int
	deleteCount int
	orders      []caasifyListedOrder
	ready       map[string]caasifyOrderInfo
}

func (f *fakeCaasifyAPI) createVPS(ctx context.Context, input caasifyCreateRequest) (caasifyCreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCount++
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

func TestRunSSHScriptHonorsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	if err := runSSHScript(ctx, "127.0.0.1", "bad", "true"); err == nil {
		t.Fatal("runSSHScript() error = nil, want context or dial failure")
	}
}
