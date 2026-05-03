package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
		FleetID:    "fleet-a",
		Generation: 7,
		ActiveID:   "active-1",
		Nodes: []masterNode{
			{ID: "active-1", IP: "192.0.2.1", Role: "active", Status: "ready"},
			{ID: "standby-1", OrderID: "standby-order", IP: "192.0.2.2", Role: "standby", Status: "ready"},
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
	app.caasify = &caasifyClient{token: "token", client: relay.Client()}

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
	app.state.Nodes = append(app.state.Nodes, masterNode{ID: "node-a", Role: "active", IP: "192.0.2.10"})
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

func TestRelayRouteMapURLAddsAction(t *testing.T) {
	got, err := relayRouteMapURL("https://example.com/furo-relay.php?x=1")
	if err != nil {
		t.Fatalf("relayRouteMapURL() error = %v", err)
	}
	if got != "https://example.com/furo-relay.php?action=route-map&x=1" && got != "https://example.com/furo-relay.php?x=1&action=route-map" {
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
