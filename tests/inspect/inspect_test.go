package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInspectSelectRoutePrefersRequestedEnabledRoute(t *testing.T) {
	t.Parallel()

	disabled := false
	enabled := true
	cfg := inspectClientConfig{
		PublicHost: "198.51.100.10",
		PublicPort: 28080,
		Routes: []inspectRouteConfigFile{
			{
				ID:         "disabled",
				RelayURL:   "https://relay-disabled.example/furo.php",
				ServerHost: "203.0.113.1",
				ServerPort: 8443,
				Enabled:    &disabled,
			},
			{
				ID:         "wanted",
				RelayURL:   "https://relay-wanted.example/furo.php",
				ServerHost: "203.0.113.2",
				ServerPort: 9443,
				Enabled:    &enabled,
			},
		},
	}

	route, err := inspectSelectRoute(cfg, "wanted")
	if err != nil {
		t.Fatalf("inspectSelectRoute() error = %v", err)
	}
	if route.ID != "wanted" {
		t.Fatalf("route id = %q, want %q", route.ID, "wanted")
	}
	if route.PublicHost != cfg.PublicHost || route.PublicPort != cfg.PublicPort {
		t.Fatalf("route callback address = %s:%d, want inherited %s:%d", route.PublicHost, route.PublicPort, cfg.PublicHost, cfg.PublicPort)
	}
}

func TestInspectSelectRouteRejectsMissingRequestedRoute(t *testing.T) {
	t.Parallel()

	cfg := inspectClientConfig{
		Routes: []inspectRouteConfigFile{
			{ID: "route-a", RelayURL: "https://relay-a.example/furo.php", ServerHost: "203.0.113.10", ServerPort: 8443},
		},
	}

	if _, err := inspectSelectRoute(cfg, "route-b"); err == nil {
		t.Fatal("inspectSelectRoute() error = nil, want missing route error")
	}
}

func TestInspectLoadClientConfigSelectsRequestedRoute(t *testing.T) {
	originalRouteID := *inspectRouteID
	t.Cleanup(func() {
		*inspectRouteID = originalRouteID
	})

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.client.json")
	payload := `{
  "api_key": "secret",
  "agent_listen": "0.0.0.0:28080",
  "public_host": "198.51.100.20",
  "public_port": 28080,
  "open_timeout": "10s",
  "keepalive": "5s",
  "routes": [
    {
      "id": "primary",
      "relay_url": "https://relay-a.example/furo.php",
      "server_host": "203.0.113.1",
      "server_port": 8443,
      "session_count": 2,
      "enabled": true
    },
    {
      "id": "backup",
      "relay_url": "https://relay-b.example/furo.php",
      "server_host": "203.0.113.2",
      "server_port": 9443,
      "session_count": 2,
      "enabled": true
    }
  ]
}`
	if err := os.WriteFile(configPath, []byte(payload), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	*inspectRouteID = "backup"
	cfg, timeout, err := inspectLoadClientConfig(configPath)
	if err != nil {
		t.Fatalf("inspectLoadClientConfig() error = %v", err)
	}
	if timeout.String() != "10s" {
		t.Fatalf("open timeout = %s, want 10s", timeout)
	}
	if cfg.RelayURL != "https://relay-b.example/furo.php" {
		t.Fatalf("relay_url = %q, want backup route", cfg.RelayURL)
	}
	if cfg.ServerHost != "203.0.113.2" || cfg.ServerPort != 9443 {
		t.Fatalf("server = %s:%d, want 203.0.113.2:9443", cfg.ServerHost, cfg.ServerPort)
	}
	if cfg.PublicHost != "198.51.100.20" || cfg.PublicPort != 28080 {
		t.Fatalf("public callback = %s:%d, want inherited 198.51.100.20:28080", cfg.PublicHost, cfg.PublicPort)
	}
}

func TestInspectLoadAllClientConfigsSelectsEnabledRoutes(t *testing.T) {
	originalRouteID := *inspectRouteID
	t.Cleanup(func() {
		*inspectRouteID = originalRouteID
	})

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.client.json")
	payload := `{
  "api_key": "secret",
  "agent_listen": "0.0.0.0:28080",
  "public_host": "198.51.100.20",
  "public_port": 28080,
  "open_timeout": "10s",
  "keepalive": "5s",
  "routes": [
    {
      "id": "primary",
      "relay_url": "https://relay-a.example/furo.php",
      "server_host": "203.0.113.1",
      "server_port": 8443,
      "session_count": 2,
      "enabled": true
    },
    {
      "id": "backup",
      "relay_url": "https://relay-b.example/furo.php",
      "public_host": "198.51.100.21",
      "public_port": 29080,
      "server_host": "203.0.113.2",
      "server_port": 9443,
      "session_count": 2,
      "enabled": true
    },
    {
      "id": "disabled",
      "relay_url": "https://relay-disabled.example/furo.php",
      "server_host": "203.0.113.3",
      "server_port": 10443,
      "session_count": 2,
      "enabled": false
    }
  ]
}`
	if err := os.WriteFile(configPath, []byte(payload), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	*inspectRouteID = ""
	cfgs, timeout, err := inspectLoadAllClientConfigs(configPath)
	if err != nil {
		t.Fatalf("inspectLoadAllClientConfigs() error = %v", err)
	}
	if timeout.String() != "10s" {
		t.Fatalf("open timeout = %s, want 10s", timeout)
	}
	if len(cfgs) != 2 {
		t.Fatalf("config count = %d, want 2", len(cfgs))
	}
	if cfgs[0].SelectedRouteID != "primary" || cfgs[1].SelectedRouteID != "backup" {
		t.Fatalf("selected route ids = %q/%q, want primary/backup", cfgs[0].SelectedRouteID, cfgs[1].SelectedRouteID)
	}
	if cfgs[0].PublicHost != "198.51.100.20" || cfgs[0].PublicPort != 28080 {
		t.Fatalf("first callback = %s:%d, want inherited", cfgs[0].PublicHost, cfgs[0].PublicPort)
	}
	if cfgs[1].PublicHost != "198.51.100.21" || cfgs[1].PublicPort != 29080 {
		t.Fatalf("second callback = %s:%d, want route override", cfgs[1].PublicHost, cfgs[1].PublicPort)
	}
}

func TestInspectLoadAllClientConfigsRejectsRouteID(t *testing.T) {
	originalRouteID := *inspectRouteID
	t.Cleanup(func() {
		*inspectRouteID = originalRouteID
	})

	*inspectRouteID = "primary"
	if _, _, err := inspectLoadAllClientConfigs("unused.json"); err == nil {
		t.Fatal("inspectLoadAllClientConfigs() error = nil, want --all/--route-id conflict")
	}
}
