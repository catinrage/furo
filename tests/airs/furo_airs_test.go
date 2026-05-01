package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
)

func TestDetachIPSkipsMissingOldPublicHost(t *testing.T) {
	t.Parallel()

	manager := &airsManager{
		cfg: airsRuntimeConfig{
			ArvanServerID: "server-1",
			FixedPublicIP: "37.152.190.18",
		},
		logger: log.New(io.Discard, "", 0),
	}

	err := manager.detachIP(context.Background(), "188.121.124.172", []airsIPAttachment{
		{Address: "37.152.185.80", PortID: "new-port"},
		{Address: "37.152.190.18", PortID: "fixed-port"},
	})
	if err != nil {
		t.Fatalf("detachIP() error = %v, want nil for already detached old public_host", err)
	}
}

func TestDetachIPRefusesFixedPublicIP(t *testing.T) {
	t.Parallel()

	manager := &airsManager{
		cfg: airsRuntimeConfig{
			FixedPublicIP: "37.152.190.18",
		},
		logger: log.New(io.Discard, "", 0),
	}

	err := manager.detachIP(context.Background(), "37.152.190.18", []airsIPAttachment{
		{Address: "37.152.190.18", PortID: "fixed-port"},
	})
	if err == nil {
		t.Fatal("detachIP() error = nil, want fixed_public_ip refusal")
	}
}

func TestUpdateClientPublicHostRewritesManagedHosts(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.client.json")
	config := []byte(`{
  "public_host": "188.121.124.172",
  "public_port": 28080,
  "routes": [
    {"id": "inherits"},
    {"id": "old", "public_host": "188.121.124.172"},
    {"id": "custom", "public_host": "203.0.113.10"}
  ]
}`)
	if err := os.WriteFile(path, config, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := updateClientPublicHost(path, "188.121.124.172", "37.152.185.80"); err != nil {
		t.Fatalf("updateClientPublicHost() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var got struct {
		PublicHost string `json:"public_host"`
		Routes     []struct {
			ID         string `json:"id"`
			PublicHost string `json:"public_host"`
		} `json:"routes"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode config: %v", err)
	}

	if got.PublicHost != "37.152.185.80" {
		t.Fatalf("public_host = %q, want new IP", got.PublicHost)
	}
	if got.Routes[0].PublicHost != "37.152.185.80" {
		t.Fatalf("inherited route public_host = %q, want new IP", got.Routes[0].PublicHost)
	}
	if got.Routes[1].PublicHost != "37.152.185.80" {
		t.Fatalf("old route public_host = %q, want new IP", got.Routes[1].PublicHost)
	}
	if got.Routes[2].PublicHost != "203.0.113.10" {
		t.Fatalf("custom route public_host = %q, want unchanged", got.Routes[2].PublicHost)
	}
}
