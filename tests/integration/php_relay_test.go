package integration

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"furo/tests/testutil"
)

func TestPHPRelayAuthAndValidation(t *testing.T) {
	php := testutil.RequirePHP(t)
	repoRoot := testutil.RepoRoot(t)
	addr := testutil.FreeAddr(t)

	phpServer := testutil.StartProcess(t, php, []string{"-S", addr, "-t", repoRoot}, repoRoot, nil)
	relayURL := fmt.Sprintf("http://%s/furo-relay.php", addr)

	testutil.WaitForHTTP(t, relayURL, 10*time.Second, func(resp *http.Response) error {
		if resp.StatusCode != http.StatusForbidden {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
		return nil
	}, phpServer)

	req, err := http.NewRequest(http.MethodGet, relayURL+"?action=invalid", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-API-KEY", testutil.DefaultAPIKey)

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request relay with auth: %v\n%s", err, phpServer.Logs())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status %d for invalid action\n%s", resp.StatusCode, phpServer.Logs())
	}
}

func TestPHPRelayRouteMapReadWrite(t *testing.T) {
	php := testutil.RequirePHP(t)
	repoRoot := testutil.RepoRoot(t)
	routeMapPath := filepath.Join(repoRoot, "furo-route-map.json")
	_ = os.Remove(routeMapPath)
	t.Cleanup(func() { _ = os.Remove(routeMapPath) })

	addr := testutil.FreeAddr(t)
	phpServer := testutil.StartProcess(t, php, []string{"-S", addr, "-t", repoRoot}, repoRoot, nil)
	relayURL := fmt.Sprintf("http://%s/furo-relay.php?action=route-map", addr)

	testutil.WaitForHTTP(t, relayURL, 10*time.Second, func(resp *http.Response) error {
		if resp.StatusCode != http.StatusForbidden {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
		return nil
	}, phpServer)

	payload := `{"fleet_id":"fleet-a","generation":4,"active":{"id":"active-a","relay_url":"http://relay/furo.php","server_host":"203.0.113.10","server_port":8443,"session_count":2},"standby":[],"retired":[]}`
	req, err := http.NewRequest(http.MethodPut, relayURL, bytes.NewBufferString(payload))
	if err != nil {
		t.Fatalf("new write request: %v", err)
	}
	req.Header.Set("X-API-KEY", testutil.DefaultAPIKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("write route map: %v\n%s", err, phpServer.Logs())
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write status = %d\n%s", resp.StatusCode, phpServer.Logs())
	}

	req, err = http.NewRequest(http.MethodGet, relayURL, nil)
	if err != nil {
		t.Fatalf("new read request: %v", err)
	}
	req.Header.Set("X-API-KEY", testutil.DefaultAPIKey)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("read route map: %v\n%s", err, phpServer.Logs())
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"fleet_id": "fleet-a"`) {
		t.Fatalf("read status=%d body=%s\n%s", resp.StatusCode, strings.TrimSpace(string(body)), phpServer.Logs())
	}
}
