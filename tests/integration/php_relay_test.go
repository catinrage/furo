package integration

import (
	"fmt"
	"net/http"
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
