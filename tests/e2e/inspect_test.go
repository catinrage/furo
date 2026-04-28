package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"furo/tests/testutil"
)

func TestInspectBinaryReportsRelayHealth(t *testing.T) {
	repoRoot := testutil.RepoRoot(t)
	php := testutil.RequirePHP(t)
	tmpDir := t.TempDir()

	inspectBin := filepath.Join(tmpDir, "inspect")
	serverBin := filepath.Join(tmpDir, "furo-server")
	testutil.BuildBinary(t, repoRoot, "./inspect.go", inspectBin)
	testutil.BuildBinary(t, repoRoot, "./furo-server.go", serverBin)

	speedPayload := strings.Repeat("furo-speed-test-", 16*1024)
	speedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/50mb.test" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(speedPayload)))
		_, _ = w.Write([]byte(speedPayload))
	}))
	defer speedSrv.Close()

	serverAgentAddr := testutil.FreeAddr(t)
	serverAdminAddr := testutil.FreeAddr(t)
	clientAgentAddr := testutil.FreeAddr(t)
	phpAddr := testutil.FreeAddr(t)

	serverConfigPath := filepath.Join(tmpDir, "config.server.json")
	clientConfigPath := filepath.Join(tmpDir, "config.client.json")

	writeServerConfig(t, serverConfigPath, serverAgentAddr, serverAdminAddr)
	writeClientConfig(t, clientConfigPath, "127.0.0.1:0", clientAgentAddr, "", phpAddr, serverAgentAddr)

	serverProc := testutil.StartProcess(t, serverBin, []string{"-c", serverConfigPath}, repoRoot, nil)
	phpProc := testutil.StartProcess(t, php, []string{"-S", phpAddr, "-t", repoRoot}, repoRoot, nil)

	testutil.WaitForHTTP(t, fmt.Sprintf("http://%s/healthz", serverAdminAddr), 10*time.Second, func(resp *http.Response) error {
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
		return nil
	}, serverProc)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, inspectBin, "-c", clientConfigPath, "--speed-test", "--speed-test-url", speedSrv.URL+"/50mb.test")
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("inspect failed: %v\noutput:\n%s\nserver:\n%s\nphp:\n%s", err, output, serverProc.Logs(), phpProc.Logs())
	}

	stdout := string(output)
	for _, token := range []string{
		"FURO inspect succeeded",
		"Relay callback:",
		"Relay request:",
		"Server handshake:",
		"Ping:",
		"Speed test:",
	} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("inspect output missing %q\noutput:\n%s", token, stdout)
		}
	}
}
