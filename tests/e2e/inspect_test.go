package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
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
	clientBin := filepath.Join(tmpDir, "furo-client")
	serverBin := filepath.Join(tmpDir, "furo-server")
	testutil.BuildBinary(t, repoRoot, "./inspect.go", inspectBin)
	testutil.BuildBinary(t, repoRoot, "./furo-client.go", clientBin)
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
	clientAdminAddr := testutil.FreeAddr(t)
	clientSocksAddr := testutil.FreeAddr(t)
	phpAddr := testutil.FreeAddr(t)

	serverConfigPath := filepath.Join(tmpDir, "config.server.json")
	clientConfigPath := filepath.Join(tmpDir, "config.client.json")

	writeServerConfigWithMaxSessions(t, serverConfigPath, serverAgentAddr, serverAdminAddr, 2)
	writeMultiRouteClientConfig(t, clientConfigPath, clientSocksAddr, clientAgentAddr, clientAdminAddr, phpAddr, serverAgentAddr)

	serverProc := testutil.StartProcess(t, serverBin, []string{"-c", serverConfigPath}, repoRoot, nil)
	phpProc := testutil.StartProcess(t, php, []string{"-S", phpAddr, "-t", repoRoot}, repoRoot, append(os.Environ(), "PHP_CLI_SERVER_WORKERS=4"))
	clientProc := testutil.StartProcess(t, clientBin, []string{"-c", clientConfigPath}, repoRoot, nil)

	testutil.WaitForHTTP(t, fmt.Sprintf("http://%s/healthz", serverAdminAddr), 10*time.Second, func(resp *http.Response) error {
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
		return nil
	}, serverProc)

	testutil.WaitForHTTP(t, fmt.Sprintf("http://%s/healthz", clientAdminAddr), 20*time.Second, func(resp *http.Response) error {
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status %d", resp.StatusCode)
		}
		return nil
	}, clientProc, serverProc, phpProc)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, inspectBin, "-c", clientConfigPath, "--all", "--speed-test", "--speed-test-url", speedSrv.URL+"/50mb.test")
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("inspect failed: %v\noutput:\n%s\nclient:\n%s\nserver:\n%s\nphp:\n%s", err, output, clientProc.Logs(), serverProc.Logs(), phpProc.Logs())
	}

	stdout := string(output)
	for _, token := range []string{
		"FURO inspect succeeded",
		"Relay callback:",
		"Relay request:",
		"Route: relay_primary",
		"Server handshake:",
		"Ping:",
		"Speed test:",
	} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("inspect output missing %q\noutput:\n%s", token, stdout)
		}
	}
	if !strings.Contains(stdout, "primary "+clientAgentAddr+" is busy; using temporary listener") {
		t.Fatalf("inspect output did not report temporary listener fallback\noutput:\n%s", stdout)
	}
}

func TestInspectBinaryHelpOutput(t *testing.T) {
	repoRoot := testutil.RepoRoot(t)
	tmpDir := t.TempDir()

	inspectBin := filepath.Join(tmpDir, "inspect")
	testutil.BuildBinary(t, repoRoot, "./inspect.go", inspectBin)

	cmd := exec.Command(inspectBin, "--help")
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("inspect --help failed: %v\noutput:\n%s", err, output)
	}

	stdout := string(output)
	for _, token := range []string{
		"Usage:",
		"--speed-test",
		"--speed-test-url",
		"--route-id",
		"--all",
		"config.client.json",
		"Reports the exact failure stage",
	} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("inspect help output missing %q\noutput:\n%s", token, stdout)
		}
	}
}

func writeMultiRouteClientConfig(t *testing.T, path, socksAddr, agentAddr, adminAddr, phpAddr, serverAgentAddr string) {
	t.Helper()

	serverHost, serverPort, err := net.SplitHostPort(serverAgentAddr)
	if err != nil {
		t.Fatalf("split server agent addr: %v", err)
	}
	clientHost, clientPort, err := net.SplitHostPort(agentAddr)
	if err != nil {
		t.Fatalf("split client agent addr: %v", err)
	}

	payload := fmt.Sprintf("{\n  \"client_id\": %q,\n  \"route_selection\": %q,\n  \"api_key\": %q,\n  \"socks_listen\": %q,\n  \"agent_listen\": %q,\n  \"public_host\": %q,\n  \"public_port\": %s,\n  \"admin_listen\": %q,\n  \"open_timeout\": \"10s\",\n  \"keepalive\": \"5s\",\n  \"log_file\": \"\",\n  \"routes\": [\n    {\n      \"id\": %q,\n      \"relay_url\": %q,\n      \"server_host\": %q,\n      \"server_port\": %s,\n      \"session_count\": 1,\n      \"enabled\": true\n    },\n    {\n      \"id\": %q,\n      \"relay_url\": %q,\n      \"server_host\": %q,\n      \"server_port\": %s,\n      \"session_count\": 1,\n      \"enabled\": false\n    }\n  ]\n}\n",
		"client-e2e",
		"least_load",
		testutil.DefaultAPIKey,
		socksAddr,
		agentAddr,
		clientHost,
		clientPort,
		adminAddr,
		"relay_primary",
		"http://"+phpAddr+"/furo-relay.php",
		serverHost,
		serverPort,
		"relay_disabled",
		"http://"+phpAddr+"/unused.php",
		serverHost,
		serverPort,
	)
	if err := os.WriteFile(path, []byte(payload), 0644); err != nil {
		t.Fatalf("write multi-route client config: %v", err)
	}
}
