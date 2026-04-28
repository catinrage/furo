package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"furo/tests/testutil"
	"golang.org/x/net/proxy"
)

func TestFullStackTunnelViaPHPRelay(t *testing.T) {
	repoRoot := testutil.RepoRoot(t)
	php := testutil.RequirePHP(t)
	tmpDir := t.TempDir()

	clientBin := filepath.Join(tmpDir, "furo-client")
	serverBin := filepath.Join(tmpDir, "furo-server")
	testutil.BuildBinary(t, repoRoot, "./furo-client.go", clientBin)
	testutil.BuildBinary(t, repoRoot, "./furo-server.go", serverBin)

	echoAddr := testutil.FreeAddr(t)
	serverAgentAddr := testutil.FreeAddr(t)
	serverAdminAddr := testutil.FreeAddr(t)
	clientAgentAddr := testutil.FreeAddr(t)
	clientAdminAddr := testutil.FreeAddr(t)
	clientSocksAddr := testutil.FreeAddr(t)
	phpAddr := testutil.FreeAddr(t)

	runEchoServer(t, echoAddr)

	serverConfigPath := filepath.Join(tmpDir, "config.server.json")
	clientConfigPath := filepath.Join(tmpDir, "config.client.json")

	writeServerConfig(t, serverConfigPath, serverAgentAddr, serverAdminAddr)
	writeClientConfig(t, clientConfigPath, clientSocksAddr, clientAgentAddr, clientAdminAddr, phpAddr, serverAgentAddr)

	serverProc := testutil.StartProcess(t, serverBin, []string{"-c", serverConfigPath}, repoRoot, nil)
	phpProc := testutil.StartProcess(t, php, []string{"-S", phpAddr, "-t", repoRoot}, repoRoot, nil)
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

	dialer, err := proxy.SOCKS5("tcp", clientSocksAddr, nil, proxy.Direct)
	if err != nil {
		t.Fatalf("create socks5 dialer: %v", err)
	}

	conn, err := dialer.Dial("tcp", echoAddr)
	if err != nil {
		t.Fatalf("dial echo through socks: %v\nclient:\n%s\nserver:\n%s\nphp:\n%s", err, clientProc.Logs(), serverProc.Logs(), phpProc.Logs())
	}
	defer conn.Close()

	payload := []byte("furo-e2e")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write through tunnel: %v", err)
	}

	reply := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatalf("read echoed payload: %v", err)
	}
	if string(reply) != string(payload) {
		t.Fatalf("echo mismatch: got %q want %q", reply, payload)
	}

	clientStatus := fetchClientStatus(t, clientAdminAddr, clientProc, serverProc, phpProc)
	if clientStatus.Totals.ReadySessions < 1 {
		t.Fatalf("expected ready sessions in client status: %+v", clientStatus)
	}
	if clientStatus.RelayRequests.Succeeded < 1 {
		t.Fatalf("expected at least one successful relay request: %+v", clientStatus.RelayRequests)
	}

	serverStatus := fetchServerStatus(t, serverAdminAddr, serverProc)
	if serverStatus.ActiveSessions < 1 {
		t.Fatalf("expected active sessions in server status: %+v", serverStatus)
	}
	if serverStatus.AcceptedSessions < 1 {
		t.Fatalf("expected accepted sessions in server status: %+v", serverStatus)
	}
}

func runEchoServer(t *testing.T, addr string) {
	t.Helper()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen echo server: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
}

func writeServerConfig(t *testing.T, path, agentAddr, adminAddr string) {
	t.Helper()
	writeServerConfigWithMaxSessions(t, path, agentAddr, adminAddr, 1)
}

func writeServerConfigWithMaxSessions(t *testing.T, path, agentAddr, adminAddr string, maxSessions int) {
	t.Helper()

	payload := fmt.Sprintf("{\n  \"api_key\": %q,\n  \"agent_listen\": %q,\n  \"admin_listen\": %q,\n  \"dial_timeout\": \"5s\",\n  \"keepalive\": \"5s\",\n  \"max_sessions\": %d,\n  \"log_file\": \"\"\n}\n", testutil.DefaultAPIKey, agentAddr, adminAddr, maxSessions)
	if err := os.WriteFile(path, []byte(payload), 0644); err != nil {
		t.Fatalf("write server config: %v", err)
	}
}

func writeClientConfig(t *testing.T, path, socksAddr, agentAddr, adminAddr, phpAddr, serverAgentAddr string) {
	t.Helper()

	serverHost, serverPort, err := net.SplitHostPort(serverAgentAddr)
	if err != nil {
		t.Fatalf("split server agent addr: %v", err)
	}
	clientHost, clientPort, err := net.SplitHostPort(agentAddr)
	if err != nil {
		t.Fatalf("split client agent addr: %v", err)
	}

	payload := fmt.Sprintf("{\n  \"relay_url\": %q,\n  \"api_key\": %q,\n  \"socks_listen\": %q,\n  \"agent_listen\": %q,\n  \"public_host\": %q,\n  \"public_port\": %s,\n  \"server_host\": %q,\n  \"server_port\": %s,\n  \"admin_listen\": %q,\n  \"open_timeout\": \"10s\",\n  \"keepalive\": \"5s\",\n  \"session_count\": 1,\n  \"log_file\": \"\"\n}\n", "http://"+phpAddr+"/furo-relay.php", testutil.DefaultAPIKey, socksAddr, agentAddr, clientHost, clientPort, serverHost, serverPort, adminAddr)
	if err := os.WriteFile(path, []byte(payload), 0644); err != nil {
		t.Fatalf("write client config: %v", err)
	}
}

type clientStatusResponse struct {
	RelayRequests struct {
		Succeeded uint64 `json:"succeeded"`
	} `json:"relay_requests"`
	Totals struct {
		ReadySessions int `json:"ready_sessions"`
	} `json:"totals"`
}

func fetchClientStatus(t *testing.T, adminAddr string, logs ...*testutil.ManagedProcess) clientStatusResponse {
	t.Helper()

	var status clientStatusResponse
	getJSON(t, "http://"+adminAddr+"/status", &status, logs...)
	return status
}

type serverStatusResponse struct {
	ActiveSessions   int32  `json:"active_sessions"`
	AcceptedSessions uint64 `json:"accepted_sessions"`
}

func fetchServerStatus(t *testing.T, adminAddr string, logs ...*testutil.ManagedProcess) serverStatusResponse {
	t.Helper()

	var status serverStatusResponse
	getJSON(t, "http://"+adminAddr+"/status", &status, logs...)
	return status
}

func getJSON(t *testing.T, url string, dst any, logs ...*testutil.ManagedProcess) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request %s: %v", url, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		failWithLogs(t, fmt.Sprintf("request %s failed: %v", url, err), logs...)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		failWithLogs(t, fmt.Sprintf("request %s returned %d", url, resp.StatusCode), logs...)
	}
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		failWithLogs(t, fmt.Sprintf("decode %s failed: %v", url, err), logs...)
	}
}

func failWithLogs(t *testing.T, message string, logs ...*testutil.ManagedProcess) {
	t.Helper()

	var extra string
	for _, proc := range logs {
		extra += "\n" + proc.Name + ":\n" + proc.Logs()
	}
	t.Fatalf("%s%s", message, extra)
}
