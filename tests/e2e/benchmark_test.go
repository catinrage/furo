package e2e

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"furo/tests/testutil"
	"golang.org/x/net/proxy"
)

func BenchmarkFullStackTunnelThroughput(b *testing.B) {
	repoRoot := repoRootForBenchmark(b)
	php := phpForBenchmark(b)
	tmpDir := b.TempDir()

	clientBin := filepath.Join(tmpDir, "furo-client")
	serverBin := filepath.Join(tmpDir, "furo-server")
	buildBinaryForBenchmark(b, repoRoot, "./furo-client.go", clientBin)
	buildBinaryForBenchmark(b, repoRoot, "./furo-server.go", serverBin)

	echoAddr := freeAddrForBenchmark(b)
	serverAgentAddr := freeAddrForBenchmark(b)
	serverAdminAddr := freeAddrForBenchmark(b)
	clientAgentAddr := freeAddrForBenchmark(b)
	clientAdminAddr := freeAddrForBenchmark(b)
	clientSocksAddr := freeAddrForBenchmark(b)
	phpAddr := freeAddrForBenchmark(b)

	runEchoServerForBenchmark(b, echoAddr)

	serverConfigPath := filepath.Join(tmpDir, "config.server.json")
	clientConfigPath := filepath.Join(tmpDir, "config.client.json")
	writeServerConfigForBenchmark(b, serverConfigPath, serverAgentAddr, serverAdminAddr)
	writeClientConfigForBenchmark(b, clientConfigPath, clientSocksAddr, clientAgentAddr, clientAdminAddr, phpAddr, serverAgentAddr)

	serverProc := startBenchmarkProcess(b, serverBin, []string{"-c", serverConfigPath}, repoRoot, nil)
	phpProc := startBenchmarkProcess(b, php, []string{"-S", phpAddr, "-t", repoRoot}, repoRoot, append(os.Environ(), "PHP_CLI_SERVER_WORKERS=4"))
	clientProc := startBenchmarkProcess(b, clientBin, []string{"-c", clientConfigPath}, repoRoot, nil)

	waitHTTPForBenchmark(b, "http://"+serverAdminAddr+"/healthz", serverProc)
	waitHTTPForBenchmark(b, "http://"+clientAdminAddr+"/healthz", clientProc, serverProc, phpProc)

	dialer, err := proxy.SOCKS5("tcp", clientSocksAddr, nil, proxy.Direct)
	if err != nil {
		b.Fatalf("create socks5 dialer: %v", err)
	}

	payload := make([]byte, 1024*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	reply := make([]byte, len(payload))

	b.SetBytes(int64(len(payload) * 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := dialer.Dial("tcp", echoAddr)
		if err != nil {
			b.Fatalf("dial echo through socks: %v", err)
		}
		if _, err := conn.Write(payload); err != nil {
			_ = conn.Close()
			b.Fatalf("write payload: %v", err)
		}
		if _, err := io.ReadFull(conn, reply); err != nil {
			_ = conn.Close()
			b.Fatalf("read reply: %v", err)
		}
		_ = conn.Close()
	}
}

func buildBinaryForBenchmark(b *testing.B, workdir, source, output string) {
	b.Helper()
	goTool := goToolForBenchmark(b)
	cmd := exec.Command(goTool, "build", "-o", output, source)
	cmd.Dir = workdir
	out, err := cmd.CombinedOutput()
	if err != nil {
		b.Fatalf("go build %s failed: %v\n%s", source, err, out)
	}
}

func repoRootForBenchmark(b *testing.B) string {
	b.Helper()
	wd, err := os.Getwd()
	if err != nil {
		b.Fatalf("get working directory: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func goToolForBenchmark(b *testing.B) string {
	b.Helper()
	if _, err := os.Stat("/usr/local/go/bin/go"); err == nil {
		return "/usr/local/go/bin/go"
	}
	path, err := exec.LookPath("go")
	if err != nil {
		b.Fatalf("go tool not found: %v", err)
	}
	return path
}

func phpForBenchmark(b *testing.B) string {
	b.Helper()
	path, err := exec.LookPath("php")
	if err != nil {
		b.Fatalf("php not found in PATH: %v", err)
	}
	cmd := exec.Command(path, "-m")
	output, err := cmd.CombinedOutput()
	if err != nil {
		b.Fatalf("php -m failed: %v\n%s", err, output)
	}
	if !bytes.Contains(output, []byte("sockets")) {
		b.Fatal("php sockets extension is required for benchmark")
	}
	return path
}

func startBenchmarkProcess(b *testing.B, name string, args []string, workdir string, env []string) *testutil.ManagedProcess {
	b.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = workdir
	if env != nil {
		cmd.Env = env
	}
	proc := &testutil.ManagedProcess{Name: filepath.Base(name), Cmd: cmd}
	cmd.Stdout = &proc.Stdout
	cmd.Stderr = &proc.Stderr
	if err := cmd.Start(); err != nil {
		b.Fatalf("start %s: %v", name, err)
	}
	b.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	return proc
}

func freeAddrForBenchmark(b *testing.B) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("allocate address: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func runEchoServerForBenchmark(b *testing.B, addr string) {
	b.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		b.Fatalf("listen echo server: %v", err)
	}
	b.Cleanup(func() { _ = ln.Close() })
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

func writeServerConfigForBenchmark(b *testing.B, path, agentAddr, adminAddr string) {
	b.Helper()
	payload := fmt.Sprintf("{\n  \"api_key\": %q,\n  \"agent_listen\": %q,\n  \"admin_listen\": %q,\n  \"dial_timeout\": \"5s\",\n  \"keepalive\": \"5s\",\n  \"write_timeout\": \"10s\",\n  \"max_sessions\": 2,\n  \"log_file\": \"\"\n}\n", testutil.DefaultAPIKey, agentAddr, adminAddr)
	if err := os.WriteFile(path, []byte(payload), 0644); err != nil {
		b.Fatalf("write server config: %v", err)
	}
}

func writeClientConfigForBenchmark(b *testing.B, path, socksAddr, agentAddr, adminAddr, phpAddr, serverAgentAddr string) {
	b.Helper()
	serverHost, serverPort, err := net.SplitHostPort(serverAgentAddr)
	if err != nil {
		b.Fatalf("split server addr: %v", err)
	}
	clientHost, clientPort, err := net.SplitHostPort(agentAddr)
	if err != nil {
		b.Fatalf("split client addr: %v", err)
	}
	payload := fmt.Sprintf("{\n  \"relay_url\": %q,\n  \"api_key\": %q,\n  \"socks_listen\": %q,\n  \"agent_listen\": %q,\n  \"public_host\": %q,\n  \"public_port\": %s,\n  \"server_host\": %q,\n  \"server_port\": %s,\n  \"admin_listen\": %q,\n  \"open_timeout\": \"10s\",\n  \"keepalive\": \"5s\",\n  \"write_timeout\": \"10s\",\n  \"session_count\": 1,\n  \"log_file\": \"\"\n}\n", "http://"+phpAddr+"/furo-relay.php", testutil.DefaultAPIKey, socksAddr, agentAddr, clientHost, clientPort, serverHost, serverPort, adminAddr)
	if err := os.WriteFile(path, []byte(payload), 0644); err != nil {
		b.Fatalf("write client config: %v", err)
	}
}

func waitHTTPForBenchmark(b *testing.B, url string, logs ...*testutil.ManagedProcess) {
	b.Helper()
	deadline := time.Now().Add(20 * time.Second)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, proc := range logs {
		b.Logf("%s logs:\n%s", proc.Name, proc.Logs())
	}
	b.Fatalf("timed out waiting for %s", url)
}
