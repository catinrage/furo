package integration

import (
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"furo/tests/testutil"
)

func TestPHPRouteDiagnosticsUnlockAndProbe(t *testing.T) {
	t.Parallel()

	php := testutil.RequirePHP(t)
	repoRoot := testutil.RepoRoot(t)
	phpAddr := testutil.FreeAddr(t)

	clientAddrA := testutil.FreeAddr(t)
	clientAddrB := testutil.FreeAddr(t)
	serverAddrA := testutil.FreeAddr(t)
	serverAddrB := testutil.FreeAddr(t)

	runAcceptCloseServer(t, clientAddrA)
	runAcceptCloseServer(t, clientAddrB)
	runAcceptCloseServer(t, serverAddrA)
	runAcceptCloseServer(t, serverAddrB)

	phpServer := testutil.StartProcess(t, php, []string{"-S", phpAddr, "-t", repoRoot}, repoRoot, append(os.Environ(), "PHP_CLI_SERVER_WORKERS=4"))
	pageURL := "http://" + phpAddr + "/furo-route-diagnostics.php"

	testutil.WaitForHTTP(t, pageURL, 10*time.Second, func(resp *http.Response) error {
		if resp.StatusCode != http.StatusOK {
			return io.ErrUnexpectedEOF
		}
		return nil
	}, phpServer)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookie jar: %v", err)
	}

	client := &http.Client{
		Timeout: 4 * time.Second,
		Jar:     jar,
	}

	body := postForm(t, client, pageURL, url.Values{
		"action":  {"unlock"},
		"passkey": {"wrong-passkey"},
	}, phpServer)
	if !strings.Contains(body, "Invalid passkey.") {
		t.Fatalf("expected invalid passkey response, got:\n%s", body)
	}

	body = postForm(t, client, pageURL, url.Values{
		"action":  {"unlock"},
		"passkey": {"change-this-passkey"},
	}, phpServer)
	if !strings.Contains(body, "Client Config Input") {
		t.Fatalf("expected unlocked diagnostics page, got:\n%s", body)
	}

	configJSON := `{
  "public_host": "127.0.0.1",
  "public_port": ` + splitPort(t, clientAddrA) + `,
  "routes": [
    {
      "id": "relay-primary",
      "relay_url": "http://` + phpAddr + `/furo-relay.php",
      "server_host": "127.0.0.1",
      "server_port": ` + splitPort(t, serverAddrA) + `,
      "session_count": 2,
      "enabled": true
    },
    {
      "id": "relay-backup",
      "relay_url": "http://` + phpAddr + `/furo-relay.php",
      "public_host": "127.0.0.1",
      "public_port": ` + splitPort(t, clientAddrB) + `,
      "server_host": "127.0.0.1",
      "server_port": ` + splitPort(t, serverAddrB) + `,
      "session_count": 1,
      "enabled": false
    }
  ]
}`

	body = postForm(t, client, pageURL, url.Values{
		"action":         {"run"},
		"config_content": {configJSON},
	}, phpServer)

	for _, want := range []string{
		"Routes analyzed",
		"relay-primary",
		"relay-backup",
		"Disabled in config",
		"TCP reachable",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected %q in diagnostics output, got:\n%s", want, body)
		}
	}
}

func runAcceptCloseServer(t *testing.T, addr string) {
	t.Helper()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
}

func splitPort(t *testing.T, addr string) string {
	t.Helper()

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host port %s: %v", addr, err)
	}
	return port
}

func postForm(t *testing.T, client *http.Client, target string, values url.Values, logs ...*testutil.ManagedProcess) string {
	t.Helper()

	resp, err := client.PostForm(target, values)
	if err != nil {
		var extra strings.Builder
		for _, proc := range logs {
			extra.WriteString("\n")
			extra.WriteString(proc.Name)
			extra.WriteString(" logs:\n")
			extra.WriteString(proc.Logs())
		}
		t.Fatalf("post %s failed: %v%s", target, err, extra.String())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return string(body)
}
