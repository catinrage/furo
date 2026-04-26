package testutil

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const DefaultAPIKey = "my_super_secret_123456789"

type ManagedProcess struct {
	Name   string
	Cmd    *exec.Cmd
	Stdout bytes.Buffer
	Stderr bytes.Buffer
}

func RepoRoot(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve caller for repo root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func GoTool(t *testing.T) string {
	t.Helper()

	if _, err := os.Stat("/usr/local/go/bin/go"); err == nil {
		return "/usr/local/go/bin/go"
	}

	path, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("go tool not found: %v", err)
	}
	return path
}

func RequirePHP(t *testing.T) string {
	t.Helper()

	path, err := exec.LookPath("php")
	if err != nil {
		t.Fatalf("php not found in PATH: %v", err)
	}

	cmd := exec.Command(path, "-m")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("php -m failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "sockets") {
		t.Fatal("php sockets extension is required for integration/e2e tests")
	}
	return path
}

func FreeAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free address: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func StartProcess(t *testing.T, name string, args []string, workdir string, env []string) *ManagedProcess {
	t.Helper()

	cmd := exec.Command(name, args...)
	cmd.Dir = workdir
	if env != nil {
		cmd.Env = env
	}

	proc := &ManagedProcess{Name: filepath.Base(name), Cmd: cmd}
	cmd.Stdout = &proc.Stdout
	cmd.Stderr = &proc.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start %s: %v", name, err)
	}

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	return proc
}

func (p *ManagedProcess) Logs() string {
	return fmt.Sprintf("stdout:\n%s\nstderr:\n%s", p.Stdout.String(), p.Stderr.String())
}

func WaitForHTTP(t *testing.T, url string, timeout time.Duration, fn func(*http.Response) error, logs ...*ManagedProcess) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}

	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			err = fn(resp)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if err == nil {
				return
			}
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}

	var extra strings.Builder
	for _, proc := range logs {
		extra.WriteString("\n")
		extra.WriteString(proc.Name)
		extra.WriteString(" logs:\n")
		extra.WriteString(proc.Logs())
	}

	t.Fatalf("timed out waiting for %s: %v%s", url, lastErr, extra.String())
}

func BuildBinary(t *testing.T, workdir, source, output string) {
	t.Helper()

	cmd := exec.Command(GoTool(t), "build", "-o", output, source)
	cmd.Dir = workdir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build %s failed: %v\n%s", source, err, out)
	}
}

func WaitForProcessExit(proc *ManagedProcess, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() {
		done <- proc.Cmd.Wait()
	}()

	select {
	case err := <-done:
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return err
	case <-time.After(timeout):
		return fmt.Errorf("%s did not exit within %s", proc.Name, timeout)
	}
}
