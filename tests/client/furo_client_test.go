package main

import (
	"bytes"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientWriteReadFrameRoundTrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	payload := []byte("hello")
	if err := writeFrame(&buf, frameData, 42, payload); err != nil {
		t.Fatalf("writeFrame() error = %v", err)
	}

	got, err := readFrame(&buf)
	if err != nil {
		t.Fatalf("readFrame() error = %v", err)
	}

	if got.typ != frameData || got.streamID != 42 || !bytes.Equal(got.payload, payload) {
		t.Fatalf("unexpected frame = %#v", got)
	}
}

func TestClientEncodeOpenPayload(t *testing.T) {
	t.Parallel()

	payload := encodeOpenPayload("example.com", 443)
	hostLen := int(binary.BigEndian.Uint16(payload[0:2]))
	if got := string(payload[2 : 2+hostLen]); got != "example.com" {
		t.Fatalf("encoded host = %q, want %q", got, "example.com")
	}
	if got := binary.BigEndian.Uint16(payload[2+hostLen:]); got != 443 {
		t.Fatalf("encoded port = %d, want 443", got)
	}
}

func TestComputeReconnectDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		failures int
		want     time.Duration
	}{
		{failures: 0, want: 0},
		{failures: 1, want: 1 * time.Second},
		{failures: 2, want: 2 * time.Second},
		{failures: 3, want: 4 * time.Second},
		{failures: 4, want: 8 * time.Second},
		{failures: 5, want: 16 * time.Second},
		{failures: 6, want: 30 * time.Second},
		{failures: 7, want: 30 * time.Second},
	}

	for _, tc := range tests {
		if got := computeReconnectDelay(tc.failures); got != tc.want {
			t.Fatalf("computeReconnectDelay(%d) = %s, want %s", tc.failures, got, tc.want)
		}
	}
}

func TestBuildClientStatusAggregatesSessions(t *testing.T) {
	t.Parallel()

	relayURL = "https://relay.example/furo-relay.php"
	socksListen = "127.0.0.1:18713"
	agentListen = "0.0.0.0:28080"
	adminListen = "127.0.0.1:19080"
	sessionCount = 2
	clientStartedAt = time.Now().Add(-5 * time.Second)
	atomic.StoreUint64(&relayRequestsStarted, 4)
	atomic.StoreUint64(&relayRequestsSucceeded, 3)
	atomic.StoreUint64(&relayRequestsFailed, 1)
	atomic.StoreUint64(&relayRequestsRejected, 0)

	pool := newSessionPool(2)

	first := pool.order[0]
	first.mu.Lock()
	first.conn = &stubConn{}
	first.ready = true
	first.requestFailures = 1
	first.retryDelay = 2 * time.Second
	first.nextRetryAt = time.Now().Add(2 * time.Second)
	first.lastRequestErr = "relay rejected"
	first.streams[1] = newMuxConn(first, 1)
	first.mu.Unlock()
	atomic.StoreUint64(&first.framesIn, 11)
	atomic.StoreUint64(&first.framesOut, 9)
	atomic.StoreUint64(&first.bytesIn, 110)
	atomic.StoreUint64(&first.bytesOut, 90)

	second := pool.order[1]
	second.mu.Lock()
	second.requestFailures = 2
	second.retryDelay = 4 * time.Second
	second.nextRetryAt = time.Now().Add(4 * time.Second)
	second.lastRequestErr = "dial tcp: timeout"
	second.mu.Unlock()

	status := buildClientStatus(pool)

	if status.Totals.ConnectedSessions != 1 {
		t.Fatalf("connected sessions = %d, want 1", status.Totals.ConnectedSessions)
	}
	if status.Totals.ReadySessions != 1 {
		t.Fatalf("ready sessions = %d, want 1", status.Totals.ReadySessions)
	}
	if status.Totals.ActiveStreams != 1 {
		t.Fatalf("active streams = %d, want 1", status.Totals.ActiveStreams)
	}
	if status.RelayRequests.Started != 4 || status.RelayRequests.Succeeded != 3 || status.RelayRequests.Failed != 1 {
		t.Fatalf("unexpected relay request stats = %#v", status.RelayRequests)
	}
	if len(status.Sessions) != 2 {
		t.Fatalf("session count = %d, want 2", len(status.Sessions))
	}
	if status.Sessions[0].LastRequestErr != "relay rejected" {
		t.Fatalf("first session last error = %q", status.Sessions[0].LastRequestErr)
	}
	if status.Sessions[1].RetryDelayMs != 4000 {
		t.Fatalf("second session retry delay = %d, want 4000", status.Sessions[1].RetryDelayMs)
	}
}

type stubConn struct{}

func (*stubConn) Read([]byte) (int, error)         { return 0, nil }
func (*stubConn) Write(b []byte) (int, error)      { return len(b), nil }
func (*stubConn) Close() error                     { return nil }
func (*stubConn) LocalAddr() net.Addr              { return nil }
func (*stubConn) RemoteAddr() net.Addr             { return nil }
func (*stubConn) SetDeadline(time.Time) error      { return nil }
func (*stubConn) SetReadDeadline(time.Time) error  { return nil }
func (*stubConn) SetWriteDeadline(time.Time) error { return nil }
