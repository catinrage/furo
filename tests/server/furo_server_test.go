package main

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestServerWriteReadFrameRoundTrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	payload := []byte("server")
	if err := writeFrame(&buf, frameOpenOK, 7, payload); err != nil {
		t.Fatalf("writeFrame() error = %v", err)
	}

	got, err := readFrame(&buf)
	if err != nil {
		t.Fatalf("readFrame() error = %v", err)
	}

	if got.typ != frameOpenOK || got.streamID != 7 || !bytes.Equal(got.payload, payload) {
		t.Fatalf("unexpected frame = %#v", got)
	}
}

func TestServerReadFrameRejectsOversizedPayload(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	header := make([]byte, frameHeaderSize)
	header[0] = frameData
	binary.BigEndian.PutUint32(header[5:9], uint32(maxFramePayload+1))
	if _, err := buf.Write(header); err != nil {
		t.Fatalf("write header: %v", err)
	}

	if _, err := readFrame(&buf); err == nil {
		t.Fatal("readFrame() error = nil, want oversized frame error")
	}
}

func TestServerDecodeOpenPayloadRejectsMalformed(t *testing.T) {
	t.Parallel()

	if _, _, err := decodeOpenPayload([]byte{0, 3, 'a'}); err == nil {
		t.Fatal("decodeOpenPayload() error = nil, want malformed payload error")
	}
}

func TestServerAdaptiveFramePayloadHonorsPendingBytes(t *testing.T) {
	originalMin := minFramePayload
	originalMid := midFramePayload
	originalMax := maxFramePayload
	t.Cleanup(func() {
		minFramePayload = originalMin
		midFramePayload = originalMid
		maxFramePayload = originalMax
	})

	minFramePayload = 8 * 1024
	midFramePayload = 16 * 1024
	maxFramePayload = 64 * 1024

	if got := adaptiveFramePayload(1, 0); got != maxFramePayload {
		t.Fatalf("idle chunk = %d, want %d", got, maxFramePayload)
	}
	if got := adaptiveFramePayload(2, 0); got != midFramePayload {
		t.Fatalf("multi-stream chunk = %d, want %d", got, midFramePayload)
	}
	if got := adaptiveFramePayload(1, 512*1024); got != minFramePayload {
		t.Fatalf("backlogged chunk = %d, want %d", got, minFramePayload)
	}
}

func TestServerEnqueueDataHonorsPendingByteLimit(t *testing.T) {
	originalLimit := maxPendingBytes
	t.Cleanup(func() { maxPendingBytes = originalLimit })
	maxPendingBytes = 8

	session := &Session{
		conn:       &serverStubConn{},
		dataQueues: make(map[uint32][]*outboundFrame),
	}

	first := &outboundFrame{streamID: 1, payload: []byte("12345"), result: make(chan error, 1)}
	if err := session.enqueueData(first); err != nil {
		t.Fatalf("first enqueueData() error = %v", err)
	}

	second := &outboundFrame{streamID: 1, payload: []byte("6789"), result: make(chan error, 1)}
	if err := session.enqueueData(second); err == nil {
		t.Fatal("second enqueueData() error = nil, want pending byte limit error")
	}
	if pending := atomic.LoadInt64(&session.pendingBytes); pending != 5 {
		t.Fatalf("pendingBytes = %d, want 5", pending)
	}

	popped := session.popDataFrame()
	if popped != first {
		t.Fatal("popDataFrame() did not return the first enqueued frame")
	}
	if pending := atomic.LoadInt64(&session.pendingBytes); pending != 0 {
		t.Fatalf("pendingBytes after pop = %d, want 0", pending)
	}
}

func TestServerEgressConfigDefaultsDisabled(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.server.json")
	payload := `{
  "api_key": "secret",
  "agent_listen": "127.0.0.1:0",
  "dial_timeout": "1s",
  "keepalive": "1s",
  "write_timeout": "1s",
  "frame_min_size": 4096,
  "frame_mid_size": 8192,
  "frame_max_size": 16384,
  "max_pending_bytes": 16384,
  "max_sessions": 1,
  "log_file": ""
}`
	if err := os.WriteFile(configPath, []byte(payload), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := loadServerConfig(configPath); err != nil {
		t.Fatalf("loadServerConfig() error = %v", err)
	}
	if egressSettings.Enabled {
		t.Fatalf("egress enabled = true, want default disabled")
	}
}

func TestServerTargetDialerUsesEgressBindIP(t *testing.T) {
	originalEgress := egressSettings
	originalDialTimeout := dialTimeout
	originalKeepalive := keepalivePeriod
	t.Cleanup(func() {
		egressSettings = originalEgress
		dialTimeout = originalDialTimeout
		keepalivePeriod = originalKeepalive
	})
	dialTimeout = time.Second
	keepalivePeriod = time.Second
	egressSettings = serverEgressConfig{Enabled: true, BindIP: "10.66.0.2"}

	dialer := targetDialer()
	addr, ok := dialer.LocalAddr.(*net.TCPAddr)
	if !ok || !addr.IP.Equal(net.ParseIP("10.66.0.2")) {
		t.Fatalf("LocalAddr = %#v, want bind IP 10.66.0.2", dialer.LocalAddr)
	}
}

func TestServerSessionRegistrySnapshotsSorted(t *testing.T) {
	t.Parallel()

	registry := newServerSessionRegistry()

	second := &Session{sid: "sess_2", streams: map[uint32]*TargetStream{}}
	first := &Session{sid: "sess_1", streams: map[uint32]*TargetStream{}}
	atomic.StoreUint64(&first.framesIn, 2)
	atomic.StoreUint64(&second.framesOut, 5)

	registry.add(second)
	registry.add(first)

	snapshots := registry.snapshots()
	if len(snapshots) != 2 {
		t.Fatalf("snapshot count = %d, want 2", len(snapshots))
	}
	if snapshots[0].SessionID != "sess_1" || snapshots[1].SessionID != "sess_2" {
		t.Fatalf("snapshot order = %#v, want sorted by session id", snapshots)
	}
	if snapshots[0].FramesIn != 2 || snapshots[1].FramesOut != 5 {
		t.Fatalf("unexpected snapshot counters = %#v", snapshots)
	}
}

func TestBuildServerStatusIncludesCounters(t *testing.T) {
	originalStartedAt := serverStartedAt
	originalAgentListen := agentListen
	originalAdminListen := adminListen
	originalDialTimeout := dialTimeout
	originalMaxSessions := maxSessions
	originalActiveSessions := activeSessions.Load()
	originalAccepted := atomic.LoadUint64(&acceptedSessions)
	originalRejected := atomic.LoadUint64(&rejectedSessions)
	originalClosed := atomic.LoadUint64(&closedSessions)
	serverStartedAt = time.Now().Add(-10 * time.Second)
	agentListen = "0.0.0.0:28081"
	adminListen = "127.0.0.1:19081"
	dialTimeout = 10 * time.Second
	maxSessions = 8
	activeSessions.Store(2)
	atomic.StoreUint64(&acceptedSessions, 6)
	atomic.StoreUint64(&rejectedSessions, 1)
	atomic.StoreUint64(&closedSessions, 4)

	originalRegistry := serverRegistry
	serverRegistry = newServerSessionRegistry()
	t.Cleanup(func() {
		serverStartedAt = originalStartedAt
		agentListen = originalAgentListen
		adminListen = originalAdminListen
		dialTimeout = originalDialTimeout
		maxSessions = originalMaxSessions
		activeSessions.Store(originalActiveSessions)
		atomic.StoreUint64(&acceptedSessions, originalAccepted)
		atomic.StoreUint64(&rejectedSessions, originalRejected)
		atomic.StoreUint64(&closedSessions, originalClosed)
		serverRegistry = originalRegistry
	})

	serverRegistry.add(&Session{sid: "sess_1", streams: map[uint32]*TargetStream{}})
	serverRegistry.add(&Session{sid: "sess_2", streams: map[uint32]*TargetStream{}})

	status := buildServerStatus()

	if status.ActiveSessions != 2 {
		t.Fatalf("active sessions = %d, want 2", status.ActiveSessions)
	}
	if status.AcceptedSessions != 6 || status.RejectedSessions != 1 || status.ClosedSessions != 4 {
		t.Fatalf("unexpected lifecycle counters = %#v", status)
	}
	if len(status.Sessions) != 2 {
		t.Fatalf("session count = %d, want 2", len(status.Sessions))
	}
}

func TestReserveSessionSlotHonorsLimit(t *testing.T) {
	originalMaxSessions := maxSessions
	originalActive := activeSessions.Load()
	t.Cleanup(func() {
		maxSessions = originalMaxSessions
		activeSessions.Store(originalActive)
	})

	maxSessions = 2
	activeSessions.Store(0)

	if !reserveSessionSlot() {
		t.Fatal("first reserveSessionSlot() = false, want true")
	}
	if !reserveSessionSlot() {
		t.Fatal("second reserveSessionSlot() = false, want true")
	}
	if reserveSessionSlot() {
		t.Fatal("third reserveSessionSlot() = true, want false")
	}
}

type serverStubConn struct{}

func (*serverStubConn) Read([]byte) (int, error)         { return 0, nil }
func (*serverStubConn) Write(b []byte) (int, error)      { return len(b), nil }
func (*serverStubConn) Close() error                     { return nil }
func (*serverStubConn) LocalAddr() net.Addr              { return nil }
func (*serverStubConn) RemoteAddr() net.Addr             { return nil }
func (*serverStubConn) SetDeadline(time.Time) error      { return nil }
func (*serverStubConn) SetReadDeadline(time.Time) error  { return nil }
func (*serverStubConn) SetWriteDeadline(time.Time) error { return nil }
