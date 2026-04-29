package main

import (
	"bytes"
	"encoding/binary"
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
	t.Parallel()

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
	t.Parallel()

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
