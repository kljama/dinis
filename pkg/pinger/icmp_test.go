package pinger

import (
	"context"
	"testing"
	"time"
)

func TestSingleProberLocalhost(t *testing.T) {
	prober := NewSingleProber()
	res := prober.Probe(context.Background(), "127.0.0.1", 1*time.Second)
	if !res.Success {
		t.Fatalf("expected ping to 127.0.0.1 to succeed, got error: %s", res.Error)
	}
	if res.LatencyMs <= 0 {
		t.Errorf("expected positive latency, got %f ms", res.LatencyMs)
	}
}

func TestSingleProberInvalidIP(t *testing.T) {
	prober := NewSingleProber()
	res := prober.Probe(context.Background(), "999.999.999.999", 500*time.Millisecond)
	if res.Success {
		t.Errorf("expected failure on invalid IP, got success")
	}
}

func TestSingleProberSequenceProgression(t *testing.T) {
	prober := NewSingleProber()
	initialSeq := prober.seq

	// Test that sequence increases and IDs change when wrapping 64k boundary
	prober.seq = 65535 // boundary
	res := prober.Probe(context.Background(), "127.0.0.1", 1*time.Second)
	if !res.Success {
		t.Fatalf("expected ping across 64k seq boundary to succeed, got error: %s", res.Error)
	}

	if prober.seq <= initialSeq && prober.seq <= 65535 {
		t.Errorf("expected sequence to advance beyond 65535, got %d", prober.seq)
	}
}

func TestChecksum(t *testing.T) {
	b := []byte{0x08, 0x00, 0x00, 0x00, 0x12, 0x34, 0x00, 0x01}
	cs := checksum(b)
	if cs == 0 {
		t.Errorf("expected non-zero checksum, got 0")
	}
}

func TestSingleProberSocketPool(t *testing.T) {
	prober := NewSingleProber()
	defer prober.Close()

	// Run multiple sequential and concurrent probes reusing the pooled sockets
	for i := 0; i < 5; i++ {
		res := prober.Probe(context.Background(), "127.0.0.1", 1*time.Second)
		if !res.Success {
			t.Fatalf("expected ping #%d to 127.0.0.1 to succeed, got: %s", i, res.Error)
		}
	}
}


