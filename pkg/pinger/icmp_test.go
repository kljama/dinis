package pinger

import (
	"context"
	"fmt"
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

func TestSingleProberConcurrentLocalhost(t *testing.T) {
	prober := NewSingleProber()
	defer prober.Close()

	const count = 100
	errChan := make(chan error, count)

	for i := 0; i < count; i++ {
		go func(idx int) {
			res := prober.Probe(context.Background(), "127.0.0.1", 1*time.Second)
			if !res.Success {
				errChan <- fmt.Errorf("probe #%d failed: %s", idx, res.Error)
				return
			}
			errChan <- nil
		}(i)
	}

	var failures []error
	for i := 0; i < count; i++ {
		if err := <-errChan; err != nil {
			failures = append(failures, err)
		}
	}

	if len(failures) > 0 {
		t.Fatalf("%d out of %d concurrent probes failed. First error: %v", len(failures), count, failures[0])
	}
}

func TestSingleProberContextCancellation(t *testing.T) {
	prober := NewSingleProber()
	defer prober.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context

	start := time.Now()
	res := prober.Probe(ctx, "127.0.0.1", 5*time.Second)
	elapsed := time.Since(start)

	if res.Success {
		t.Errorf("expected cancelled probe to fail, but got success")
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("expected immediate cancellation, took %v", elapsed)
	}
}

