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
