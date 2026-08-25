package pinger

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestEngineLifecycle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 200 * time.Millisecond
	cfg.Timeout = 200 * time.Millisecond
	cfg.FailThreshold = 1

	engine := NewEngine(cfg)

	hosts := map[string]*HostState{
		"127.0.0.1": {
			IP:     "127.0.0.1",
			Alias:  "Localhost Loopback",
			CIDR:   "127.0.0.1/32",
			Status: StatusPending,
		},
		"192.0.2.1": {
			IP:         "192.0.2.1",
			Alias:      "Excluded Test Host",
			CIDR:       "192.0.2.0/24",
			Status:     StatusExcluded,
			IsExcluded: true,
		},
	}

	engine.SetHosts(hosts)

	// Test PingSingle
	res := engine.PingSingle(context.Background(), "127.0.0.1")
	if !res.Success {
		t.Fatalf("PingSingle failed: %v", res.Error)
	}

	h, ok := engine.GetHost("127.0.0.1")
	if !ok || h.Status != StatusUp {
		t.Fatalf("expected host 127.0.0.1 to be UP, got status %s", h.Status)
	}
	if h.SentPackets != 1 || h.RecvPackets != 1 {
		t.Errorf("expected 1 sent, 1 recv, got sent=%d, recv=%d", h.SentPackets, h.RecvPackets)
	}

	summary := engine.GetSummary()
	if summary.TotalTargets != 2 {
		t.Errorf("expected 2 total targets, got %d", summary.TotalTargets)
	}
	if summary.UpCount != 1 {
		t.Errorf("expected 1 UP host, got %d", summary.UpCount)
	}
	if summary.ExcludedCount != 1 {
		t.Errorf("expected 1 Excluded host, got %d", summary.ExcludedCount)
	}
}

func TestEnginePacing(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Interval = 1000 * time.Millisecond
	cfg.Timeout = 200 * time.Millisecond

	engine := NewEngine(cfg)

	hosts := make(map[string]*HostState)
	for i := 1; i <= 10; i++ {
		ip := fmt.Sprintf("127.0.0.%d", i)
		hosts[ip] = &HostState{
			IP:     ip,
			CIDR:   "127.0.0.0/24",
			Status: StatusPending,
		}
	}
	engine.SetHosts(hosts)

	summary := engine.GetSummary()
	if summary.PacketsPerSec <= 0 {
		t.Errorf("expected positive PacketsPerSec, got %f", summary.PacketsPerSec)
	}
	if summary.PacedDelayMs <= 0 {
		t.Errorf("expected positive PacedDelayMs, got %f", summary.PacedDelayMs)
	}

	// Verify that 10 hosts over 1000ms (with 200ms timeout reserve -> 800ms window) gives ~80ms pace delay
	expectedPace := 80.0
	if summary.PacedDelayMs < 75.0 || summary.PacedDelayMs > 85.0 {
		t.Errorf("expected ~%f ms pace delay, got %f ms", expectedPace, summary.PacedDelayMs)
	}
}
