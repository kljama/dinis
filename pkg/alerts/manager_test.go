package alerts

import (
	"testing"
)

func TestAlertManagerLifecycle(t *testing.T) {
	mgr := NewManager(50)

	// 1. Trigger alert
	alt := mgr.Trigger("192.168.1.100", "DB Server", "192.168.1.0/24", "Request timeout")
	if alt.State != AlertStateFiring || alt.Acknowledged {
		t.Fatalf("expected FIRING unacknowledged alert, got: %+v", alt)
	}

	active := mgr.GetActiveAlerts()
	if len(active) != 1 {
		t.Fatalf("expected 1 active alert, got %d", len(active))
	}

	// 2. Acknowledge alert
	ackAlt, err := mgr.Acknowledge("192.168.1.100", "Alice", "Investigating power supply")
	if err != nil {
		t.Fatalf("failed to acknowledge: %v", err)
	}
	if !ackAlt.Acknowledged || ackAlt.State != AlertStateAcknowledged || ackAlt.AcknowledgedBy != "Alice" {
		t.Fatalf("unexpected ack state: %+v", ackAlt)
	}

	// 3. Resolve alert
	resolved, ok := mgr.Resolve("192.168.1.100")
	if !ok || resolved.State != AlertStateResolved {
		t.Fatalf("failed to resolve alert")
	}

	active = mgr.GetActiveAlerts()
	if len(active) != 0 {
		t.Fatalf("expected 0 active alerts after resolution, got %d", len(active))
	}

	hist := mgr.GetAlertHistory(10)
	if len(hist) != 1 || hist[0].IP != "192.168.1.100" {
		t.Fatalf("expected 1 resolved history alert, got %+v", hist)
	}
}

func TestAcknowledgeAll(t *testing.T) {
	mgr := NewManager(50)
	mgr.Trigger("10.0.0.1", "Router 1", "10.0.0.0/24", "Unreachable")
	mgr.Trigger("10.0.0.2", "Router 2", "10.0.0.0/24", "Unreachable")

	acked := mgr.AcknowledgeAll("Bob", "Scheduled maintenance")
	if len(acked) != 2 {
		t.Fatalf("expected 2 acknowledged alerts, got %d", len(acked))
	}

	active := mgr.GetActiveAlerts()
	for _, a := range active {
		if !a.Acknowledged || a.AcknowledgedBy != "Bob" {
			t.Errorf("alert not acknowledged properly: %+v", a)
		}
	}
}

func TestResolveIf(t *testing.T) {
	mgr := NewManager(50)
	mgr.Trigger("10.0.0.1", "Host 1", "10.0.0.0/24", "Down")
	mgr.Trigger("10.0.0.2", "Host 2", "10.0.0.0/24", "Down")
	mgr.Trigger("10.0.0.3", "Host 3", "10.0.0.0/24", "Down")

	// Resolve hosts 1 and 3 based on predicate
	resolved := mgr.ResolveIf(func(a *Alert) bool {
		return a.IP == "10.0.0.1" || a.IP == "10.0.0.3"
	})

	if len(resolved) != 2 {
		t.Fatalf("expected 2 resolved alerts, got %d", len(resolved))
	}

	active := mgr.GetActiveAlerts()
	if len(active) != 1 || active[0].IP != "10.0.0.2" {
		t.Fatalf("expected remaining active alert for 10.0.0.2, got %+v", active)
	}
}

func BenchmarkGetActiveAlertsCleanupBaseline(b *testing.B) {
	mgr := NewManager(1000)
	for i := 0; i < 500; i++ {
		ip := "10.0.0." + string(rune(i))
		mgr.Trigger(ip, "host", "10.0.0.0/24", "err")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		alerts := mgr.GetActiveAlerts()
		for _, a := range alerts {
			_ = a.IP
		}
	}
}

func BenchmarkResolveIfCleanup(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mgr := NewManager(1000)
		for j := 0; j < 500; j++ {
			ip := "10.0.0." + string(rune(j))
			mgr.Trigger(ip, "host", "10.0.0.0/24", "err")
		}
		b.StartTimer()

		mgr.ResolveIf(func(a *Alert) bool {
			return false // none match, typical case where no alerts need cleanup
		})
	}
}
