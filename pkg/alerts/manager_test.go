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
