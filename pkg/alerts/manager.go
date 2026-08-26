package alerts

import (
	"fmt"
	"sync"
	"time"
)

// AlertState represents the status of an alert.
type AlertState string

const (
	AlertStateFiring       AlertState = "FIRING"
	AlertStateAcknowledged AlertState = "ACKNOWLEDGED"
	AlertStateResolved     AlertState = "RESOLVED"
)

// Alert represents an incident for an unreachable IP.
type Alert struct {
	ID             string     `json:"id"`
	IP             string     `json:"ip"`
	Alias          string     `json:"alias"`
	CIDR           string     `json:"cidr"`
	State          AlertState `json:"state"`
	StartedAt      time.Time  `json:"startedAt"`
	ResolvedAt     *time.Time `json:"resolvedAt,omitempty"`
	DurationSec    int64      `json:"durationSec"`
	LastError      string     `json:"lastError"`
	Acknowledged   bool       `json:"acknowledged"`
	AcknowledgedBy string     `json:"acknowledgedBy,omitempty"`
	AcknowledgedAt *time.Time `json:"acknowledgedAt,omitempty"`
	AckNote        string     `json:"ackNote,omitempty"`
}

// Manager manages active alerts, acknowledgements, and incident history.
type Manager struct {
	mu           sync.RWMutex
	activeAlerts map[string]*Alert // IP -> Alert
	history      []*Alert
	historyHead  int
	historyCount int
	maxHistory   int

	OnAlertTriggered    func(alert *Alert)
	OnAlertAcknowledged func(alert *Alert)
	OnAlertResolved     func(alert *Alert)
}

// NewManager creates a new Alert Manager.
func NewManager(maxHistory int) *Manager {
	if maxHistory <= 0 {
		maxHistory = 500
	}
	return &Manager{
		activeAlerts: make(map[string]*Alert),
		history:      make([]*Alert, maxHistory),
		maxHistory:   maxHistory,
	}
}

func (m *Manager) pushHistory(alert *Alert) {
	m.history[m.historyHead] = alert
	m.historyHead = (m.historyHead + 1) % m.maxHistory
	if m.historyCount < m.maxHistory {
		m.historyCount++
	}
}

// Trigger creates or updates an alert when an IP becomes unreachable.
func (m *Manager) Trigger(ip, alias, cidr, lastErr string) *Alert {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if existing, exists := m.activeAlerts[ip]; exists {
		existing.LastError = lastErr
		existing.DurationSec = int64(now.Sub(existing.StartedAt).Seconds())
		cpy := *existing
		return &cpy
	}

	id := fmt.Sprintf("alt-%s-%d", ip, now.Unix())
	alert := &Alert{
		ID:           id,
		IP:           ip,
		Alias:        alias,
		CIDR:         cidr,
		State:        AlertStateFiring,
		StartedAt:    now,
		LastError:    lastErr,
		Acknowledged: false,
	}

	m.activeAlerts[ip] = alert
	cpy := *alert

	if m.OnAlertTriggered != nil {
		go m.OnAlertTriggered(&cpy)
	}

	return &cpy
}

// Acknowledge marks an alert as acknowledged by an operator with an optional note.
func (m *Manager) Acknowledge(ipOrID, ackBy, note string) (*Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var target *Alert
	if a, exists := m.activeAlerts[ipOrID]; exists {
		target = a
	} else {
		for _, a := range m.activeAlerts {
			if a.ID == ipOrID {
				target = a
				break
			}
		}
	}

	if target == nil {
		return nil, fmt.Errorf("active alert for %q not found", ipOrID)
	}

	now := time.Now()
	if ackBy == "" {
		ackBy = "Operator"
	}

	target.Acknowledged = true
	target.AcknowledgedBy = ackBy
	target.AcknowledgedAt = &now
	target.AckNote = note
	target.State = AlertStateAcknowledged
	target.DurationSec = int64(now.Sub(target.StartedAt).Seconds())

	cpy := *target
	if m.OnAlertAcknowledged != nil {
		go m.OnAlertAcknowledged(&cpy)
	}

	return &cpy, nil
}

// AcknowledgeAll acknowledges all currently firing unacknowledged alerts.
func (m *Manager) AcknowledgeAll(ackBy, note string) []*Alert {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if ackBy == "" {
		ackBy = "Operator"
	}

	var acknowledged []*Alert
	for _, target := range m.activeAlerts {
		if !target.Acknowledged {
			target.Acknowledged = true
			target.AcknowledgedBy = ackBy
			target.AcknowledgedAt = &now
			target.AckNote = note
			target.State = AlertStateAcknowledged
			target.DurationSec = int64(now.Sub(target.StartedAt).Seconds())

			cpy := *target
			acknowledged = append(acknowledged, &cpy)
			if m.OnAlertAcknowledged != nil {
				go m.OnAlertAcknowledged(&cpy)
			}
		}
	}

	return acknowledged
}

// Resolve clears the alert when a host recovers and moves it to history.
func (m *Manager) Resolve(ip string) (*Alert, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	alert, exists := m.activeAlerts[ip]
	if !exists {
		return nil, false
	}

	delete(m.activeAlerts, ip)
	now := time.Now()
	alert.ResolvedAt = &now
	alert.State = AlertStateResolved
	alert.DurationSec = int64(now.Sub(alert.StartedAt).Seconds())

	m.pushHistory(alert)

	cpy := *alert
	if m.OnAlertResolved != nil {
		go m.OnAlertResolved(&cpy)
	}

	return &cpy, true
}

// ResolveIf evaluates active alerts directly under lock and resolves any matching alerts without creating intermediate slices of all active alerts.
func (m *Manager) ResolveIf(predicate func(a *Alert) bool) []*Alert {
	m.mu.Lock()
	defer m.mu.Unlock()

	var resolved []*Alert
	now := time.Now()

	for ip, alert := range m.activeAlerts {
		if predicate(alert) {
			delete(m.activeAlerts, ip)
			alert.ResolvedAt = &now
			alert.State = AlertStateResolved
			alert.DurationSec = int64(now.Sub(alert.StartedAt).Seconds())

			m.pushHistory(alert)

			cpy := *alert
			resolved = append(resolved, &cpy)
			if m.OnAlertResolved != nil {
				go m.OnAlertResolved(&cpy)
			}
		}
	}

	return resolved
}

// GetActiveAlerts returns all currently active (firing or acknowledged) alerts.
func (m *Manager) GetActiveAlerts() []*Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	res := make([]*Alert, 0, len(m.activeAlerts))
	for _, a := range m.activeAlerts {
		cpy := *a
		cpy.DurationSec = int64(now.Sub(a.StartedAt).Seconds())
		res = append(res, &cpy)
	}
	return res
}

// GetActiveAlertsMap returns a map of IP -> Alert copy for all currently active alerts.
func (m *Manager) GetActiveAlertsMap() map[string]*Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	res := make(map[string]*Alert, len(m.activeAlerts))
	for ip, a := range m.activeAlerts {
		cpy := *a
		cpy.DurationSec = int64(now.Sub(a.StartedAt).Seconds())
		res[ip] = &cpy
	}
	return res
}

// GetAlertHistory returns historical resolved alerts, ordered newest to oldest.
func (m *Manager) GetAlertHistory(limit int) []*Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 || limit > m.historyCount {
		limit = m.historyCount
	}

	res := make([]*Alert, 0, limit)
	for i := 0; i < limit; i++ {
		idx := (m.historyHead - 1 - i + m.maxHistory*2) % m.maxHistory
		cpy := *m.history[idx]
		res = append(res, &cpy)
	}
	return res
}

// GetAlertForIP checks if an active alert exists for the given IP.
func (m *Manager) GetAlertForIP(ip string) (*Alert, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.activeAlerts[ip]
	if !ok {
		return nil, false
	}
	cpy := *a
	return &cpy, true
}
