package pinger

import "time"

// Stats is a snapshot of the probe engine for health checks and metrics.
type Stats struct {
	Running        bool
	Hosts          int // monitored hosts, including excluded ones
	ScheduledHosts int // hosts with a probe schedule
	Workers        int
	BusyWorkers    int // probes in flight
	SuspectBusy    int // probes of failing hosts in flight
	// HealthyLag is how long the most overdue probe of a replying host has waited for a free
	// worker; SuspectLag is the same for hosts whose last probe failed. Both stay near zero
	// while the engine keeps up with its schedule.
	HealthyLag    time.Duration
	SuspectLag    time.Duration
	ProbesTotal   uint64    // scheduled probes completed since the start
	FailuresTotal uint64    // scheduled probes without a reply
	LastProbe     time.Time // completion of the last scheduled probe; zero before the first
	// LocalErrors counts probes that failed because of this machine; they are not part of
	// ProbesTotal and FailuresTotal.
	LocalErrors uint64
	ProbeMode   string // ModePingSocket, ModeRawSocket or ModePingCommand; empty before the first probe
	LateReplies uint64 // echo replies that matched no waiting probe (late or duplicate)
	RxDropped   uint64 // packets the kernel dropped because the socket buffer was full
}

// Stats returns a snapshot of the engine state.
func (e *Engine) Stats() Stats {
	ps := e.prober.Stats()
	e.mu.RLock()
	defer e.mu.RUnlock()

	s := Stats{
		Running:        e.ctx != nil,
		Hosts:          len(e.hosts),
		ScheduledHosts: len(e.sched),
		Workers:        len(e.workerStops),
		BusyWorkers:    e.inFlight,
		SuspectBusy:    e.suspectInFlight,
		ProbesTotal:    e.probesTotal.Load(),
		FailuresTotal:  e.failuresTotal.Load(),
		LocalErrors:    e.localErrorsTotal.Load(),
		ProbeMode:      ps.Mode,
		LateReplies:    ps.LateReplies,
		RxDropped:      ps.RxDropped,
	}
	if ns := e.lastProbe.Load(); ns != 0 {
		s.LastProbe = time.Unix(0, ns)
	}
	now := e.mono()
	if len(e.healthyQ) > 0 && e.healthyQ[0].due < now {
		s.HealthyLag = now - e.healthyQ[0].due
	}
	if len(e.suspectQ) > 0 && e.suspectQ[0].due < now {
		s.SuspectLag = now - e.suspectQ[0].due
	}
	return s
}

// ProbeMode returns how the engine sends probes (ModePingSocket, ModeRawSocket or
// ModePingCommand), opening the ICMP socket if needed. It is empty if no socket could be
// opened for now.
func (e *Engine) ProbeMode() string {
	return e.prober.Mode()
}
