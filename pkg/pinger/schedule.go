package pinger

import (
	"container/heap"
	"context"
	"hash/fnv"
	"time"
)

const (
	// minProbeInterval is the shortest probe interval the engine accepts.
	minProbeInterval = 500 * time.Millisecond
	// newHostSpread bounds how long a host added while the engine runs waits for its first
	// probe. A batch of new hosts (e.g. after a discovery sweep) is spread over this window.
	newHostSpread = 5 * time.Second
)

// schedEntry tracks when a host is next due for a probe.
type schedEntry struct {
	ip           string
	phase        float64       // fixed position within the interval, derived from the IP
	iv           time.Duration // interval the current due time was computed with
	due          time.Duration // monotonic offset from Engine.epoch
	lastDispatch time.Duration // when the last probe was handed to a worker
	probed       bool          // lastDispatch is set
	suspect      bool          // last probe failed: queued in suspectQ, probed under the suspect cap
	inFlight     bool
	index        int // position in its queue; -1 while not queued
}

// schedHeap is a min-heap of entries ordered by due time.
type schedHeap []*schedEntry

func (h schedHeap) Len() int           { return len(h) }
func (h schedHeap) Less(i, j int) bool { return h[i].due < h[j].due }
func (h schedHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *schedHeap) Push(x any) {
	ent := x.(*schedEntry)
	ent.index = len(*h)
	*h = append(*h, ent)
}

func (h *schedHeap) Pop() any {
	old := *h
	n := len(old)
	ent := old[n-1]
	old[n-1] = nil
	ent.index = -1
	*h = old[:n-1]
	return ent
}

// probeJob is a probe handed from the dispatcher to a worker.
type probeJob struct {
	ip      string
	suspect bool // holds a suspect slot until the probe completes
}

// ipPhase maps an IP to a fixed position in [0, 1) within its probe interval, so hosts that
// share an interval are spread across it and keep their spacing from one probe to the next.
func ipPhase(ip string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(ip))
	return float64(h.Sum64()>>11) / float64(1<<53)
}

// nextSlot returns the first time after `after` at which a host with the given phase is
// due. A host's slots repeat every interval at offset phase × interval.
func nextSlot(phase float64, interval, after time.Duration) time.Duration {
	off := time.Duration(phase * float64(interval))
	k := floorDiv(int64(after-off), int64(interval))
	return off + time.Duration(k+1)*interval
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// mono returns the current time as an offset from the engine's monotonic epoch, so the
// schedule is unaffected by wall-clock adjustments.
func (e *Engine) mono() time.Duration {
	return time.Since(e.epoch)
}

// probeIntervalUnsafe returns how often a host is probed: its subnet's interval, or the
// down-probe interval once the host has been DOWN for at least that long.
func (e *Engine) probeIntervalUnsafe(h *HostState) time.Duration {
	iv := e.effectiveIntervalUnsafe(h.CIDR)
	// Only while the last probe failed: a DOWN host that replied again (but has not reached
	// the recovery threshold yet) is probed at its normal interval.
	if d := e.config.DownProbeInterval; d > iv && h.Status == StatusDown && h.ConsecutiveFails > 0 &&
		h.LastStateChange != nil && time.Since(*h.LastStateChange) >= d {
		return d
	}
	return iv
}

// suspectCapUnsafe is how many workers probes of failing hosts may occupy at once. A probe of
// an unreachable host holds its worker for the whole timeout; reserving a quarter of the
// workers for hosts that answer keeps a large outage from delaying everyone else's probes.
func (e *Engine) suspectCapUnsafe() int {
	n := len(e.workerStops)
	reserved := n / 4
	if reserved < 1 {
		reserved = 1
	}
	if c := n - reserved; c > 1 {
		return c
	}
	return 1
}

func (e *Engine) queueFor(ent *schedEntry) *schedHeap {
	if ent.suspect {
		return &e.suspectQ
	}
	return &e.healthyQ
}

// signalSchedule wakes the dispatcher to re-evaluate the schedule.
func (e *Engine) signalSchedule() {
	select {
	case e.schedWake <- struct{}{}:
	default:
	}
}

// schedPushUnsafe queues an entry and wakes the dispatcher if it is now first in its queue.
func (e *Engine) schedPushUnsafe(ent *schedEntry) {
	heap.Push(e.queueFor(ent), ent)
	if ent.index == 0 {
		e.signalSchedule()
	}
}

// schedAddUnsafe starts scheduling a host. With spreadAll its first probe falls on its slot
// within one interval (used at Start, so startup probes are spread out); otherwise the host
// is probed within newHostSpread.
func (e *Engine) schedAddUnsafe(h *HostState, spreadAll bool) {
	if h.IsExcluded {
		return
	}
	if _, ok := e.sched[h.IP]; ok {
		return
	}
	ent := &schedEntry{
		ip:      h.IP,
		phase:   ipPhase(h.IP),
		iv:      e.probeIntervalUnsafe(h),
		suspect: h.ConsecutiveFails > 0,
		index:   -1,
	}
	now := e.mono()
	if spreadAll {
		ent.due = nextSlot(ent.phase, ent.iv, now)
	} else {
		spread := ent.iv
		if spread > newHostSpread {
			spread = newHostSpread
		}
		ent.due = now + time.Duration(ent.phase*float64(spread))
	}
	e.sched[h.IP] = ent
	e.schedPushUnsafe(ent)
}

// schedRemoveUnsafe stops scheduling a host. A probe already in flight still completes.
func (e *Engine) schedRemoveUnsafe(ip string) {
	ent, ok := e.sched[ip]
	if !ok {
		return
	}
	delete(e.sched, ip)
	if ent.index >= 0 {
		heap.Remove(e.queueFor(ent), ent.index)
	}
}

// schedRescheduleUnsafe moves a queued entry to its slot in a new interval, effective now
// rather than after the old interval runs out.
func (e *Engine) schedRescheduleUnsafe(ent *schedEntry, iv time.Duration) {
	now := e.mono()
	ent.iv = iv
	if !ent.probed {
		// Not probed yet: keep a pending first probe if it is sooner.
		if slot := nextSlot(ent.phase, iv, now); slot < ent.due {
			ent.due = slot
		}
	} else {
		after := ent.lastDispatch + iv/2
		if after < now {
			after = now
		}
		ent.due = nextSlot(ent.phase, iv, after)
	}
	if ent.index >= 0 {
		heap.Fix(e.queueFor(ent), ent.index)
		e.signalSchedule()
	}
}

// schedSyncUnsafe brings the schedule in line with the host map after targets, intervals or
// exclusions change: removed and excluded hosts leave the schedule, new hosts are probed
// within newHostSpread, and hosts whose probe interval changed move to their new slot.
func (e *Engine) schedSyncUnsafe() {
	if e.ctx == nil {
		return
	}
	for ip := range e.sched {
		if h, ok := e.hosts[ip]; !ok || h.IsExcluded {
			e.schedRemoveUnsafe(ip)
		}
	}
	for _, h := range e.hosts {
		if h.IsExcluded {
			continue
		}
		ent, ok := e.sched[h.IP]
		if !ok {
			e.schedAddUnsafe(h, false)
			continue
		}
		if ent.inFlight {
			continue // rescheduled with the current interval when the probe completes
		}
		if iv := e.probeIntervalUnsafe(h); iv != ent.iv {
			e.schedRescheduleUnsafe(ent, iv)
		}
	}
}

// schedDoneUnsafe reschedules a host after its probe completed. h is nil if the host is no
// longer monitored.
func (e *Engine) schedDoneUnsafe(ip string, h *HostState) {
	ent, ok := e.sched[ip]
	if !ok || !ent.inFlight {
		return // removed (and possibly re-added) while the probe ran
	}
	ent.inFlight = false
	if h == nil || h.IsExcluded {
		delete(e.sched, ip)
		return
	}
	ent.suspect = h.ConsecutiveFails > 0
	ent.iv = e.probeIntervalUnsafe(h)
	// The next slot at least half an interval from now keeps the host on its slot grid and,
	// after a late dispatch, skips missed slots instead of catching up in a burst.
	ent.due = nextSlot(ent.phase, ent.iv, e.mono()+ent.iv/2)
	e.schedPushUnsafe(ent)
}

// nextJobUnsafe takes the next due probe, preferring hosts that answer. Probes of failing
// hosts are handed out only while a suspect slot is free. If nothing can be handed out it
// returns how long until the next probe is due, or a negative duration if none is queued.
func (e *Engine) nextJobUnsafe() (probeJob, time.Duration, bool) {
	now := e.mono()
	suspectSlotFree := e.suspectInFlight < e.suspectCapUnsafe()

	if len(e.healthyQ) > 0 && e.healthyQ[0].due <= now {
		return e.startJobUnsafe(heap.Pop(&e.healthyQ).(*schedEntry), now), 0, true
	}
	if suspectSlotFree && len(e.suspectQ) > 0 && e.suspectQ[0].due <= now {
		e.suspectInFlight++
		return e.startJobUnsafe(heap.Pop(&e.suspectQ).(*schedEntry), now), 0, true
	}

	wait := time.Duration(-1)
	if len(e.healthyQ) > 0 {
		wait = e.healthyQ[0].due - now
	}
	if suspectSlotFree && len(e.suspectQ) > 0 {
		if w := e.suspectQ[0].due - now; wait < 0 || w < wait {
			wait = w
		}
	}
	return probeJob{}, wait, false
}

func (e *Engine) startJobUnsafe(ent *schedEntry, now time.Duration) probeJob {
	ent.inFlight = true
	ent.probed = true
	ent.lastDispatch = now
	return probeJob{ip: ent.ip, suspect: ent.suspect}
}

// dispatchLoop hands due probes to the workers. The work channel is unbuffered, so which
// probe goes next is decided when a worker is actually free.
func (e *Engine) dispatchLoop(ctx context.Context, workChan chan<- probeJob) {
	defer e.dispatchWg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		e.mu.Lock()
		job, wait, ok := e.nextJobUnsafe()
		e.mu.Unlock()

		if ok {
			select {
			case workChan <- job:
			case <-ctx.Done():
				return
			}
			continue
		}

		if wait < 0 {
			wait = time.Hour
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return
		case <-e.schedWake:
		case <-timer.C:
		}
	}
}
