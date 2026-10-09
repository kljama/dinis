package timeseries

import (
	"container/list"
	"context"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OutlierHost represents a monitored endpoint showing degraded performance, packet loss, or high jitter.
type OutlierHost struct {
	IP            string  `json:"ip"`
	Alias         string  `json:"alias,omitempty"`
	Subnet        string  `json:"subnet"`
	AvgLatencyMs  float64 `json:"avgLatencyMs"`
	P95LatencyMs  float64 `json:"p95LatencyMs"`
	PacketLossPct float64 `json:"packetLossPct"`
	JitterMs      float64 `json:"jitterMs"`
	SampleCount   int     `json:"sampleCount"`
	Severity      string  `json:"severity"` // "CRITICAL", "WARNING", "DEGRADED"
}

// SubnetMatrixCell represents a single IP inside a /24 subnet block.
type SubnetMatrixCell struct {
	IP            string  `json:"ip"`
	HostIndex     int     `json:"hostIndex"` // 0 to 255
	Status        string  `json:"status"`    // "UP", "DOWN", "EXCLUDED", "PENDING"
	LatencyMs     float64 `json:"latencyMs"`
	PacketLossPct float64 `json:"packetLossPct"`
	AlertActive   bool    `json:"alertActive"`
	AlertAck      bool    `json:"alertAck"`
	Alias         string  `json:"alias,omitempty"`
}

// SubnetMatrixBlock represents a /24 subnet containing up to 256 cells and aggregate statistics.
type SubnetMatrixBlock struct {
	CIDR              string             `json:"cidr"`
	ParentCIDR        string             `json:"parentCidr,omitempty"`
	ParentDescription string             `json:"parentDescription,omitempty"`
	IntervalSec       float64            `json:"intervalSec"`
	IsCustomInterval  bool               `json:"isCustomInterval"`
	TotalHosts        int                `json:"totalHosts"`
	OnlineCount       int                `json:"onlineCount"`
	OfflineCount      int                `json:"offlineCount"`
	PendingCount      int                `json:"pendingCount"`
	ExcludedCount     int                `json:"excludedCount"`
	AvgLatencyMs      float64            `json:"avgLatencyMs"`
	P95LatencyMs      float64            `json:"p95LatencyMs"`
	HealthPct         float64            `json:"healthPct"`
	Cells             []SubnetMatrixCell `json:"cells"`
}

const DefaultMaxHosts = 5000

// BytesPerHost is the measured memory (Go heap) that the full latency history of one host
// uses: 128 raw samples, 2 hours of minute points, 30 days of hour points, the hour in
// progress, and the map and list entries.
const BytesPerHost = 44 * 1024

// EstimateMemoryMB returns the memory in MB that the latency history of maxHosts hosts uses
// when all of them have a full history.
func EstimateMemoryMB(maxHosts int) int {
	return int(int64(maxHosts) * BytesPerHost / (1 << 20))
}

// Store manages in-memory multi-tier time-series metric retention and rollups for all IPs.
type Store struct {
	mu           sync.RWMutex
	maxHosts     int
	rawBuffers   map[string]*HostRingBuffer
	minuteSeries map[string]*RollupSeries
	hourSeries   map[string]*RollupSeries
	aggs         map[string]*hostAgg // jitter carry and hour in progress, per host
	lruList      *list.List
	lruIndex     map[string]*list.Element

	// Samples dropped because the host limit was reached, reported via a rate-limited log
	droppedSamples   uint64
	lastCapacityWarn time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewStore creates a new time-series metric store with default host limit (5,000 hosts).
func NewStore() *Store {
	return NewStoreWithLimit(DefaultMaxHosts)
}

// NewStoreWithLimit creates a new time-series metric store with a custom max host capacity.
func NewStoreWithLimit(maxHosts int) *Store {
	if maxHosts <= 0 {
		maxHosts = DefaultMaxHosts
	}
	return &Store{
		maxHosts:     maxHosts,
		rawBuffers:   make(map[string]*HostRingBuffer),
		minuteSeries: make(map[string]*RollupSeries),
		hourSeries:   make(map[string]*RollupSeries),
		aggs:         make(map[string]*hostAgg),
		lruList:      list.New(),
		lruIndex:     make(map[string]*list.Element),
	}
}

// SetCapacity dynamically updates the maximum host retention capacity.
// Shrinking below the current host count evicts the least-recently-updated hosts.
func (s *Store) SetCapacity(maxHosts int) {
	if maxHosts <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxHosts = maxHosts
	for s.lruList.Len() > s.maxHosts {
		oldest := s.lruList.Back()
		oldestIP := oldest.Value.(string)
		s.lruList.Remove(oldest)
		delete(s.lruIndex, oldestIP)
		delete(s.rawBuffers, oldestIP)
		delete(s.minuteSeries, oldestIP)
		delete(s.hourSeries, oldestIP)
		delete(s.aggs, oldestIP)
	}
}

// Start launches the background automated downsampling ticker.
// Safe to call multiple times; redundant calls while running are ignored.
func (s *Store) Start() {
	s.mu.Lock()
	if s.ctx != nil {
		s.mu.Unlock()
		return
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	s.mu.Unlock()

	go s.rollupLoop()
}

// Stop gracefully stops background downsampling routines.
// Safe to call multiple times or before Start. Allows subsequent Start calls.
func (s *Store) Stop() {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
		s.ctx = nil
		s.cancel = nil
	}
	s.mu.Unlock()

	s.wg.Wait()
}

func (s *Store) getOrCreateRawBuffer(ip string) *HostRingBuffer {
	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, ok := s.lruIndex[ip]; ok {
		s.lruList.MoveToFront(elem)
		return s.rawBuffers[ip]
	}

	// At capacity: don't track new hosts. Evicting here would thrash, because hosts are
	// probed round-robin and the evicted host is always the next one probed, leaving
	// every host with no history. Slots free up when hosts are pruned or removed.
	if s.lruList.Len() >= s.maxHosts {
		s.droppedSamples++
		if now := time.Now(); now.Sub(s.lastCapacityWarn) >= time.Minute {
			log.Printf("[TIMESERIES] Metric host limit (%d) reached; %d samples from additional hosts not recorded. Raise MaxMetricHosts to retain history for all hosts.", s.maxHosts, s.droppedSamples)
			s.lastCapacityWarn = now
			s.droppedSamples = 0
		}
		return nil
	}

	elem := s.lruList.PushFront(ip)
	s.lruIndex[ip] = elem
	rb := NewHostRingBuffer(RawSampleRetention)
	s.rawBuffers[ip] = rb
	return rb
}

func (s *Store) getOrCreateMinuteSeries(ip string) *RollupSeries {
	s.mu.RLock()
	rs, ok := s.minuteSeries[ip]
	s.mu.RUnlock()
	if ok {
		return rs
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if rs, ok := s.minuteSeries[ip]; ok {
		return rs
	}
	rs = NewRollupSeries(MinuteRollupRetention, time.Minute)
	s.minuteSeries[ip] = rs
	return rs
}

func (s *Store) getOrCreateAgg(ip string) *hostAgg {
	s.mu.RLock()
	a, ok := s.aggs[ip]
	s.mu.RUnlock()
	if ok {
		return a
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.aggs[ip]; ok {
		return a
	}
	a = &hostAgg{}
	s.aggs[ip] = a
	return a
}

func (s *Store) getOrCreateHourSeries(ip string) *RollupSeries {
	s.mu.RLock()
	rs, ok := s.hourSeries[ip]
	s.mu.RUnlock()
	if ok {
		return rs
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if rs, ok := s.hourSeries[ip]; ok {
		return rs
	}
	rs = NewRollupSeries(HourRollupRetention, time.Hour)
	s.hourSeries[ip] = rs
	return rs
}

// Record inserts a new raw probe sample into the time-series store in O(1) time.
func (s *Store) Record(ip string, timestamp time.Time, latencyMs float64, success bool) {
	rb := s.getOrCreateRawBuffer(ip)
	if rb == nil {
		return
	}
	rb.Push(timestamp, latencyMs, success)
}

// RemoveHost removes all stored metric series for an IP.
func (s *Store) RemoveHost(ip string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem, ok := s.lruIndex[ip]; ok {
		s.lruList.Remove(elem)
		delete(s.lruIndex, ip)
	}
	delete(s.rawBuffers, ip)
	delete(s.minuteSeries, ip)
	delete(s.hourSeries, ip)
	delete(s.aggs, ip)
}

// GetRecentRawSamples returns the most recent raw samples for an IP.
func (s *Store) GetRecentRawSamples(ip string, count int) []RawSample {
	s.mu.RLock()
	rb, ok := s.rawBuffers[ip]
	s.mu.RUnlock()
	if !ok {
		return nil
	}

	all := rb.GetAll()
	if count > 0 && len(all) > count {
		return all[len(all)-count:]
	}
	return all
}

// GetHostHistory returns historical time-series rollups for an IP based on the requested time window.
func (s *Store) GetHostHistory(ip string, window time.Duration) []RollupPoint {
	if window <= 2*time.Hour {
		// Use 1-minute rollups for windows up to 2 hours
		s.mu.RLock()
		ms, ok := s.minuteSeries[ip]
		s.mu.RUnlock()
		if !ok {
			return nil
		}
		cutoff := time.Now().Add(-window)
		return ms.GetSince(cutoff)
	}

	// Use 1-hour rollups for longer windows, plus the hour in progress
	s.mu.RLock()
	hs, ok := s.hourSeries[ip]
	agg := s.aggs[ip]
	s.mu.RUnlock()
	cutoff := time.Now().Add(-window)
	var pts []RollupPoint
	if ok {
		pts = hs.GetSince(cutoff)
	}
	if agg != nil {
		if p, ok := agg.currentHour(); ok && !p.Timestamp.Before(cutoff) {
			pts = append(pts, p)
		}
	}
	return pts
}

// PruneHosts removes time-series metric buffers and rollups for hosts that are no longer monitored.
func (s *Store) PruneHosts(activeIPs map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for ip := range s.rawBuffers {
		if !activeIPs[ip] {
			if elem, ok := s.lruIndex[ip]; ok {
				s.lruList.Remove(elem)
				delete(s.lruIndex, ip)
			}
			delete(s.rawBuffers, ip)
			delete(s.minuteSeries, ip)
			delete(s.hourSeries, ip)
			delete(s.aggs, ip)
		}
	}
	for ip := range s.minuteSeries {
		if !activeIPs[ip] {
			delete(s.minuteSeries, ip)
		}
	}
	for ip := range s.hourSeries {
		if !activeIPs[ip] {
			delete(s.hourSeries, ip)
		}
	}
	for ip := range s.aggs {
		if !activeIPs[ip] {
			delete(s.aggs, ip)
		}
	}
}

// GetTopOutliers returns hosts exhibiting high packet loss, latency spikes, or severe jitter.
func (s *Store) GetTopOutliers(limit int, isValidHostFn func(ip string) (valid bool, subnet string, alias string)) []OutlierHost {
	s.mu.RLock()
	ips := make([]string, 0, len(s.rawBuffers))
	for ip := range s.rawBuffers {
		ips = append(ips, ip)
	}
	s.mu.RUnlock()

	var outliers []OutlierHost
	var scratch []float64
	for _, ip := range ips {
		subnet := ""
		alias := ""
		if isValidHostFn != nil {
			valid, sub, al := isValidHostFn(ip)
			if !valid {
				continue
			}
			subnet = sub
			alias = al
		}

		s.mu.RLock()
		rb, ok := s.rawBuffers[ip]
		s.mu.RUnlock()
		if !ok {
			continue
		}

		var avgLat, p95Lat, lossRatio, jitter float64
		var count int
		avgLat, _, _, p95Lat, lossRatio, jitter, count, scratch = rb.computeSummary(scratch)
		if count < 2 {
			continue
		}

		lossPct := lossRatio * 100.0

		// Identify outliers: packet loss > 0% or latency > 100ms or jitter > 30ms
		if lossPct > 0.0 || avgLat > 100.0 || p95Lat > 150.0 || jitter > 30.0 {
			var severity string
			if lossPct >= 50.0 {
				severity = "CRITICAL"
			} else if lossPct > 0.0 || p95Lat > 250.0 || jitter > 50.0 {
				severity = "WARNING"
			} else {
				severity = "DEGRADED"
			}

			outliers = append(outliers, OutlierHost{
				IP:            ip,
				Alias:         alias,
				Subnet:        subnet,
				AvgLatencyMs:  math.Round(avgLat*100) / 100,
				P95LatencyMs:  math.Round(p95Lat*100) / 100,
				PacketLossPct: math.Round(lossPct*10) / 10,
				JitterMs:      jitter,
				SampleCount:   count,
				Severity:      severity,
			})
		}
	}

	// Sort outliers by severity / packet loss descending, then P95 latency descending
	sort.Slice(outliers, func(i, j int) bool {
		if outliers[i].PacketLossPct != outliers[j].PacketLossPct {
			return outliers[i].PacketLossPct > outliers[j].PacketLossPct
		}
		return outliers[i].P95LatencyMs > outliers[j].P95LatencyMs
	})

	if limit > 0 && len(outliers) > limit {
		return outliers[:limit]
	}
	return outliers
}

const (
	// rollupTick is how often the rollup loop checks for completed buckets.
	rollupTick = time.Second
	// rollupGrace is how long after a bucket ends it is rolled up, so probes that finish
	// right at the boundary are recorded first.
	rollupGrace = time.Second
)

// Background Downsampling Routine.
//
// Buckets are aligned to wall-clock minutes and hours, and each completed bucket is rolled
// up exactly once from the samples timestamped inside it. No sample is skipped or counted
// twice, however the ticker jitters.
func (s *Store) rollupLoop() {
	defer s.wg.Done()

	s.mu.RLock()
	ctx := s.ctx
	s.mu.RUnlock()
	if ctx == nil {
		return
	}

	ticker := time.NewTicker(rollupTick)
	defer ticker.Stop()

	start := time.Now()
	nextMinute := start.Truncate(time.Minute).Add(time.Minute)
	nextHour := start.Truncate(time.Hour).Add(time.Hour)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			// Minutes first: an hour bucket aggregates the minute rollups inside it.
			nextMinute = rollupDueBuckets(now, nextMinute, time.Minute, s.computeMinuteRollups)
			nextHour = rollupDueBuckets(now, nextHour, time.Hour, s.computeHourRollups)
		}
	}
}

// rollupDueBuckets rolls up every bucket of the given size that ended at least rollupGrace
// before now, starting with the bucket ending at nextEnd, and returns the end of the next
// pending bucket. After a long pause (suspend, clock jump) it skips to the most recently
// completed bucket instead of replaying buckets whose samples are gone.
func rollupDueBuckets(now, nextEnd time.Time, size time.Duration, compute func(start, end time.Time)) time.Time {
	if now.Sub(nextEnd) > 2*size {
		nextEnd = now.Truncate(size)
	}
	for !now.Before(nextEnd.Add(rollupGrace)) {
		compute(nextEnd.Add(-size), nextEnd)
		nextEnd = nextEnd.Add(size)
	}
	return nextEnd
}

// computeMinuteRollups rolls up the raw samples timestamped in [start, end) into one
// 1-minute point per host, timestamped at the start of the bucket. The samples also go into
// the host's hour in progress, from which computeHourRollups makes the hour point.
func (s *Store) computeMinuteRollups(start, end time.Time) {
	s.mu.RLock()
	ips := make([]string, 0, len(s.rawBuffers))
	for ip := range s.rawBuffers {
		ips = append(ips, ip)
	}
	s.mu.RUnlock()

	b := bucketStats{}
	for _, ip := range ips {
		s.mu.RLock()
		rb, ok := s.rawBuffers[ip]
		s.mu.RUnlock()
		if !ok {
			continue
		}

		if rollup, ok := s.getOrCreateAgg(ip).rollupMinute(rb, start, end, &b); ok {
			s.getOrCreateMinuteSeries(ip).Append(rollup)
		}
	}
}

// computeHourRollups ends the hour [start, end) of every host: its hour in progress becomes
// a 1-hour point, timestamped at the start of the bucket. Percentiles come from all samples
// of the hour (see hourStats), not from the minute points.
func (s *Store) computeHourRollups(start, end time.Time) {
	s.mu.RLock()
	ips := make([]string, 0, len(s.aggs))
	for ip := range s.aggs {
		ips = append(ips, ip)
	}
	s.mu.RUnlock()

	for _, ip := range ips {
		s.mu.RLock()
		agg, ok := s.aggs[ip]
		s.mu.RUnlock()
		if !ok {
			continue
		}
		if point, ok := agg.rollupHour(start); ok {
			s.getOrCreateHourSeries(ip).Append(point)
		}
	}
}

// GenerateSubnetMatrix builds matrix blocks only for subnets with monitored/discovered hosts.
func GenerateSubnetMatrix(hostsBySubnet map[string][]SubnetMatrixCell) []SubnetMatrixBlock {
	blocks := make([]SubnetMatrixBlock, 0, len(hostsBySubnet))

	for cidr, cells := range hostsBySubnet {
		if len(cells) == 0 {
			continue
		}

		var online, offline, pending, excluded int
		var sumLat float64
		var latencies []float64

		for _, c := range cells {
			switch c.Status {
			case "UP":
				online++
				if c.LatencyMs > 0 {
					sumLat += c.LatencyMs
					latencies = append(latencies, c.LatencyMs)
				}
			case "DOWN":
				offline++
			case "PENDING":
				pending++
			case "EXCLUDED":
				excluded++
			}
		}

		totalActive := online + offline
		var healthPct float64
		if totalActive > 0 {
			healthPct = (float64(online) / float64(totalActive)) * 100.0
		} else {
			healthPct = 100.0
		}

		var avgLat, p95Lat float64
		if len(latencies) > 0 {
			avgLat = sumLat / float64(len(latencies))
			sort.Float64s(latencies)
			p95Lat = getPercentile(latencies, 0.95)
		}

		// Pre-parse cells to uint32 for fast sorting
		type parsedCell struct {
			cell   SubnetMatrixCell
			ipUint uint32
		}
		parsedCells := make([]parsedCell, len(cells))
		for i, c := range cells {
			parsedCells[i] = parsedCell{
				cell:   c,
				ipUint: parseIPv4ToUint32(c.IP),
			}
		}

		sort.Slice(parsedCells, func(i, j int) bool {
			return parsedCells[i].ipUint < parsedCells[j].ipUint
		})

		for i := range cells {
			cells[i] = parsedCells[i].cell
		}

		blocks = append(blocks, SubnetMatrixBlock{
			CIDR:          cidr,
			TotalHosts:    len(cells),
			OnlineCount:   online,
			OfflineCount:  offline,
			PendingCount:  pending,
			ExcludedCount: excluded,
			AvgLatencyMs:  avgLat,
			P95LatencyMs:  p95Lat,
			HealthPct:     healthPct,
			Cells:         cells,
		})
	}

	// Pre-parse block CIDR bases for fast block sorting
	type parsedBlock struct {
		block     SubnetMatrixBlock
		ipUint    uint32
		prefixLen int
	}
	parsedBlocks := make([]parsedBlock, len(blocks))
	for i, b := range blocks {
		ipUint, prefixLen := parseCIDRBaseUint32(b.CIDR)
		parsedBlocks[i] = parsedBlock{
			block:     b,
			ipUint:    ipUint,
			prefixLen: prefixLen,
		}
	}

	sort.Slice(parsedBlocks, func(i, j int) bool {
		if parsedBlocks[i].ipUint != parsedBlocks[j].ipUint {
			return parsedBlocks[i].ipUint < parsedBlocks[j].ipUint
		}
		if parsedBlocks[i].prefixLen != parsedBlocks[j].prefixLen {
			return parsedBlocks[i].prefixLen < parsedBlocks[j].prefixLen
		}
		return parsedBlocks[i].block.CIDR < parsedBlocks[j].block.CIDR
	})

	for i := range blocks {
		blocks[i] = parsedBlocks[i].block
	}

	return blocks
}

func parseIPv4ToUint32(ipStr string) uint32 {
	var val uint32
	var octet uint32
	var dots int
	var hasDigits bool
	for i := 0; i < len(ipStr); i++ {
		b := ipStr[i]
		if b >= '0' && b <= '9' {
			octet = octet*10 + uint32(b-'0')
			if octet > 255 {
				return 0
			}
			hasDigits = true
		} else if b == '.' {
			if !hasDigits || dots >= 3 {
				return 0
			}
			val = (val << 8) | octet
			octet = 0
			dots++
			hasDigits = false
		} else {
			return 0
		}
	}
	if dots != 3 || !hasDigits || octet > 255 {
		return 0
	}
	return (val << 8) | octet
}

func parseCIDRBaseUint32(cidrStr string) (uint32, int) {
	ipStr := cidrStr
	prefixLen := 32
	if idx := strings.IndexByte(cidrStr, '/'); idx != -1 {
		ipStr = cidrStr[:idx]
		if p, err := strconv.Atoi(cidrStr[idx+1:]); err == nil {
			prefixLen = p
		}
	}
	return parseIPv4ToUint32(ipStr), prefixLen
}
