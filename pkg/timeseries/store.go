package timeseries

import (
	"container/list"
	"context"
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
	CIDR          string             `json:"cidr"`
	TotalHosts    int                `json:"totalHosts"`
	OnlineCount   int                `json:"onlineCount"`
	OfflineCount  int                `json:"offlineCount"`
	PendingCount  int                `json:"pendingCount"`
	ExcludedCount int                `json:"excludedCount"`
	AvgLatencyMs  float64            `json:"avgLatencyMs"`
	P95LatencyMs  float64            `json:"p95LatencyMs"`
	HealthPct     float64            `json:"healthPct"`
	Cells         []SubnetMatrixCell `json:"cells"`
}

const DefaultMaxHosts = 5000

// Store manages in-memory multi-tier time-series metric retention and rollups for all IPs.
type Store struct {
	mu           sync.RWMutex
	maxHosts     int
	rawBuffers   map[string]*HostRingBuffer
	minuteSeries map[string]*RollupSeries
	hourSeries   map[string]*RollupSeries
	lruList      *list.List
	lruIndex     map[string]*list.Element

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
		lruList:      list.New(),
		lruIndex:     make(map[string]*list.Element),
	}
}

// SetCapacity dynamically updates the maximum host retention capacity.
func (s *Store) SetCapacity(maxHosts int) {
	if maxHosts <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxHosts = maxHosts
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

	// Evict least-recently-used host if at capacity
	if s.lruList.Len() >= s.maxHosts {
		oldest := s.lruList.Back()
		if oldest != nil {
			oldestIP := oldest.Value.(string)
			s.lruList.Remove(oldest)
			delete(s.lruIndex, oldestIP)
			delete(s.rawBuffers, oldestIP)
			delete(s.minuteSeries, oldestIP)
			delete(s.hourSeries, oldestIP)
		}
	}

	elem := s.lruList.PushFront(ip)
	s.lruIndex[ip] = elem
	rb := NewHostRingBuffer(120) // ~10-20 min raw memory window
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
	rs = NewRollupSeries(1440) // 24 hours of 1-minute rollups
	s.minuteSeries[ip] = rs
	return rs
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
	rs = NewRollupSeries(720) // 30 days of 1-hour rollups
	s.hourSeries[ip] = rs
	return rs
}

// Record inserts a new raw probe sample into the time-series store in O(1) time.
func (s *Store) Record(ip string, timestamp time.Time, latencyMs float64, success bool) {
	rb := s.getOrCreateRawBuffer(ip)
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

	// Use 1-hour rollups for longer windows
	s.mu.RLock()
	hs, ok := s.hourSeries[ip]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	cutoff := time.Now().Add(-window)
	return hs.GetSince(cutoff)
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
}

// GetTopOutliers returns hosts exhibiting high packet loss, latency spikes, or severe jitter.
func (s *Store) GetTopOutliers(limit int, isValidHostFn func(ip string) (bool, string)) []OutlierHost {
	s.mu.RLock()
	ips := make([]string, 0, len(s.rawBuffers))
	for ip := range s.rawBuffers {
		ips = append(ips, ip)
	}
	s.mu.RUnlock()

	var outliers []OutlierHost
	for _, ip := range ips {
		subnet := ""
		if isValidHostFn != nil {
			valid, sub := isValidHostFn(ip)
			if !valid {
				continue
			}
			subnet = sub
		}

		s.mu.RLock()
		rb, ok := s.rawBuffers[ip]
		s.mu.RUnlock()
		if !ok {
			continue
		}

		avgLat, _, _, p95Lat, lossRatio, jitter, count := rb.ComputeSummary()
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

// Background Downsampling Routine
func (s *Store) rollupLoop() {
	defer s.wg.Done()

	s.mu.RLock()
	ctx := s.ctx
	s.mu.RUnlock()
	if ctx == nil {
		return
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	lastMinuteRollup := time.Now()
	lastHourRollup := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			// Check if 1-minute downsampling is due
			if now.Sub(lastMinuteRollup) >= 1*time.Minute {
				s.computeMinuteRollups(now)
				lastMinuteRollup = now
			}

			// Check if 1-hour downsampling is due
			if now.Sub(lastHourRollup) >= 1*time.Hour {
				s.computeHourRollups(now)
				lastHourRollup = now
			}
		}
	}
}

func (s *Store) computeMinuteRollups(now time.Time) {
	s.mu.RLock()
	ips := make([]string, 0, len(s.rawBuffers))
	for ip := range s.rawBuffers {
		ips = append(ips, ip)
	}
	s.mu.RUnlock()

	cutoff := now.Add(-1 * time.Minute)
	for _, ip := range ips {
		s.mu.RLock()
		rb, ok := s.rawBuffers[ip]
		s.mu.RUnlock()
		if !ok {
			continue
		}

		samples := rb.GetSince(cutoff)
		if len(samples) > 0 {
			rollup := ComputeRollup(now, 1*time.Minute, samples)
			ms := s.getOrCreateMinuteSeries(ip)
			ms.Append(rollup)
		}
	}
}

func (s *Store) computeHourRollups(now time.Time) {
	s.mu.RLock()
	ips := make([]string, 0, len(s.minuteSeries))
	for ip := range s.minuteSeries {
		ips = append(ips, ip)
	}
	s.mu.RUnlock()

	cutoff := now.Add(-1 * time.Hour)
	for _, ip := range ips {
		s.mu.RLock()
		ms, ok := s.minuteSeries[ip]
		s.mu.RUnlock()
		if !ok {
			continue
		}

		minuteRollups := ms.GetSince(cutoff)
		if len(minuteRollups) > 0 {
			hourPoint := AggregateRollups(now, 1*time.Hour, minuteRollups)
			hs := s.getOrCreateHourSeries(ip)
			hs.Append(hourPoint)
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
	for i := 0; i < len(ipStr); i++ {
		b := ipStr[i]
		if b >= '0' && b <= '9' {
			octet = octet*10 + uint32(b-'0')
		} else if b == '.' {
			val = (val << 8) | octet
			octet = 0
		}
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
