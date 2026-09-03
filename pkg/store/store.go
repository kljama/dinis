package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// CIDRConfig represents a configured CIDR block to monitor.
type CIDRConfig struct {
	CIDR               string    `json:"cidr"`
	Description        string    `json:"description"`
	Enabled            bool      `json:"enabled"`
	IncludeNetAndBcast bool      `json:"includeNetAndBcast"`
	CreatedAt          time.Time `json:"createdAt"`
}

// ExclusionConfig represents a host or subnet to exclude from monitoring.
type ExclusionConfig struct {
	Rule      string    `json:"rule"` // IP or CIDR
	Reason    string    `json:"reason"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"createdAt"`
}

// HostMeta represents user-defined metadata for a specific host IP.
type HostMeta struct {
	IP        string    `json:"ip"`
	Alias     string    `json:"alias"`
	Notes     string    `json:"notes"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// DiscoveredHost represents a dynamically discovered active endpoint under an enrolled CIDR.
type DiscoveredHost struct {
	IP             string    `json:"ip"`
	CIDR           string    `json:"cidr"`
	DiscoveredAt   time.Time `json:"discoveredAt"`
	LastDiscovered time.Time `json:"lastDiscovered"`
	IsStatic       bool      `json:"isStatic"`
}

// AppSettings holds dynamic configuration tunable via Web UI settings modal.
type AppSettings struct {
	DiscoveryIntervalMin int     `json:"discoveryIntervalMin"` // 0 disables auto-discovery
	IntervalSec          float64 `json:"intervalSec"`
	TimeoutMs            int     `json:"timeoutMs"`
	FailThreshold        int     `json:"failThreshold"`
	Concurrency          int     `json:"concurrency"`
	MaxMetricHosts       int     `json:"maxMetricHosts"` // Capacity limit for time-series metric retention
	AutoDiscovery        bool    `json:"autoDiscovery"`
}

// DefaultSettings returns safe production defaults.
func DefaultSettings() AppSettings {
	return AppSettings{
		DiscoveryIntervalMin: 240, // 4 hours
		IntervalSec:          60,  // 60s ping cycle
		TimeoutMs:            1000,
		FailThreshold:        2,
		Concurrency:          100,
		MaxMetricHosts:       10000,
		AutoDiscovery:        true,
	}
}

// StoreData represents the root persisted JSON structure.
type StoreData struct {
	CIDRs           []CIDRConfig               `json:"cidrs"`
	Exclusions      []ExclusionConfig          `json:"exclusions"`
	HostMeta        map[string]HostMeta        `json:"hostMeta"`
	DiscoveredHosts map[string]DiscoveredHost  `json:"discoveredHosts"`
	Settings        AppSettings                `json:"settings"`
}

// Store manages thread-safe atomic reading and writing of configuration.
type Store struct {
	mu       sync.RWMutex
	filePath string
	lockFile *os.File
	data     StoreData
}

// NewStore initializes or loads the persistent store from the specified file path.
// It acquires an OS advisory lock on <filePath>.lock to prevent multi-instance data corruption.
func NewStore(filePath string) (*Store, error) {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	lockPath := filePath + ".lock"
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file %s: %w", lockPath, err)
	}

	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("cannot acquire lock on %s: database is locked by another running DINIS process", lockPath)
	}

	s := &Store{
		filePath: filePath,
		lockFile: lf,
		data: StoreData{
			CIDRs:           make([]CIDRConfig, 0),
			Exclusions:      make([]ExclusionConfig, 0),
			HostMeta:        make(map[string]HostMeta),
			DiscoveredHosts: make(map[string]DiscoveredHost),
			Settings:        DefaultSettings(),
		},
	}

	if err := s.load(); err != nil {
		_ = s.Close()
		return nil, err
	}

	return s, nil
}

// Close unlocks and releases the file lock descriptor.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lockFile != nil {
		_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
		err := s.lockFile.Close()
		s.lockFile = nil
		return err
	}
	return nil
}

func (s *Store) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		// File does not exist yet; initialize default sample targets
		s.data.CIDRs = []CIDRConfig{
			{
				CIDR:        "127.0.0.1/32",
				Description: "Localhost loopback",
				Enabled:     true,
				CreatedAt:   time.Now(),
			},
			{
				CIDR:        "1.1.1.1/32",
				Description: "Cloudflare DNS",
				Enabled:     true,
				CreatedAt:   time.Now(),
			},
			{
				CIDR:        "8.8.8.8/32",
				Description: "Google DNS",
				Enabled:     true,
				CreatedAt:   time.Now(),
			},
		}
		return s.saveUnsafe()
	}

	raw, err := os.ReadFile(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to read store file %s: %w", s.filePath, err)
	}

	var data StoreData
	if err := json.Unmarshal(raw, &data); err != nil {
		return fmt.Errorf("failed to parse JSON from %s: %w", s.filePath, err)
	}

	if data.HostMeta == nil {
		data.HostMeta = make(map[string]HostMeta)
	}
	if data.DiscoveredHosts == nil {
		data.DiscoveredHosts = make(map[string]DiscoveredHost)
	}
	if data.CIDRs == nil {
		data.CIDRs = make([]CIDRConfig, 0)
	}
	if data.Exclusions == nil {
		data.Exclusions = make([]ExclusionConfig, 0)
	}
	if data.Settings.IntervalSec <= 0 {
		data.Settings = DefaultSettings()
	}

	s.data = data
	return nil
}

func (s *Store) saveUnsafe() error {
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Compact JSON serialization eliminates indentation overhead and cuts disk I/O by ~50%
	raw, err := json.Marshal(s.data)
	if err != nil {
		return fmt.Errorf("failed to encode store data: %w", err)
	}

	tmpFile := s.filePath + ".tmp"
	f, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create tmp file: %w", err)
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		_ = os.Remove(tmpFile)
		return fmt.Errorf("failed to write tmp file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmpFile)
		return fmt.Errorf("failed to fsync tmp file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("failed to close tmp file: %w", err)
	}

	if err := os.Rename(tmpFile, s.filePath); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("failed to atomic rename tmp file: %w", err)
	}

	return nil
}

// GetCIDRs returns all configured CIDRs.
func (s *Store) GetCIDRs() []CIDRConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]CIDRConfig, len(s.data.CIDRs))
	copy(res, s.data.CIDRs)
	return res
}

// AddOrUpdateCIDR adds or updates a CIDR block.
func (s *Store) AddOrUpdateCIDR(c CIDRConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var found bool
	for i, existing := range s.data.CIDRs {
		if existing.CIDR == c.CIDR {
			s.data.CIDRs[i] = c
			found = true
			break
		}
	}
	if !found {
		if c.CreatedAt.IsZero() {
			c.CreatedAt = time.Now()
		}
		s.data.CIDRs = append(s.data.CIDRs, c)
	}

	return s.saveUnsafe()
}

// DeleteCIDR deletes a CIDR configuration.
func (s *Store) DeleteCIDR(cidr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	newList := make([]CIDRConfig, 0, len(s.data.CIDRs))
	for _, c := range s.data.CIDRs {
		if c.CIDR != cidr {
			newList = append(newList, c)
		}
	}
	s.data.CIDRs = newList
	return s.saveUnsafe()
}

// GetExclusions returns all exclusions.
func (s *Store) GetExclusions() []ExclusionConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]ExclusionConfig, len(s.data.Exclusions))
	copy(res, s.data.Exclusions)
	return res
}

// AddOrUpdateExclusion adds or updates an exclusion rule.
func (s *Store) AddOrUpdateExclusion(e ExclusionConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var found bool
	for i, existing := range s.data.Exclusions {
		if existing.Rule == e.Rule {
			s.data.Exclusions[i] = e
			found = true
			break
		}
	}
	if !found {
		if e.CreatedAt.IsZero() {
			e.CreatedAt = time.Now()
		}
		s.data.Exclusions = append(s.data.Exclusions, e)
	}

	return s.saveUnsafe()
}

// DeleteExclusion deletes an exclusion rule.
func (s *Store) DeleteExclusion(rule string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	newList := make([]ExclusionConfig, 0, len(s.data.Exclusions))
	for _, e := range s.data.Exclusions {
		if e.Rule != rule {
			newList = append(newList, e)
		}
	}
	s.data.Exclusions = newList
	return s.saveUnsafe()
}

// GetDiscoveredHosts returns all discovered and static hosts.
func (s *Store) GetDiscoveredHosts() map[string]DiscoveredHost {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]DiscoveredHost, len(s.data.DiscoveredHosts))
	for ip, h := range s.data.DiscoveredHosts {
		res[ip] = h
	}
	return res
}

// AddOrUpdateDiscoveredHost saves or updates a discovered host entry.
func (s *Store) AddOrUpdateDiscoveredHost(h DiscoveredHost) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mergeDiscoveredHostUnsafe(h)
	return s.saveUnsafe()
}

// AddOrUpdateDiscoveredHostsBatch saves or updates multiple discovered host
// entries in a single atomic write. This avoids N separate JSON serializations
// and disk writes when processing large discovery sweeps.
func (s *Store) AddOrUpdateDiscoveredHostsBatch(hosts []DiscoveredHost) error {
	if len(hosts) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, h := range hosts {
		s.mergeDiscoveredHostUnsafe(h)
	}
	return s.saveUnsafe()
}

// mergeDiscoveredHostUnsafe applies the merge logic for a single discovered
// host without acquiring the lock or writing to disk. Caller must hold s.mu.
func (s *Store) mergeDiscoveredHostUnsafe(h DiscoveredHost) {
	if s.data.DiscoveredHosts == nil {
		s.data.DiscoveredHosts = make(map[string]DiscoveredHost)
	}

	if existing, exists := s.data.DiscoveredHosts[h.IP]; exists {
		h.DiscoveredAt = existing.DiscoveredAt
		if !h.IsStatic && existing.IsStatic {
			h.IsStatic = true
		}
	} else if h.DiscoveredAt.IsZero() {
		h.DiscoveredAt = time.Now()
	}
	h.LastDiscovered = time.Now()

	s.data.DiscoveredHosts[h.IP] = h
}


// RemoveDiscoveredHost removes an IP from the discovered/monitored set.
func (s *Store) RemoveDiscoveredHost(ip string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.data.DiscoveredHosts != nil {
		delete(s.data.DiscoveredHosts, ip)
	}
	return s.saveUnsafe()
}

// PruneDiscoveredHosts removes discovered hosts that no longer belong to any configured enabled CIDR.
// Static hosts (IsStatic == true) are preserved.
func (s *Store) PruneDiscoveredHosts(validCIDRs map[string]bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.data.DiscoveredHosts == nil {
		return nil
	}

	changed := false
	for ip, h := range s.data.DiscoveredHosts {
		if !h.IsStatic && !validCIDRs[h.CIDR] {
			delete(s.data.DiscoveredHosts, ip)
			changed = true
		}
	}

	if changed {
		return s.saveUnsafe()
	}
	return nil
}

// GetHostMeta returns metadata for an IP.
func (s *Store) GetHostMeta(ip string) (HostMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.data.HostMeta[ip]
	return meta, ok
}

// GetAllHostMeta returns a snapshot copy of all host metadata.
func (s *Store) GetAllHostMeta() map[string]HostMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]HostMeta, len(s.data.HostMeta))
	for ip, m := range s.data.HostMeta {
		res[ip] = m
	}
	return res
}

// SetHostMeta sets alias or notes for an IP.
func (s *Store) SetHostMeta(meta HostMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta.UpdatedAt = time.Now()
	s.data.HostMeta[meta.IP] = meta
	return s.saveUnsafe()
}

// GetSettings returns application settings.
func (s *Store) GetSettings() AppSettings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data.Settings
}

// UpdateSettings updates application settings.
func (s *Store) UpdateSettings(cfg AppSettings) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Settings = cfg
	return s.saveUnsafe()
}
