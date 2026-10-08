package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dinis-store-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "data.json")
	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// 1. Verify default CIDRs
	cidrs := s.GetCIDRs()
	if len(cidrs) == 0 {
		t.Fatalf("expected default CIDRs, got none")
	}

	// 2. Add custom CIDR
	err = s.AddOrUpdateCIDR(CIDRConfig{
		CIDR:        "10.0.0.0/24",
		Description: "Office LAN",
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("failed to add CIDR: %v", err)
	}

	// 3. Add exclusion
	err = s.AddOrUpdateExclusion(ExclusionConfig{
		Rule:    "10.0.0.1",
		Reason:  "Default Gateway",
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("failed to add exclusion: %v", err)
	}

	// 4. Reload from disk to verify persistence
	_ = s.Close()
	s2, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to reload store: %v", err)
	}
	defer s2.Close()

	var foundCIDR, foundExcl bool
	for _, c := range s2.GetCIDRs() {
		if c.CIDR == "10.0.0.0/24" {
			foundCIDR = true
		}
	}
	for _, e := range s2.GetExclusions() {
		if e.Rule == "10.0.0.1" {
			foundExcl = true
		}
	}

	if !foundCIDR || !foundExcl {
		t.Fatalf("reloaded store missing data: cidr=%v, excl=%v", foundCIDR, foundExcl)
	}
}

func TestPruneDiscoveredHostsPreservesStatic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dinis-store-prune-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "data.json")
	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Add dynamic host in 192.168.1.0/24
	_ = s.AddOrUpdateDiscoveredHost(DiscoveredHost{
		IP:       "192.168.1.50",
		CIDR:     "192.168.1.0/24",
		IsStatic: false,
	})

	// Add dynamic host in old CIDR 172.16.0.0/24
	_ = s.AddOrUpdateDiscoveredHost(DiscoveredHost{
		IP:       "172.16.0.20",
		CIDR:     "172.16.0.0/24",
		IsStatic: false,
	})

	// Add static host promoted with CIDR "Static" or standalone IP
	_ = s.AddOrUpdateDiscoveredHost(DiscoveredHost{
		IP:       "8.8.8.8",
		CIDR:     "Static",
		IsStatic: true,
	})

	// Add static host with /32 CIDR
	_ = s.AddOrUpdateDiscoveredHost(DiscoveredHost{
		IP:       "1.1.1.1",
		CIDR:     "1.1.1.1/32",
		IsStatic: true,
	})

	// Valid CIDRs now only includes 192.168.1.0/24
	validCIDRs := map[string]bool{
		"192.168.1.0/24": true,
	}

	if err := s.PruneDiscoveredHosts(validCIDRs); err != nil {
		t.Fatalf("failed to prune discovered hosts: %v", err)
	}

	hosts := s.GetDiscoveredHosts()

	// 192.168.1.50 (valid CIDR, dynamic) should remain
	if _, ok := hosts["192.168.1.50"]; !ok {
		t.Errorf("expected 192.168.1.50 to remain")
	}

	// 172.16.0.20 (invalid CIDR, dynamic) should be pruned
	if _, ok := hosts["172.16.0.20"]; ok {
		t.Errorf("expected 172.16.0.20 to be pruned")
	}

	// 8.8.8.8 (Static, not in validCIDRs) MUST NOT be pruned
	if _, ok := hosts["8.8.8.8"]; !ok {
		t.Errorf("expected static host 8.8.8.8 (CIDR: Static) to be preserved")
	}

	// 1.1.1.1 (Static, not in validCIDRs) MUST NOT be pruned
	if _, ok := hosts["1.1.1.1"]; !ok {
		t.Errorf("expected static host 1.1.1.1 to be preserved")
	}
}

func TestStoreNullJSONDeserialization(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dinis_null_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dataPath := filepath.Join(tmpDir, "null_dinis.json")
	nullJSON := `{
		"cidrs": null,
		"exclusions": null,
		"hostMeta": null,
		"discoveredHosts": null,
		"settings": {
			"intervalSec": 60,
			"timeoutMs": 1000
		}
	}`

	if err := os.WriteFile(dataPath, []byte(nullJSON), 0644); err != nil {
		t.Fatalf("failed to write null json: %v", err)
	}

	st, err := NewStore(dataPath)
	if err != nil {
		t.Fatalf("failed to initialize store from null json: %v", err)
	}

	cidrs := st.GetCIDRs()
	if cidrs == nil {
		t.Errorf("expected non-nil CIDRs slice, got nil")
	}
	if len(cidrs) != 0 {
		t.Errorf("expected empty CIDRs slice, got len %d", len(cidrs))
	}

	exclusions := st.GetExclusions()
	if exclusions == nil {
		t.Errorf("expected non-nil Exclusions slice, got nil")
	}
	if len(exclusions) != 0 {
		t.Errorf("expected empty Exclusions slice, got len %d", len(exclusions))
	}

	hosts := st.GetDiscoveredHosts()
	if hosts == nil {
		t.Errorf("expected non-nil DiscoveredHosts map, got nil")
	}

	_, ok := st.GetHostMeta("192.168.1.1")
	if ok {
		t.Errorf("expected ok == false for non-existent HostMeta")
	}
}

func TestSaveUnsafeTmpFileCleanupOnRenameError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dinis_rename_fail_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "data.json")
	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Remove original file and create a non-empty directory with the same name,
	// causing os.Rename(tmpFile, dbPath) to fail.
	_ = os.Remove(dbPath)
	if err := os.Mkdir(dbPath, 0755); err != nil {
		t.Fatalf("failed to create directory in place of file: %v", err)
	}
	// Add a dummy file inside dbPath so it's a non-empty directory (ensures EISDIR / ENOTEMPTY on rename)
	_ = os.WriteFile(filepath.Join(dbPath, "dummy"), []byte("data"), 0644)

	// Attempt saveUnsafe, which will fail during Rename
	err = s.saveUnsafe()
	if err == nil {
		t.Fatalf("expected saveUnsafe to fail when renaming file onto a non-empty directory")
	}

	// Verify that the .tmp file was deleted and not left orphaned
	tmpFile := dbPath + ".tmp"
	if _, statErr := os.Stat(tmpFile); !os.IsNotExist(statErr) {
		t.Errorf("expected tmp file %s to be cleaned up, but it still exists (statErr=%v)", tmpFile, statErr)
	}
}

func TestDisabledCIDRDoesNotPruneDiscoveredHosts(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dinis_disabled_cidr_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "data.json")
	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Add CIDR 192.168.10.0/24 (disabled)
	_ = s.AddOrUpdateCIDR(CIDRConfig{
		CIDR:    "192.168.10.0/24",
		Enabled: false,
	})

	// Add discovered host under this CIDR
	_ = s.AddOrUpdateDiscoveredHost(DiscoveredHost{
		IP:       "192.168.10.5",
		CIDR:     "192.168.10.0/24",
		IsStatic: false,
	})

	// When allConfiguredCIDRs includes the disabled CIDR, PruneDiscoveredHosts MUST NOT delete the host
	allConfiguredCIDRs := map[string]bool{
		"192.168.10.0/24": true,
	}

	if err := s.PruneDiscoveredHosts(allConfiguredCIDRs); err != nil {
		t.Fatalf("prune failed: %v", err)
	}

	hosts := s.GetDiscoveredHosts()
	if _, exists := hosts["192.168.10.5"]; !exists {
		t.Errorf("expected discovered host in disabled CIDR to be preserved on disk")
	}

	// Now simulate deleting the CIDR from configuration (allConfiguredCIDRs empty)
	if err := s.PruneDiscoveredHosts(map[string]bool{}); err != nil {
		t.Fatalf("prune after delete failed: %v", err)
	}

	hostsAfterDelete := s.GetDiscoveredHosts()
	if _, exists := hostsAfterDelete["192.168.10.5"]; exists {
		t.Errorf("expected discovered host to be pruned after CIDR was deleted")
	}
}

func TestStoreFileLock(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dinis_file_lock_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "data.json")
	s1, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create initial store: %v", err)
	}
	defer s1.Close()

	// Attempting to open a second store on the same file path while s1 is open MUST fail
	s2, err := NewStore(dbPath)
	if err == nil {
		s2.Close()
		t.Fatalf("expected NewStore to fail due to file lock, but succeeded")
	}
	if !strings.Contains(err.Error(), "database is locked") {
		t.Errorf("expected 'database is locked' error, got: %v", err)
	}

	// Close s1; now a new store MUST succeed
	if err := s1.Close(); err != nil {
		t.Fatalf("failed to close s1: %v", err)
	}

	s3, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("expected NewStore to succeed after s1 was closed, got: %v", err)
	}
	_ = s3.Close()
}

func TestCIDRIntervalSec(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dinis-store-interval-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "data.json")
	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// 1. Add CIDR with custom interval
	err = s.AddOrUpdateCIDR(CIDRConfig{
		CIDR:        "10.50.0.0/24",
		Description: "Fast Poll Subnet",
		Enabled:     true,
		IntervalSec: 2.5,
	})
	if err != nil {
		t.Fatalf("failed to add CIDR: %v", err)
	}

	// 2. Add CIDR with clamped interval (> 3600 -> 3600)
	err = s.AddOrUpdateCIDR(CIDRConfig{
		CIDR:        "10.60.0.0/24",
		Description: "Slow Poll Subnet",
		Enabled:     true,
		IntervalSec: 99999,
	})
	if err != nil {
		t.Fatalf("failed to add CIDR: %v", err)
	}

	_ = s.Close()

	// 3. Reload and verify
	s2, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to reload store: %v", err)
	}
	defer s2.Close()

	for _, c := range s2.GetCIDRs() {
		if c.CIDR == "10.50.0.0/24" {
			if c.IntervalSec != 2.5 {
				t.Errorf("expected IntervalSec=2.5, got %f", c.IntervalSec)
			}
		}
		if c.CIDR == "10.60.0.0/24" {
			if c.IntervalSec != 3600 {
				t.Errorf("expected IntervalSec=3600, got %f", c.IntervalSec)
			}
		}
	}
}

func TestStoreSettingsPartialDefaults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dinis-store-settings-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "data.json")
	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	customSettings := AppSettings{
		DiscoveryIntervalMin: 120,
		IntervalSec:          15.0,
		TimeoutMs:            2500,
		FailThreshold:        5,
		Concurrency:          350,
		MaxMetricHosts:       25000,
		AutoDiscovery:        false,
	}
	if err := s.UpdateSettings(customSettings); err != nil {
		t.Fatalf("failed to update settings: %v", err)
	}
	_ = s.Close()

	// Manually corrupt only IntervalSec to 0 in the JSON file
	raw, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read store file: %v", err)
	}
	corrupted := strings.Replace(string(raw), `"intervalSec":15`, `"intervalSec":0`, 1)
	if err := os.WriteFile(dbPath, []byte(corrupted), 0644); err != nil {
		t.Fatalf("failed to write corrupted store file: %v", err)
	}

	// Reload store and verify custom settings are preserved, while IntervalSec reset to default
	s2, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to reload store: %v", err)
	}
	defer s2.Close()

	loaded := s2.GetSettings()
	if loaded.IntervalSec != 60.0 {
		t.Errorf("expected default IntervalSec 60.0, got %f", loaded.IntervalSec)
	}
	if loaded.TimeoutMs != 2500 {
		t.Errorf("expected TimeoutMs 2500 to be preserved, got %d", loaded.TimeoutMs)
	}
	if loaded.FailThreshold != 5 {
		t.Errorf("expected FailThreshold 5 to be preserved, got %d", loaded.FailThreshold)
	}
	if loaded.Concurrency != 350 {
		t.Errorf("expected Concurrency 350 to be preserved, got %d", loaded.Concurrency)
	}
	if loaded.MaxMetricHosts != 25000 {
		t.Errorf("expected MaxMetricHosts 25000 to be preserved, got %d", loaded.MaxMetricHosts)
	}
	if loaded.AutoDiscovery != false {
		t.Errorf("expected AutoDiscovery false to be preserved, got %v", loaded.AutoDiscovery)
	}
}

func TestLoadOlderFileGetsDefaultsForNewSettings(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dinis.json")
	old := `{"cidrs":[],"exclusions":[],"hostMeta":{},"discoveredHosts":{},` +
		`"settings":{"discoveryIntervalMin":60,"intervalSec":30,"timeoutMs":800,"failThreshold":3,"concurrency":50,"maxMetricHosts":2000,"autoDiscovery":false}}`
	if err := os.WriteFile(path, []byte(old), 0644); err != nil {
		t.Fatal(err)
	}

	st, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to load store: %v", err)
	}
	defer st.Close()

	s := st.GetSettings()
	if s.DownProbeIntervalSec != 300 {
		t.Errorf("expected missing downProbeIntervalSec to default to 300, got %d", s.DownProbeIntervalSec)
	}
	if s.IntervalSec != 30 || s.AutoDiscovery || s.MaxMetricHosts != 2000 {
		t.Errorf("expected stored settings to be kept, got %+v", s)
	}

	s.DownProbeIntervalSec = 0
	if err := st.UpdateSettings(s); err != nil {
		t.Fatal(err)
	}
	st.Close()
	st2, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to reload store: %v", err)
	}
	defer st2.Close()
	if got := st2.GetSettings().DownProbeIntervalSec; got != 0 {
		t.Errorf("expected an explicit 0 to be kept, got %d", got)
	}
}

func TestAlertStateFile(t *testing.T) {
	dir := t.TempDir()
	st, err := NewStore(filepath.Join(dir, "dinis.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if want := filepath.Join(dir, "dinis.alerts.json"); st.AlertStatePath() != want {
		t.Errorf("expected alert state path %s, got %s", want, st.AlertStatePath())
	}

	var v map[string]int
	if ok, err := st.LoadAlertState(&v); err != nil || ok {
		t.Fatalf("expected no saved state yet, got ok=%v err=%v", ok, err)
	}
	if err := st.SaveAlertState(map[string]int{"active": 2}); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	if ok, err := st.LoadAlertState(&v); err != nil || !ok || v["active"] != 2 {
		t.Fatalf("expected saved state back, got ok=%v err=%v v=%v", ok, err, v)
	}
}
