package store

import (
	"os"
	"path/filepath"
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
	s2, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to reload store: %v", err)
	}

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


