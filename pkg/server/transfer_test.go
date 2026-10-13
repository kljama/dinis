package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"dinis/pkg/store"
)

func addStaticHosts(coord *Coordinator, ips ...string) {
	now := time.Now()
	for _, ip := range ips {
		_ = coord.store.AddOrUpdateDiscoveredHost(store.DiscoveredHost{
			IP: ip, CIDR: "10.0.0.0/24", DiscoveredAt: now, LastDiscovered: now, IsStatic: true,
		})
	}
	coord.RebuildTargetList()
}

func TestHostsExportCSV(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()
	addStaticHosts(coord, "10.0.0.10", "10.0.0.2")
	_ = coord.store.SetHostMeta(store.HostMeta{IP: "10.0.0.10", Alias: "Router Wien", Notes: "Größe; 2. Stock"})

	rec := serve(srv, http.MethodGet, "/api/hosts/export?delimiter=semicolon", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Header().Get("Content-Disposition"), "dinis-hosts.csv") {
		t.Fatalf("unexpected answer %d %v", rec.Code, rec.Header())
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "\xef\xbb\xbf") {
		t.Fatal("the export must start with a UTF-8 byte order mark")
	}
	lines := strings.Split(strings.TrimSpace(strings.TrimPrefix(body, "\xef\xbb\xbf")), "\n")
	if lines[0] != "ip;alias;notes;subnet;status;static;last_reply" {
		t.Fatalf("unexpected header:\n%s", body)
	}
	// Sorted by IP as a number (the default targets 1.1.1.1, 8.8.8.8 and 127.0.0.1 are also
	// in the list); a cell with the delimiter is quoted
	var ips []string
	row := map[string]string{}
	for _, l := range lines[1:] {
		ip := strings.SplitN(l, ";", 2)[0]
		ips = append(ips, ip)
		row[ip] = l
	}
	if strings.Join(ips, " ") != "1.1.1.1 8.8.8.8 10.0.0.2 10.0.0.10 127.0.0.1" {
		t.Fatalf("unexpected order %v", ips)
	}
	if !strings.HasPrefix(row["10.0.0.2"], "10.0.0.2;;;10.0.0.0/24;") ||
		!strings.HasPrefix(row["10.0.0.10"], `10.0.0.10;Router Wien;"Größe; 2. Stock";10.0.0.0/24;`) ||
		!strings.Contains(row["10.0.0.10"], ";true;") {
		t.Fatalf("unexpected rows:\n%s", body)
	}
}

func TestHostsImportCSV(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()
	addStaticHosts(coord, "10.0.0.1", "10.0.0.2")
	_ = coord.store.SetHostMeta(store.HostMeta{IP: "10.0.0.1", Alias: "Router", Notes: "Keller"})

	// Excel style: byte order mark, semicolons, header in other case, no notes column
	csvFile := "\xef\xbb\xbfIP;Alias;Subnet\r\n10.0.0.2;Switch;x\r\n10.0.0.1;Router;x\r\n10.9.9.9;Unknown;x\r\nbogus;y;x\r\n\r\n"
	var res HostImportResult
	rec := serve(srv, http.MethodPost, "/api/hosts/import?dryRun=true", csvFile)
	_ = json.Unmarshal(rec.Body.Bytes(), &res)
	if rec.Code != http.StatusOK || !res.DryRun || res.Rows != 4 || res.Updated != 1 || res.Unchanged != 1 ||
		res.UnknownN != 1 || res.Unknown[0] != "10.9.9.9" || res.InvalidN != 1 || !strings.Contains(res.Invalid[0], "line 5") {
		t.Fatalf("unexpected dry run %d %s", rec.Code, rec.Body.String())
	}
	if m, _ := coord.store.GetHostMeta("10.0.0.2"); m.Alias != "" {
		t.Fatalf("a dry run must not change anything, got alias %q", m.Alias)
	}

	rec = serve(srv, http.MethodPost, "/api/hosts/import", csvFile)
	_ = json.Unmarshal(rec.Body.Bytes(), &res)
	if rec.Code != http.StatusOK || res.DryRun || res.Updated != 1 {
		t.Fatalf("unexpected import %d %s", rec.Code, rec.Body.String())
	}
	if m, _ := coord.store.GetHostMeta("10.0.0.2"); m.Alias != "Switch" {
		t.Fatalf("expected alias Switch, got %q", m.Alias)
	}
	if m, _ := coord.store.GetHostMeta("10.0.0.1"); m.Notes != "Keller" {
		t.Fatalf("a missing notes column must keep the notes, got %q", m.Notes)
	}
	h, _ := coord.pinger.GetHost("10.0.0.2")
	if h.Alias != "Switch" {
		t.Fatalf("the monitored host must show the new alias, got %q", h.Alias)
	}

	// An empty cell clears the value; a missing alias column keeps the alias
	rec = serve(srv, http.MethodPost, "/api/hosts/import", "ip,notes\n10.0.0.1,\n")
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected import %d %s", rec.Code, rec.Body.String())
	}
	if m, _ := coord.store.GetHostMeta("10.0.0.1"); m.Alias != "Router" || m.Notes != "" {
		t.Fatalf("expected alias Router and no notes, got %+v", m)
	}

	// Without an alias or notes column, the file is refused
	if rec := serve(srv, http.MethodPost, "/api/hosts/import", "ip,subnet\n10.0.0.1,x\n"); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d %s", rec.Code, rec.Body.String())
	}
	if rec := serve(srv, http.MethodGet, "/api/hosts/import", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestConfigExportPreviewApply(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	_ = coord.store.ReplaceConfig(store.ConfigReplacement{ // without the default targets
		CIDRs: []store.CIDRConfig{
			{CIDR: "10.0.0.0/24", Description: "LAN", Enabled: true, CreatedAt: created},
			{CIDR: "192.168.1.10/32", Description: "Printer", Enabled: true, CreatedAt: created},
		},
		Exclusions: []store.ExclusionConfig{{Rule: "10.0.0.5", Reason: "lab", Enabled: true, CreatedAt: created}},
		Settings:   coord.store.GetSettings(),
	})
	coord.RebuildTargetList()
	if h, ok := coord.store.GetDiscoveredHost("192.168.1.10"); !ok || !h.IsStatic {
		t.Fatal("expected a static host for the single-IP CIDR")
	}

	// Export
	rec := serve(srv, http.MethodGet, "/api/config", "")
	var file map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &file); err != nil || rec.Code != http.StatusOK ||
		!strings.Contains(rec.Header().Get("Content-Disposition"), "dinis-config.json") {
		t.Fatalf("unexpected export %d %s", rec.Code, rec.Body.String())
	}
	if file["version"] != float64(1) || len(file["cidrs"].([]any)) != 2 || len(file["exclusions"].([]any)) != 1 ||
		file["settings"].(map[string]any)["intervalSec"] != float64(60) {
		t.Fatalf("unexpected export %s", rec.Body.String())
	}

	// An unchanged export changes nothing
	var ch ConfigChanges
	rec = serve(srv, http.MethodPost, "/api/config/preview", rec.Body.String())
	_ = json.Unmarshal(rec.Body.Bytes(), &ch)
	if rec.Code != http.StatusOK || !ch.Valid || ch.Total != 0 {
		t.Fatalf("expected no changes, got %d %s", rec.Code, rec.Body.String())
	}

	// Edited file: one CIDR changed, one removed, one added; exclusion reason changed; one setting
	edited := `{"version": 1,
		"settings": {"intervalSec": 30},
		"cidrs": [
			{"cidr": "10.0.0.0/24", "description": "Office", "enabled": true},
			{"cidr": "10.1.0.1/30", "description": "New", "enabled": false}
		],
		"exclusions": [{"rule": "10.0.0.5", "reason": "spare", "enabled": true}]}`
	rec = serve(srv, http.MethodPost, "/api/config/preview", edited)
	ch = ConfigChanges{}
	_ = json.Unmarshal(rec.Body.Bytes(), &ch)
	if rec.Code != http.StatusOK || !ch.Valid || ch.Total != 5 ||
		strings.Join(ch.CIDRs.Added, ",") != "10.1.0.0/30" ||
		strings.Join(ch.CIDRs.Removed, ",") != "192.168.1.10/32" ||
		strings.Join(ch.CIDRs.Changed, ",") != `10.0.0.0/24: description "LAN" → "Office"` ||
		strings.Join(ch.Exclusions.Changed, ",") != `10.0.0.5: reason "lab" → "spare"` ||
		strings.Join(ch.Settings, ",") != "intervalSec: 60 → 30" {
		t.Fatalf("unexpected preview %d %s", rec.Code, rec.Body.String())
	}
	if len(coord.store.GetCIDRs()) != 2 || coord.store.GetSettings().IntervalSec != 60 {
		t.Fatal("a preview must not change anything")
	}

	// Apply
	rec = serve(srv, http.MethodPut, "/api/config", edited)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected apply %d %s", rec.Code, rec.Body.String())
	}
	cidrs := map[string]store.CIDRConfig{}
	for _, c := range coord.store.GetCIDRs() {
		cidrs[c.CIDR] = c
	}
	if len(cidrs) != 2 || cidrs["10.0.0.0/24"].Description != "Office" || !cidrs["10.0.0.0/24"].CreatedAt.Equal(created) ||
		cidrs["10.1.0.0/30"].CreatedAt.IsZero() {
		t.Fatalf("unexpected CIDRs %+v", cidrs)
	}
	if _, ok := coord.store.GetDiscoveredHost("192.168.1.10"); ok {
		t.Fatal("the static host of the removed single-IP CIDR must be removed")
	}
	if _, ok := coord.pinger.GetHost("192.168.1.10"); ok {
		t.Fatal("the removed single-IP host must not be monitored")
	}
	if s := coord.store.GetSettings(); s.IntervalSec != 30 || s.TimeoutMs != 1000 {
		t.Fatalf("expected intervalSec 30 and the other settings kept, got %+v", s)
	}
	if ex := coord.store.GetExclusions(); len(ex) != 1 || ex[0].Reason != "spare" || !ex[0].CreatedAt.Equal(created) {
		t.Fatalf("unexpected exclusions %+v", ex)
	}
}

func TestConfigImportRefusesInvalidFiles(t *testing.T) {
	srv, coord, cleanup := setupTestServer(t)
	defer cleanup()
	before := coord.store.GetCIDRs()

	for name, tc := range map[string]struct{ body, wantErr string }{
		"bad CIDR":        {`{"version":1,"cidrs":[{"cidr":"300.1.1.0/24"}],"exclusions":[]}`, "cidrs[0]: invalid CIDR"},
		"duplicate CIDR":  {`{"version":1,"cidrs":[{"cidr":"10.2.0.0/24"},{"cidr":"10.2.0.1/24"}],"exclusions":[]}`, "more than once"},
		"bad exclusion":   {`{"version":1,"cidrs":[],"exclusions":[{"rule":"nonsense"}]}`, "exclusions[0]"},
		"no exclusions":   {`{"version":1,"cidrs":[]}`, `must contain`},
		"wrong version":   {`{"version":2,"cidrs":[],"exclusions":[]}`, "unsupported file version 2"},
		"bad setting":     {`{"version":1,"settings":{"intervalSec":"fast"},"cidrs":[],"exclusions":[]}`, "settings:"},
		"not a JSON file": {`ip,alias`, "invalid configuration file"},
	} {
		rec := serve(srv, http.MethodPut, "/api/config", tc.body)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), tc.wantErr) {
			t.Errorf("%s: expected 400 with %q, got %d %s", name, tc.wantErr, rec.Code, rec.Body.String())
		}
	}
	if cidrs := coord.store.GetCIDRs(); len(cidrs) != len(before) || len(cidrs) == 0 {
		t.Fatalf("an invalid file must not change anything, got %+v", cidrs)
	}
}
