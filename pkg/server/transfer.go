package server

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"dinis/pkg/network"
	"dinis/pkg/store"
)

// ---------------------------------------------------------------------------------------------
// Hosts as CSV
// ---------------------------------------------------------------------------------------------

// hostCSVColumns are the columns of the host export. The import reads ip, alias and notes and
// ignores the other columns, so an exported file can be edited and imported again.
var hostCSVColumns = []string{"ip", "alias", "notes", "subnet", "status", "static", "last_reply"}

// maxImportProblems limits the lists of unknown and invalid rows in an import result.
const maxImportProblems = 100

// handleHostsExport writes all monitored hosts as CSV. ?delimiter=semicolon writes semicolons
// (Excel with a comma as decimal separator expects them).
func (s *Server) handleHostsExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	sep := ','
	if d := r.URL.Query().Get("delimiter"); d == "semicolon" || d == ";" {
		sep = ';'
	}

	hosts := s.coord.pinger.GetAllHostsLite()
	sort.Slice(hosts, func(i, j int) bool {
		a, b := ipToUint32(hosts[i].IP), ipToUint32(hosts[j].IP)
		if a != b {
			return a < b
		}
		return hosts[i].IP < hosts[j].IP
	})
	meta := s.coord.store.GetAllHostMeta()

	var buf bytes.Buffer
	buf.WriteString("\xef\xbb\xbf") // UTF-8 byte order mark: Excel then reads umlauts correctly
	cw := csv.NewWriter(&buf)
	cw.Comma = sep
	_ = cw.Write(hostCSVColumns)
	for _, h := range hosts {
		lastReply := ""
		if h.LastSeen != nil {
			lastReply = h.LastSeen.UTC().Format(time.RFC3339)
		}
		m := meta[h.IP] // the alias of the host itself, not the CIDR description shown in its place
		_ = cw.Write([]string{h.IP, m.Alias, m.Notes, h.CIDR, string(h.Status), fmt.Sprint(h.IsStatic), lastReply})
	}
	cw.Flush()

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="dinis-hosts.csv"`)
	_, _ = w.Write(buf.Bytes())
}

// HostImportResult is the answer of the host import.
type HostImportResult struct {
	DryRun    bool     `json:"dryRun"`
	Rows      int      `json:"rows"`
	Updated   int      `json:"updated"`
	Unchanged int      `json:"unchanged"`
	Unknown   []string `json:"unknown"` // IPs that DINIS does not monitor (first 100)
	Invalid   []string `json:"invalid"` // rows that DINIS cannot read (first 100)
	UnknownN  int      `json:"unknownCount"`
	InvalidN  int      `json:"invalidCount"`
}

// handleHostsImport sets the alias and notes of monitored hosts from a CSV file. The file needs
// an ip column and an alias column, a notes column, or both. A missing column keeps the current
// value; an empty cell clears it. ?dryRun=true only reports what would change.
func (s *Server) handleHostsImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeDecodeError(w, err)
		return
	}
	res, metas, err := s.coord.planHostImport(data)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	res.DryRun = r.URL.Query().Get("dryRun") == "true"
	if !res.DryRun && len(metas) > 0 {
		if err := s.coord.ImportHostMeta(metas); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, res)
}

// planHostImport reads a host CSV and returns the result and the metadata to save.
func (c *Coordinator) planHostImport(data []byte) (HostImportResult, []store.HostMeta, error) {
	res := HostImportResult{Unknown: []string{}, Invalid: []string{}}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))

	cr := csv.NewReader(bytes.NewReader(data))
	cr.Comma = detectDelimiter(data)
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true
	header, err := cr.Read()
	if err != nil {
		return res, nil, fmt.Errorf("cannot read the CSV header: %v", err)
	}
	col := map[string]int{}
	for i, name := range header {
		col[strings.ToLower(strings.TrimSpace(name))] = i
	}
	ipCol, hasIP := col["ip"]
	aliasCol, hasAlias := col["alias"]
	notesCol, hasNotes := col["notes"]
	if !hasIP || (!hasAlias && !hasNotes) {
		return res, nil, errors.New("the CSV file needs an ip column and an alias column, a notes column, or both")
	}

	monitored := c.pinger.GetStatusMap()
	current := c.store.GetAllHostMeta()
	planned := map[string]store.HostMeta{} // last row of an IP wins
	var order []string
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		line, _ := cr.FieldPos(0)
		if err != nil {
			res.InvalidN++
			addProblem(&res.Invalid, fmt.Sprintf("line %d: %v", line, err))
			continue
		}
		if len(rec) == 1 && strings.TrimSpace(rec[0]) == "" {
			continue // empty line
		}
		res.Rows++
		cell := func(i int) string {
			if i < len(rec) {
				return strings.TrimSpace(rec[i])
			}
			return ""
		}
		parsed := net.ParseIP(cell(ipCol))
		if parsed == nil {
			res.InvalidN++
			addProblem(&res.Invalid, fmt.Sprintf("line %d: invalid IP address %q", line, cell(ipCol)))
			continue
		}
		ip := parsed.String()
		if _, ok := monitored[ip]; !ok {
			res.UnknownN++
			addProblem(&res.Unknown, ip)
			continue
		}
		m, seen := planned[ip]
		if !seen {
			m = store.HostMeta{IP: ip, Alias: current[ip].Alias, Notes: current[ip].Notes}
			order = append(order, ip)
		}
		if hasAlias {
			m.Alias = cell(aliasCol)
		}
		if hasNotes {
			m.Notes = cell(notesCol)
		}
		planned[ip] = m
	}

	var metas []store.HostMeta
	for _, ip := range order {
		m := planned[ip]
		if cur := current[ip]; cur.Alias == m.Alias && cur.Notes == m.Notes {
			res.Unchanged++
			continue
		}
		metas = append(metas, m)
	}
	res.Updated = len(metas)
	return res, metas, nil
}

func addProblem(list *[]string, s string) {
	if len(*list) < maxImportProblems {
		*list = append(*list, s)
	}
}

// detectDelimiter returns ';' if the header line has more semicolons than commas, else ','.
func detectDelimiter(data []byte) rune {
	first := data
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		first = data[:i]
	}
	if bytes.Count(first, []byte{';'}) > bytes.Count(first, []byte{','}) {
		return ';'
	}
	return ','
}

// ---------------------------------------------------------------------------------------------
// Configuration file
// ---------------------------------------------------------------------------------------------

// configFileVersion is the format version of the configuration file.
const configFileVersion = 1

// ConfigFile is the configuration of DINIS as a file: the CIDRs, the exclusion rules and the
// settings. Discovered hosts, host metadata and alerts are not part of it.
type ConfigFile struct {
	Version    int                      `json:"version"`
	ExportedAt *time.Time               `json:"exportedAt,omitempty"`
	Settings   json.RawMessage          `json:"settings,omitempty"`
	CIDRs      *[]store.CIDRConfig      `json:"cidrs"`
	Exclusions *[]store.ExclusionConfig `json:"exclusions"`
}

// ConfigChanges lists the differences between the current configuration and a file.
type ConfigChanges struct {
	Valid      bool         `json:"valid"`
	Errors     []string     `json:"errors"`
	CIDRs      EntryChanges `json:"cidrs"`
	Exclusions EntryChanges `json:"exclusions"`
	Settings   []string     `json:"settings"` // "intervalSec: 60 → 30"
	Total      int          `json:"total"`
}

// EntryChanges lists added, removed and changed entries of a list.
type EntryChanges struct {
	Added   []string `json:"added"`
	Removed []string `json:"removed"`
	Changed []string `json:"changed"`
}

func (e EntryChanges) count() int { return len(e.Added) + len(e.Removed) + len(e.Changed) }

// configPlan is a validated configuration file, ready to apply.
type configPlan struct {
	changes     ConfigChanges
	replacement store.ConfigReplacement
	addedCIDRs  []string
}

// handleConfig exports (GET) or replaces (PUT, POST) the configuration.
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		now := time.Now().UTC()
		settings, _ := json.Marshal(s.coord.store.GetSettings())
		cidrs := s.coord.store.GetCIDRs()
		excls := s.coord.store.GetExclusions()
		file := ConfigFile{Version: configFileVersion, ExportedAt: &now, Settings: settings, CIDRs: &cidrs, Exclusions: &excls}
		body, err := json.MarshalIndent(file, "", "  ")
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", `attachment; filename="dinis-config.json"`)
		_, _ = w.Write(append(body, '\n'))

	case http.MethodPut, http.MethodPost:
		plan, status, err := s.readConfigPlan(r)
		if err != nil {
			writeError(w, status, err.Error())
			return
		}
		if !plan.changes.Valid {
			writeJSON(w, http.StatusBadRequest, plan.changes)
			return
		}
		if err := s.coord.ApplyConfig(plan); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, plan.changes)

	default:
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// handleConfigPreview reports what a configuration file would change, without changing anything.
func (s *Server) handleConfigPreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	plan, status, err := s.readConfigPlan(r)
	if err != nil {
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, plan.changes)
}

func (s *Server) readConfigPlan(r *http.Request) (configPlan, int, error) {
	var file ConfigFile
	if err := decodeJSON(r, &file); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return configPlan{}, http.StatusRequestEntityTooLarge, fmt.Errorf("the file exceeds %d bytes", maxImportBodyBytes)
		}
		return configPlan{}, http.StatusBadRequest, fmt.Errorf("invalid configuration file: %v", err)
	}
	return s.coord.planConfig(file), http.StatusOK, nil
}

// planConfig validates a configuration file and compares it with the current configuration.
// The file replaces the CIDRs and the exclusion rules completely. Settings that the file does
// not contain keep their current values.
func (c *Coordinator) planConfig(file ConfigFile) configPlan {
	ch := ConfigChanges{
		Errors:     []string{},
		CIDRs:      EntryChanges{Added: []string{}, Removed: []string{}, Changed: []string{}},
		Exclusions: EntryChanges{Added: []string{}, Removed: []string{}, Changed: []string{}},
		Settings:   []string{},
	}
	plan := configPlan{}
	fail := func(format string, a ...any) { ch.Errors = append(ch.Errors, fmt.Sprintf(format, a...)) }

	if file.Version != configFileVersion {
		fail("unsupported file version %d (expected %d)", file.Version, configFileVersion)
	}
	if file.CIDRs == nil || file.Exclusions == nil {
		fail("the file must contain \"cidrs\" and \"exclusions\" (empty lists are allowed)")
	}

	now := time.Now()

	// Settings: decode onto the current values, then bring them into range
	curSettings := c.store.GetSettings()
	newSettings := curSettings
	if len(file.Settings) > 0 && string(file.Settings) != "null" {
		if err := json.Unmarshal(file.Settings, &newSettings); err != nil {
			fail("settings: %v", err)
		}
	}
	newSettings = normalizeSettings(newSettings)
	ch.Settings = settingsDiff(curSettings, newSettings)

	// CIDRs
	curCIDRs := map[string]store.CIDRConfig{}
	for _, cfg := range c.store.GetCIDRs() {
		curCIDRs[cfg.CIDR] = cfg
	}
	var newCIDRs []store.CIDRConfig
	seen := map[string]bool{}
	if file.CIDRs != nil {
		for i, cfg := range *file.CIDRs {
			info, err := network.ParseCIDR(cfg.CIDR, cfg.IncludeNetAndBcast)
			if err != nil {
				fail("cidrs[%d]: invalid CIDR %q: %v", i, cfg.CIDR, err)
				continue
			}
			cfg.CIDR = info.CIDR
			if seen[cfg.CIDR] {
				fail("cidrs[%d]: %s is in the file more than once", i, cfg.CIDR)
				continue
			}
			seen[cfg.CIDR] = true
			cfg.IntervalSec = clampCIDRInterval(cfg.IntervalSec)
			if cur, ok := curCIDRs[cfg.CIDR]; ok {
				cfg.CreatedAt = cur.CreatedAt
				if diff := cidrDiff(cur, cfg); diff != "" {
					ch.CIDRs.Changed = append(ch.CIDRs.Changed, cfg.CIDR+": "+diff)
				}
			} else {
				if cfg.CreatedAt.IsZero() {
					cfg.CreatedAt = now
				}
				ch.CIDRs.Added = append(ch.CIDRs.Added, cfg.CIDR)
				if cfg.Enabled {
					plan.addedCIDRs = append(plan.addedCIDRs, cfg.CIDR)
				}
			}
			newCIDRs = append(newCIDRs, cfg)
		}
	}
	for cidr, cur := range curCIDRs {
		if seen[cidr] {
			continue
		}
		ch.CIDRs.Removed = append(ch.CIDRs.Removed, cidr)
		// A removed single-IP CIDR also removes its static host entry (as DeleteCIDR does)
		if info, err := network.ParseCIDR(cur.CIDR, false); err == nil && info.TotalHosts == 1 {
			if h, ok := c.store.GetDiscoveredHost(info.IPs[0]); ok && h.IsStatic {
				plan.replacement.RemoveHosts = append(plan.replacement.RemoveHosts, info.IPs[0])
			}
		}
	}

	// Exclusion rules
	curExcl := map[string]store.ExclusionConfig{}
	for _, e := range c.store.GetExclusions() {
		curExcl[e.Rule] = e
	}
	var newExcl []store.ExclusionConfig
	seenRule := map[string]bool{}
	if file.Exclusions != nil {
		for i, e := range *file.Exclusions {
			e.Rule = strings.TrimSpace(e.Rule)
			if e.Rule == "" {
				fail("exclusions[%d]: the rule is empty", i)
				continue
			}
			if err := network.NewExclusionMatcher().AddExclusion(e.Rule, e.Reason); err != nil {
				fail("exclusions[%d]: %v", i, err)
				continue
			}
			if seenRule[e.Rule] {
				fail("exclusions[%d]: %s is in the file more than once", i, e.Rule)
				continue
			}
			seenRule[e.Rule] = true
			if cur, ok := curExcl[e.Rule]; ok {
				e.CreatedAt = cur.CreatedAt
				if diff := exclusionDiff(cur, e); diff != "" {
					ch.Exclusions.Changed = append(ch.Exclusions.Changed, e.Rule+": "+diff)
				}
			} else {
				if e.CreatedAt.IsZero() {
					e.CreatedAt = now
				}
				ch.Exclusions.Added = append(ch.Exclusions.Added, e.Rule)
			}
			newExcl = append(newExcl, e)
		}
	}
	for rule := range curExcl {
		if !seenRule[rule] {
			ch.Exclusions.Removed = append(ch.Exclusions.Removed, rule)
		}
	}

	for _, l := range [][]string{ch.CIDRs.Added, ch.CIDRs.Removed, ch.CIDRs.Changed, ch.Exclusions.Added, ch.Exclusions.Removed, ch.Exclusions.Changed} {
		sort.Strings(l)
	}
	ch.Total = ch.CIDRs.count() + ch.Exclusions.count() + len(ch.Settings)
	ch.Valid = len(ch.Errors) == 0
	plan.changes = ch
	plan.replacement.CIDRs = newCIDRs
	plan.replacement.Exclusions = newExcl
	plan.replacement.Settings = newSettings
	return plan
}

// ApplyConfig replaces the configuration with a validated plan and applies it: the engine
// settings, the target list, and a discovery sweep of new CIDRs (if autoDiscovery is on).
func (c *Coordinator) ApplyConfig(plan configPlan) error {
	previousMaxHosts := c.store.GetSettings().MaxMetricHosts
	if err := c.store.ReplaceConfig(plan.replacement); err != nil {
		return err
	}
	c.applyEngineSettings(plan.replacement.Settings, previousMaxHosts)
	c.RebuildTargetList()
	if plan.replacement.Settings.AutoDiscovery {
		for _, cidr := range plan.addedCIDRs {
			c.TriggerDiscovery(cidr)
		}
	}
	log.Printf("[DINIS] Configuration replaced from file: %d changes", plan.changes.Total)
	return nil
}

func clampCIDRInterval(iv float64) float64 {
	switch {
	case iv < 0:
		return 0
	case iv > 0 && iv < 0.5:
		return 0.5
	case iv > 3600:
		return 3600
	}
	return iv
}

func cidrDiff(a, b store.CIDRConfig) string {
	var d []string
	if a.Description != b.Description {
		d = append(d, fmt.Sprintf("description %q → %q", a.Description, b.Description))
	}
	if a.Enabled != b.Enabled {
		d = append(d, fmt.Sprintf("enabled %v → %v", a.Enabled, b.Enabled))
	}
	if a.IncludeNetAndBcast != b.IncludeNetAndBcast {
		d = append(d, fmt.Sprintf("includeNetAndBcast %v → %v", a.IncludeNetAndBcast, b.IncludeNetAndBcast))
	}
	if a.IntervalSec != b.IntervalSec {
		d = append(d, fmt.Sprintf("intervalSec %g → %g", a.IntervalSec, b.IntervalSec))
	}
	return strings.Join(d, ", ")
}

func exclusionDiff(a, b store.ExclusionConfig) string {
	var d []string
	if a.Reason != b.Reason {
		d = append(d, fmt.Sprintf("reason %q → %q", a.Reason, b.Reason))
	}
	if a.Enabled != b.Enabled {
		d = append(d, fmt.Sprintf("enabled %v → %v", a.Enabled, b.Enabled))
	}
	return strings.Join(d, ", ")
}

// settingsDiff lists the settings that differ, as "name: old → new", sorted by name.
func settingsDiff(a, b store.AppSettings) []string {
	var ma, mb map[string]any
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	_ = json.Unmarshal(ja, &ma)
	_ = json.Unmarshal(jb, &mb)
	keys := make([]string, 0, len(mb))
	for k := range mb {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []string{}
	for _, k := range keys {
		if fmt.Sprint(ma[k]) != fmt.Sprint(mb[k]) {
			out = append(out, fmt.Sprintf("%s: %v → %v", k, ma[k], mb[k]))
		}
	}
	return out
}
