package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rapidloop/pgmetrics"
)

// csvlog records have 23 columns before pg13, 24 in pg13 (backend_type) and 26
// since pg14 (leader_pid, query_id); all of them must be read
func TestReadLogLinesCSV(t *testing.T) {
	ts := time.Now().UTC().Format("2006-01-02 15:04:05.000") + " UTC"
	row := func(cols int, msg string) string {
		f := make([]string, cols)
		f[0], f[1], f[2], f[11] = ts, "app", "shop", "LOG"
		f[13] = `"` + strings.ReplaceAll(msg, `"`, `""`) + `"`
		if cols >= 26 {
			f[25] = "42"
		}
		return strings.Join(f, ",") + "\n"
	}
	plan := "duration: 1.250 ms  plan:\n{\n  \"Query Text\": \"select 1\",\n  \"Plan\": {\"Node Type\": \"Result\"}\n}"
	av := "automatic vacuum of table \"shop.public.orders\": index scans: 0\n" +
		"system usage: CPU: user: 0.00 s, system: 0.00 s, elapsed: 0.05 s"

	for _, cols := range []int{23, 24, 26} {
		path := filepath.Join(t.TempDir(), "postgresql.csv")
		if err := os.WriteFile(path, []byte(row(cols, plan)+row(cols, av)), 0o600); err != nil {
			t.Fatal(err)
		}
		c := &collector{logSpan: 10}
		if err := c.readLogLinesCSV(path); err != nil {
			t.Fatalf("%d columns: %v", cols, err)
		}
		r := c.result
		if len(r.LogEntries) != 2 || len(r.Plans) != 1 || len(r.AutoVacuums) != 1 {
			t.Fatalf("%d columns: %d entries, %d plans, %d autovacuums, want 2, 1, 1",
				cols, len(r.LogEntries), len(r.Plans), len(r.AutoVacuums))
		}
		if p := r.Plans[0]; p.Format != "json" || p.Query != "select 1" || p.Database != "shop" {
			t.Fatalf("%d columns: plan %+v", cols, p)
		}
		if want := map[bool]int64{true: 42, false: 0}[cols >= 26]; r.Plans[0].QueryID != want {
			t.Fatalf("%d columns: query id %d, want %d", cols, r.Plans[0].QueryID, want)
		}
		if a := r.AutoVacuums[0]; a.Table != "shop.public.orders" || a.Elapsed != 0.05 {
			t.Fatalf("%d columns: autovacuum %+v", cols, a)
		}
	}

	// a file that is not csvlog is skipped without an error
	path := filepath.Join(t.TempDir(), "postgresql.log")
	if err := os.WriteFile(path, []byte(ts+" [123] LOG:  database system is ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := &collector{logSpan: 10}
	if err := c.readLogLinesCSV(path); err != nil || len(c.result.LogEntries) != 0 {
		t.Fatalf("text log: err %v, %d entries", err, len(c.result.LogEntries))
	}

	// csvlog records do not use log_line_prefix, so one without a timestamp must not stop them
	path = filepath.Join(t.TempDir(), "postgresql.csv")
	if err := os.WriteFile(path, []byte(row(26, av)), 0o600); err != nil {
		t.Fatal(err)
	}
	c = &collector{logSpan: 10, csvlog: true}
	c.result.Settings = map[string]pgmetrics.Setting{"log_line_prefix": {Setting: "[%p] "}}
	c.collectLogs(CollectConfig{LogFile: path})
	if len(c.result.AutoVacuums) != 1 {
		t.Fatalf("csvlog, log_line_prefix without timestamp: %d autovacuums, want 1", len(c.result.AutoVacuums))
	}
}
