package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rapidloop/pgmetrics"
)

// log timestamps carry a zone abbreviation of log_timezone (MSK, CET, -03); they
// must be read right whatever the time zone of the pgmetrics process
func TestLogTimezone(t *testing.T) {
	local := time.Local
	defer func() { time.Local = local }()
	time.Local = time.UTC
	now := time.Now().Add(-time.Minute).Truncate(time.Millisecond)

	for _, tz := range []string{"Europe/Moscow", "America/Sao_Paulo", "Europe/Berlin", "Asia/Kolkata", "UTC"} {
		loc, err := time.LoadLocation(tz)
		if err != nil {
			t.Fatal(err)
		}
		c := &collector{logSpan: 10, result: pgmetrics.Model{
			Settings: map[string]pgmetrics.Setting{"log_timezone": {Setting: tz}},
		}}
		for layout, want := range map[string]time.Time{
			"2006-01-02 15:04:05.000 MST": now,                       // %m
			"2006-01-02 15:04:05 MST":     now.Truncate(time.Second), // %t
		} {
			s := now.In(loc).Format(layout)
			if got, err := parseLogTime(layout, s, c.logLocation()); err != nil || !got.Equal(want) {
				t.Errorf("%s: %q read as %v (%v), want %v", tz, s, got, err, want)
			}
		}

		// csvlog: the entry is inside the log span and keeps its time
		f := make([]string, 23)
		f[0], f[11], f[13] = now.In(loc).Format("2006-01-02 15:04:05.000 MST"), "LOG", "checkpoint starting: time"
		path := filepath.Join(t.TempDir(), "postgresql.csv")
		if err := os.WriteFile(path, []byte(strings.Join(f, ",")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := c.readLogLinesCSV(path); err != nil || len(c.result.LogEntries) != 1 || c.result.LogEntries[0].At != now.Unix() {
			t.Errorf("%s: csvlog entries %+v (%v), want one at %d", tz, c.result.LogEntries, err, now.Unix())
		}
	}
}
