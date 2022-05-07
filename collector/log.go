package collector

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rapidloop/pgmetrics"
)

var (
	rxLogLevel  = regexp.MustCompile(`^([A-Z]+):\s+`)
	rxAEStart   = regexp.MustCompile(`^duration: [0-9]+\.[0-9]+ ms  plan:\n[ \t]*({[ \t]*\n)?(<explain xml.*\n)?(Query Text: ".*"\n)?(Query Text: [^"].*\n)?`)
	rxAESwitch1 = regexp.MustCompile(`^\s+Query Text: (.*)$`)
	rxAESwitch2 = regexp.MustCompile(`cost=\d+.*rows=\d`)
	rxAVStart   = regexp.MustCompile(`automatic (aggressive )?vacuum (to prevent wraparound )?of table "([^"]+)": index`)
	rxAVElapsed = regexp.MustCompile(`, elapsed: ([0-9.]+) s`)
)

func (c *collector) readLogs(filenames []string) {
	for _, filename := range filenames {
		//log.Printf("debug: reading %s, csv=%v", filename, c.csvlog)
		if err := c.readLogLines(filename); err != nil {
			log.Printf("warning: while reading log file %s: %v", filename, err)
		}
	}
}

func (c *collector) readLogLines(filename string) error {
	if c.csvlog {
		return c.readLogLinesCSV(filename)
	}
	return c.readLogLinesText(filename)
}

func (c *collector) readLogLinesText(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// we're seeking to just before this
	window := time.Duration(c.logSpan) * time.Minute
	start := time.Now().Add(-window)

	// get current length of file
	flen, err := f.Seek(0, 2)
	if err != nil {
		return err
	}
	if flen <= 0 {
		return nil // empty file, nothing to do
	}
	//log.Printf("debug: file %s of length %d", filename, flen)

	// a buffer for reuse
	buf := make([]byte, 4096)

	// seek to flen-4k
	ofs := flen - 4096
	for {
		buflen := int64(4096)
		if ofs < 0 {
			buflen += ofs
			ofs = 0
		}
		if ofs, err = f.Seek(ofs, 0); err != nil {
			return err
		}
		//log.Printf("debug: seeked to %d", ofs)

		// read the last 4k of the file
		//log.Printf("debug: reading %d bytes", len(buf[0:buflen]))
		if _, err := io.ReadFull(f, buf[0:buflen]); err != nil {
			return err
		}
		ts, err := firstTS(buf[0:buflen], c.rxPrefix)
		if err != nil {
			return err
		}
		if ts.IsZero() {
			//log.Printf("debug: not found in block")
		} else {
			//log.Printf("debug: got first ts in block = %v", ts)
			if ts.Before(start) {
				//log.Printf("debug: got good ts %v before %v", ts, start)
				break
			}
		}
		// we need to seek backward
		if ofs == 0 {
			// reached the top, we need the whole file
			break
		}
		ofs -= 4096 // go back by 4k
	}

	// read the file from this position (ofs) into one big block
	if _, err := f.Seek(ofs, 0); err != nil {
		return err
	}
	bigbuf := make([]byte, flen-ofs)
	if _, err := io.ReadFull(f, bigbuf); err != nil {
		return err
	}

	return c.processLogBuf(start, bigbuf)
}

func (c *collector) processLogBuf(start time.Time, bigbuf []byte) (err error) {
	count := 0
	pos := c.rxPrefix.FindIndex(bigbuf)
	for len(pos) == 2 && len(bigbuf) > 0 {
		// match again for submatches, can't do this in one go :-(
		// TODO: no longer the case, use FindSubmatchIndex
		match := c.rxPrefix.FindSubmatch(bigbuf[pos[0]:])
		t, user, db, qid, err := getMatchData(match, c.rxPrefix)
		if err != nil {
			return nil
		}
		var line string
		// seek to start of next line
		pos2 := c.rxPrefix.FindIndex(bigbuf[pos[1]:])
		if pos2 == nil {
			line = string(bigbuf[pos[1]:])
		} else {
			line = string(bigbuf[pos[1] : pos[1]+pos2[0]])
			bigbuf = bigbuf[pos[1]:]
		}
		pos = pos2
		// finally process the line
		if !t.Before(start) {
			// remove a single final \n if present
			if n := len(line); n > 0 && line[n-1] == '\n' {
				line = line[0 : n-1]
			}
			// extract the level
			var level string
			if match := rxLogLevel.FindStringSubmatch(line); len(match) > 0 {
				level = match[1]
				line = line[len(match[0]):]
			}
			c.processLogLine(count == 0, t, user, db, qid, level, line)
			count++
		}
	}

	if count > 0 {
		c.processLogEntry()
	}
	return nil
}

//  1. time stamp with milliseconds
//  2. user name
//  3. database name
//  4. process ID
//  5. client host:port number
//  6. session ID
//  7. per-session line number
//  8. command tag
//  9. session start time
// 10. virtual transaction ID
// 11. regular transaction ID
// 12. error severity
// 13. SQLSTATE code
// 14. error message
// 15. error message detail
// 16. hint
// 17. internal query that led to the error (if any)
// 18. character count of the error position therein
// 19. error context
// 20. user query that led to the error (if any and enabled by log_min_error_statement)
// 21. character count of the error position therein
// 22. location of the error in the PostgreSQL source code (if log_error_verbosity is set to verbose)
// 23. application name
// 24. backend type (>= pg13)
// 25. leader pid (>= pg14)
// 26. query id (>= pg14)

func (c *collector) readLogLinesCSV(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	window := time.Duration(c.logSpan) * time.Minute
	start := time.Now().Add(-window)

	r := csv.NewReader(f)
	r.FieldsPerRecord = 23
	r.ReuseRecord = true
	for {
		record, err := r.Read()
		if err != nil {
			if err == io.EOF || errors.Is(err, csv.ErrFieldCount) {
				// ignore file if ErrFieldCount, probably not a csv file
				return nil
			}
			return err
		}
		t, err := time.Parse("2006-01-02 15:04:05.999 MST", record[0])
		if err != nil || t.Before(start) {
			continue
		}
		var qid int64
		if len(record) >= 26 {
			qid, _ = strconv.ParseInt(record[25], 10, 64)
		}
		c.currLog = logEntry{
			t:     t,
			user:  record[1],
			db:    record[2],
			qid:   qid,
			level: record[11],
			line:  record[13],
		}
		if d := record[14]; len(d) > 0 {
			c.currLog.extra = []logEntryExtra{{level: "DETAIL", line: d}}
		}
		c.processLogEntry()
	}
}

var severities = []string{"DEBUG", "LOG", "INFO", "NOTICE", "WARNING", "ERROR", "FATAL", "PANIC"}

type logEntry struct {
	t     time.Time
	user  string
	db    string
	qid   int64
	level string
	line  string
	extra []logEntryExtra
}

func (l *logEntry) get(level string) string {
	for _, e := range l.extra {
		if e.level == level {
			return e.line
		}
	}
	return ""
}

type logEntryExtra struct {
	level string
	line  string
}

func (c *collector) processLogLine(first bool, t time.Time, user, db string,
	qid int64, level, line string) {

	//log.Printf("debug:got log line [%s] [%s] [%d] [%s] [%s]", user, db, qid, level, line)
	// is this the start of a new entry?
	start := false
	for _, s := range severities {
		if level == s {
			start = true
			break
		}
	}
	if start {
		// flush if required
		if !first {
			c.processLogEntry()
		}
		// start new entry
		c.currLog = logEntry{
			t:     t,
			user:  user,
			db:    db,
			qid:   qid,
			level: level,
			line:  line,
			extra: nil,
		}
	} else {
		// add to extra
		c.currLog.extra = append(c.currLog.extra, logEntryExtra{level: level, line: line})
	}
}

func (c *collector) processLogEntry() {
	//log.Printf("debug: got log entry %+v", c.currLog)
	if sm := rxAEStart.FindStringSubmatch(c.currLog.line); sm != nil {
		c.processAE(sm)
	} else if sm := rxAVStart.FindStringSubmatch(c.currLog.line); sm != nil {
		c.processAV(sm)
	} else if c.currLog.line == "deadlock detected" {
		c.processDeadlock()
	}
}

func (c *collector) processAE(sm []string) {
	e := c.currLog
	p := pgmetrics.Plan{
		Database: e.db,
		UserName: e.user,
		Format:   "text",
		At:       e.t.Unix(),
		QueryID:  e.qid,
	}
	switch {
	case len(sm[1]) > 0:
		p.Format = "json"
		if parts := strings.SplitN(e.line, "\n", 2); len(parts) == 2 { // has to be 2
			var obj map[string]interface{}
			if err := json.Unmarshal([]byte(parts[1]), &obj); err == nil {
				// extract the query and remove it out
				if q, ok := obj["Query Text"]; ok {
					p.Query, _ = q.(string)
					delete(obj, "Query Text")
				}
				if planb, err := json.Marshal(obj); err == nil {
					p.Plan = string(planb)
				}
			}
		}
	case len(sm[2]) > 0:
		p.Format = "xml"
		log.Print("warning: xml format auto_explain output not supported yet")
	case len(sm[3]) > 0:
		p.Format = "yaml"
		log.Print("warning: yaml format auto_explain output not supported yet")
	case len(sm[4]) > 0:
		p.Format = "text"
		var sp *string = nil
		for _, l := range strings.Split(e.line, "\n") {
			if sm := rxAESwitch1.FindStringSubmatch(l); sm != nil {
				p.Query = sm[1]
				sp = &p.Query
				continue
			} else if rxAESwitch2.MatchString(l) {
				sp = &p.Plan
			}
			if sp != nil {
				*sp += l
				*sp += "\n"
			}
		}
	}
	c.result.Plans = append(c.result.Plans, p)
}

func (c *collector) processAV(sm []string) {
	e := c.currLog
	if len(sm) != 4 {
		return
	}
	sm2 := rxAVElapsed.FindStringSubmatch(e.line)
	if len(sm2) != 2 {
		return
	}
	elapsed, _ := strconv.ParseFloat(sm2[1], 64)
	c.result.AutoVacuums = append(c.result.AutoVacuums, pgmetrics.AutoVacuum{
		At:      e.t.Unix(),
		Table:   sm[3],
		Elapsed: elapsed,
	})
}

func (c *collector) processDeadlock() {
	e := c.currLog
	text := strings.ReplaceAll(e.get("DETAIL"), "\t", "") + "\n"
	c.result.Deadlocks = append(c.result.Deadlocks, pgmetrics.Deadlock{At: e.t.Unix(), Detail: text})
}

//------------------------------------------------------------------------------

func getMatchData(match [][]byte, prefix *regexp.Regexp) (t time.Time, user, db string, qid int64, err error) {
	idxT, idxM, idxN := -1, -1, -1
	for i, s := range prefix.SubexpNames() {
		switch s {
		case "t":
			idxT = i
		case "m":
			idxM = i
		case "n":
			idxN = i
		case "u":
			user = string(match[i])
		case "d":
			db = string(match[i])
		case "Q":
			qid, _ = strconv.ParseInt(string(match[i]), 10, 64)
		}
	}
	if idxM != -1 && len(match[idxM]) > 0 {
		t, err = time.Parse("2006-01-02 15:04:05.000 MST", string(match[idxM]))
	} else if idxT != -1 && len(match[idxT]) > 0 {
		t, err = time.Parse("2006-01-02 15:04:05 MST", string(match[idxT]))
	} else if idxN != -1 && len(match[idxN]) > 0 {
		parts := strings.Split(string(match[idxN]), ".")
		if n := len(parts); n < 1 || n > 2 {
			err = fmt.Errorf("wrong %%n format in log line: %s", string(match[idxN]))
			return
		}
		var t1, t2 int64
		if t1, err = strconv.ParseInt(parts[0], 10, 64); err != nil {
			err = fmt.Errorf("bad time format in log line: %s", string(match[idxN]))
			return
		}
		if len(parts) == 2 {
			if t2, err = strconv.ParseInt(parts[1], 10, 64); err != nil {
				err = fmt.Errorf("bad time format in log line: %s", string(match[idxN]))
				return
			}
		}
		t = time.Unix(t1, int64(float64(t2)*1e9))
	}
	return
}

func firstTS(buf []byte, prefix *regexp.Regexp) (t time.Time, err error) {
	matches := prefix.FindSubmatch(buf)
	if len(matches) == 0 {
		return
	}
	idxT, idxM, idxN := -1, -1, -1
	for i, s := range prefix.SubexpNames() {
		if s == "t" {
			idxT = i
		}
		if s == "m" {
			idxM = i
		}
		if s == "n" {
			idxN = i
		}
	}
	if idxM != -1 && len(matches[idxM]) > 0 {
		t, err = time.Parse("2006-01-02 15:04:05.000 MST", string(matches[idxM]))
	} else if idxT != -1 && len(matches[idxT]) > 0 {
		t, err = time.Parse("2006-01-02 15:04:05 MST", string(matches[idxT]))
	} else if idxN != -1 && len(matches[idxN]) > 0 {
		parts := strings.Split(string(matches[idxN]), ".")
		if n := len(parts); n < 1 || n > 2 {
			err = fmt.Errorf("wrong %%n format in log line: %s", string(matches[idxN]))
			return
		}
		var t1, t2 int64
		if t1, err = strconv.ParseInt(parts[0], 10, 64); err != nil {
			err = fmt.Errorf("bad time format in log line: %s", string(matches[idxN]))
			return
		}
		if len(parts) == 2 {
			if t2, err = strconv.ParseInt(parts[1], 10, 64); err != nil {
				err = fmt.Errorf("bad time format in log line: %s", string(matches[idxN]))
				return
			}
		}
		t = time.Unix(t1, int64(float64(t2)*1e9))
	}
	return
}

func compilePrefix(prefix string) (*regexp.Regexp, error) {
	ts, hasq := false, false
	var r string
	for i := 0; i < len(prefix); i++ {
		if prefix[i] != '%' {
			r += regexp.QuoteMeta(string(prefix[i]))
			continue
		}
		if i+1 >= len(prefix) { // bad prefix, ends with a %
			break // postgres ignores it
		}
		i++
		switch prefix[i] {
		case 't': // timestamp without milliseconds
			r += `(?P<t>\d{4}-\d{1,2}-\d{1,2} \d{2}:\d{2}:\d{2} \S+)`
			ts = true
		case 'm': // timestamp with milliseconds
			r += `(?P<m>\d{4}-\d{1,2}-\d{1,2} \d{2}:\d{2}:\d{2}\.\d+ \S+)`
			ts = true
		case 'n': // epoch with milliseconds
			r += `(?P<n>\d+\.\d+)`
			ts = true
		case 's': // process start timestamp
			r += `\d{4}-\d{1,2}-\d{1,2} \d{2}:\d{2}:\d{2} \S+`
		case 'u': // username
			r += `(?P<u>[A-Za-z0-9_.\[\]-]{1,64})`
			if !hasq {
				r += `?`
			}
		case 'd': // database name
			r += `(?P<d>[A-Za-z0-9_.\[\]-]{1,64})`
			if !hasq {
				r += `?`
			}
		case 'Q': // query identifier
			r += `(?P<Q>-?\d+)`
		case 'q': // rest are optional
			r += `(?:` // needs termination
			hasq = true
		default: // optional sequence of non-whitespace characters
			r += `(\S+)?`
		}
	}
	if hasq {
		r += `)?`
	}

	if !ts {
		return nil, errors.New("no timestamp escape sequence was found in log_line_prefix")
	}
	return regexp.Compile(r)
}
