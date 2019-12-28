package collector

import (
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
	rxAEStart   = regexp.MustCompile(`^duration: [0-9]+\.[0-9]+ ms  plan:\n[ \t]+({[ \t]*\n)?(<explain xml.*\n)?(Query Text: ".*"\n)?(Query Text: [^"].*\n)?`)
	rxAESwitch1 = regexp.MustCompile(`^\s+Query Text: (.*)$`)
	rxAESwitch2 = regexp.MustCompile(`cost=\d+.*rows=\d`)
)

func readLogLines(filename string, window time.Duration, prefix *regexp.Regexp, result *pgmetrics.Model) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// we're seeking to just before this
	start := time.Now().Add(-window)

	// get current length of file
	flen, err := f.Seek(0, 2)
	if err != nil {
		return err
	}
	if flen <= 0 {
		return nil // empty file, nothing to do
	}

	// a buffer for reuse
	buf := make([]byte, 4096)

	// seek to flen-4k
	ofs := flen - 4096
	for {
		if ofs < 0 {
			ofs = 0
		}
		if ofs, err = f.Seek(ofs, 0); err != nil {
			return err
		}
		//log.Printf("debug: seeked to %d", ofs)

		// read the last 4k of the file
		if _, err := io.ReadFull(f, buf); err != nil {
			return err
		}
		ts, err := firstTS(buf, prefix)
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
			// oops reached the top
			//log.Printf("nothing recent in file")
			return nil
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

	pos := prefix.FindIndex(bigbuf)
	for len(pos) == 2 && len(bigbuf) > 0 {
		// match again for submatches, can't do this in one go :-(
		match := prefix.FindSubmatch(bigbuf[pos[0]:])
		t, user, db, err := getMatchData(match, prefix)
		if err != nil {
			return nil
		}
		var line string
		// seek to start of next line
		pos2 := prefix.FindIndex(bigbuf[pos[1]:])
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
			processLogLine(t, user, db, level, line, result)
		}
	}
	return nil
}

func processLogLine(t time.Time, user, db, level, line string, result *pgmetrics.Model) {
	sm := rxAEStart.FindStringSubmatch(line)
	if sm == nil {
		return
	}

	p := pgmetrics.Plan{Database: db, UserName: user, Format: "text", At: t.Unix()}
	switch {
	case len(sm[1]) > 0:
		p.Format = "json"
		if parts := strings.SplitN(line, "\n", 2); len(parts) == 2 { // has to be 2
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
		log.Print("warning: yaml format auto_explain output not supported yet")
	case len(sm[3]) > 0:
		p.Format = "yaml"
		log.Print("warning: yaml format auto_explain output not supported yet")
	case len(sm[4]) > 0:
		p.Format = "text"
		var sp *string = nil
		for _, l := range strings.Split(line, "\n") {
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
	result.Plans = append(result.Plans, p)
}

func getMatchData(match [][]byte, prefix *regexp.Regexp) (t time.Time, user, db string, err error) {
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
	ts := false
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
		case 'd': // database name
			r += `(?P<d>[A-Za-z0-9_.\[\]-]{1,64})`
		case 'q': // ignore, processing directive
		default: // optional sequence of non-whitespace characters
			r += `(\S+)?`
		}
	}

	if !ts {
		return nil, errors.New("no timestamp escape sequence was found in log_line_prefix")
	}
	return regexp.Compile(r)
}

func readLog(filename string, spanMins uint, result *pgmetrics.Model) {
	var prefix string
	if s, ok := result.Settings["log_line_prefix"]; ok {
		prefix = s.Setting
	} else {
		log.Print("failed to get log_line_prefix setting, cannot read log file")
		return
	}

	prefixRE, err := compilePrefix(prefix)
	if err != nil {
		log.Print(err)
		return
	}

	if err := readLogLines(filename, time.Duration(spanMins)*time.Minute, prefixRE, result); err != nil {
		log.Print(err)
		return
	}
}
