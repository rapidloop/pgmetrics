/*
 * Copyright 2018 RapidLoop, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"

	"github.com/howeyc/gopass"
	"github.com/pborman/getopt"
	"github.com/rapidloop/pgmetrics"
	"github.com/rapidloop/pgmetrics/collector"
	"golang.org/x/crypto/ssh/terminal"
)

const usage = `pgmetrics collects PostgreSQL information and metrics.

Usage:
  pgmetrics [OPTION]... [DBNAME]

General options:
  -t, --timeout=SECS           individual query timeout in seconds (default: 5)
  -i, --input=FILE             don't connect to db, instead read and display
                                   this previously saved JSON file
  -V, --version                output version information, then exit
  -?, --help[=options]         show this help, then exit
      --help=variables         list environment variables, then exit

Collection options:
  -S, --no-sizes               don't collect tablespace and relation sizes
  -c, --schema=REGEXP          collect only from schema(s) matching POSIX regexp
  -C, --exclude-schema=REGEXP  do NOT collect from schema(s) matching POSIX regexp
  -a, --table=REGEXP           collect only from table(s) matching POSIX regexp
  -A, --exclude-table=REGEXP   do NOT collect from table(s) matching POSIX regexp
      --omit=WHAT              do NOT collect the items specified as a comma-separated
                                   list of: "tables", "indexes", "sequences",
                                   "functions", "extensions", "triggers", "statements"
      --sql-length=LIMIT       collect only first LIMIT characters of all SQL
                                   queries (default: 500)
      --statements-limit=LIMIT collect only utmost LIMIT number of row from
                                   pg_stat_statements (default: 100)

Output options:
  -f, --format=FORMAT          output format; "human", or "json" (default: "human")
  -l, --toolong=SECS           for human output, transactions running longer than
                                   this are considered too long (default: 60)
  -o, --output=FILE            write output to the specified file
      --no-pager               do not invoke the pager for tty output

Connection options:
  -h, --host=HOSTNAME          database server host or socket directory
                                   (default: "%s")
  -p, --port=PORT              database server port (default: %d)
  -U, --username=USERNAME      database user name (default: "%s")
      --no-password            never prompt for password

For more information, visit <https://pgmetrics.io>.
`

const variables = `Environment variables:
Usage:
  NAME=VALUE [NAME=VALUE] pgmetrics ...

  PAGER              name of external pager program
  PGAPPNAME          the application_name connection parameter
  PGDATABASE         the dbname connection parameter
  PGHOST             the host connection parameter
  PGPORT             the port connection parameter
  PGUSER             the user connection parameter
  PGPASSWORD         connection password (not recommended)
  PGPASSFILE         path to the pgpass password file
  PGSSLMODE          "disable", "require", "verify-ca", "verify-full"
  PGSSLCERT          path to client SSL certificate
  PGSSLKEY           path to secret key for client SSL certificate
  PGSSLROOTCERT      path to SSL root CA
  PGCONNECT_TIMEOUT  connection timeout in seconds

Also, the following libpq-related environment variarables are not
required/used by pgmetrics and are IGNORED:

  PGHOSTADDR, PGSERVICE,     PGSERVICEFILE, PGREALM,  PGREQUIRESSL,
  PGSSLCRL,   PGREQUIREPEER, PGKRBSRVNAME,  PGGSSLIB, PGSYSCONFDIR,
  PGLOCALEDIR
`

var version string // set during build
var ignoreEnvs = []string{
	"PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGREALM", "PGREQUIRESSL",
	"PGSSLCRL", "PGREQUIREPEER", "PGKRBSRVNAME", "PGGSSLIB", "PGSYSCONFDIR",
	"PGLOCALEDIR",
}

type options struct {
	// collection options
	collector.CollectConfig
	// general
	input     string
	help      string
	helpShort bool
	version   bool
	// output
	format     string
	output     string
	tooLongSec uint
	nopager    bool
	// connection
	passNone bool
}

func (o *options) defaults() {
	// collection options
	o.CollectConfig = collector.DefaultCollectConfig()
	// general
	o.input = ""
	o.help = ""
	o.helpShort = false
	o.version = false
	// output
	o.format = "human"
	o.output = ""
	o.tooLongSec = 60
	o.nopager = false
	// connection
	o.passNone = false
}

func (o *options) usage(code int) {
	fp := os.Stdout
	if code != 0 {
		fp = os.Stderr
	}
	if o.helpShort || code != 0 || o.help == "short" {
		fmt.Fprintf(fp, usage, o.CollectConfig.Host, o.CollectConfig.Port, o.CollectConfig.User)
	} else if o.help == "variables" {
		fmt.Fprint(fp, variables)
	}
	os.Exit(code)
}

func printTry() {
	fmt.Fprintf(os.Stderr, "Try \"pgmetrics --help\" for more information.\n")
}

func getRegexp(r string) (err error) {
	if len(r) > 0 {
		_, err = regexp.CompilePOSIX(r)
	}
	return
}

func (o *options) parse() (args []string) {
	// make getopt
	s := getopt.New()
	s.SetUsage(printTry)
	s.SetProgram("pgmetrics")
	// general
	s.UintVarLong(&o.CollectConfig.TimeoutSec, "timeout", 't', "")
	s.BoolVarLong(&o.CollectConfig.NoSizes, "no-sizes", 'S', "")
	s.StringVarLong(&o.input, "input", 'i', "")
	help := s.StringVarLong(&o.help, "help", '?', "").SetOptional()
	s.BoolVarLong(&o.version, "version", 'V', "").SetFlag()
	// collection
	s.StringVarLong(&o.CollectConfig.Schema, "schema", 'c', "")
	s.StringVarLong(&o.CollectConfig.ExclSchema, "exclude-schema", 'C', "")
	s.StringVarLong(&o.CollectConfig.Table, "table", 'a', "")
	s.StringVarLong(&o.CollectConfig.ExclTable, "exclude-table", 'A', "")
	s.ListVarLong(&o.CollectConfig.Omit, "omit", 0, "")
	s.UintVarLong(&o.CollectConfig.SQLLength, "sql-length", 0, "")
	s.UintVarLong(&o.CollectConfig.StmtsLimit, "statements-limit", 0, "")
	// output
	s.StringVarLong(&o.format, "format", 'f', "")
	s.StringVarLong(&o.output, "output", 'o', "")
	s.UintVarLong(&o.tooLongSec, "toolong", 'l', "")
	s.BoolVarLong(&o.nopager, "no-pager", 0, "").SetFlag()
	// connection
	s.StringVarLong(&o.CollectConfig.Host, "host", 'h', "")
	s.Uint16VarLong(&o.CollectConfig.Port, "port", 'p', "")
	s.StringVarLong(&o.CollectConfig.User, "username", 'U', "")
	s.BoolVarLong(&o.passNone, "no-password", 0, "")

	// parse
	s.Parse(os.Args)
	if help.Seen() && o.help == "" {
		o.help = "short"
	}

	// check values
	if o.help != "" && o.help != "short" && o.help != "variables" {
		printTry()
		os.Exit(2)
	}
	if o.format != "human" && o.format != "json" {
		fmt.Fprintln(os.Stderr, `option -f/--format must be "human" or "json"`)
		printTry()
		os.Exit(2)
	}
	if o.CollectConfig.Port == 0 {
		fmt.Fprintln(os.Stderr, "port must be between 1 and 65535")
		printTry()
		os.Exit(2)
	}
	if o.CollectConfig.TimeoutSec == 0 {
		fmt.Fprintln(os.Stderr, "timeout must be greater than 0")
		printTry()
		os.Exit(2)
	}
	if err := getRegexp(o.CollectConfig.Schema); err != nil {
		fmt.Fprintf(os.Stderr, "bad POSIX regular expression for -c/--schema: %v\n", err)
		printTry()
		os.Exit(2)
	}
	if err := getRegexp(o.CollectConfig.ExclSchema); err != nil {
		fmt.Fprintf(os.Stderr, "bad POSIX regular expression for -C/--exclude-schema: %v\n", err)
		printTry()
		os.Exit(2)
	}
	if err := getRegexp(o.CollectConfig.Table); err != nil {
		fmt.Fprintf(os.Stderr, "bad POSIX regular expression for -a/--table: %v\n", err)
		printTry()
		os.Exit(2)
	}
	if err := getRegexp(o.CollectConfig.ExclTable); err != nil {
		fmt.Fprintf(os.Stderr, "bad POSIX regular expression for -A/--exclude-table: %v\n", err)
		printTry()
		os.Exit(2)
	}
	for _, om := range o.CollectConfig.Omit {
		if om != "tables" && om != "indexes" && om != "sequences" &&
			om != "functions" && om != "extensions" && om != "triggers" &&
			om != "statements" {
			fmt.Fprintf(os.Stderr, "unknown item \"%s\" in --omit option\n", om)
			printTry()
			os.Exit(2)
		}
	}

	// help action
	if o.helpShort || o.help == "short" || o.help == "variables" {
		o.usage(0)
	}

	// version action
	if o.version {
		if len(version) == 0 {
			version = "devel"
		}
		fmt.Println("pgmetrics", version)
		os.Exit(0)
	}

	// return remaining args
	return s.Args()
}

func writeTo(fd io.Writer, o options, result *pgmetrics.Model) {
	if o.format == "json" {
		writeJSONTo(fd, result)
	} else {
		writeHumanTo(fd, o, result)
	}
}

func writeJSONTo(fd io.Writer, result *pgmetrics.Model) {
	enc := json.NewEncoder(fd)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		log.Fatal(err)
	}
}

func process(result *pgmetrics.Model, o options, args []string) {
	if o.output == "-" {
		o.output = ""
	}
	pager := os.Getenv("PAGER")
	if pager == "" {
		if _, err := exec.LookPath("less"); err == nil {
			pager = "less"
		} else if _, err := exec.LookPath("more"); err == nil {
			pager = "more"
		}
	}
	usePager := o.output == "" && o.nopager == false && pager != "" &&
		terminal.IsTerminal(int(os.Stdout.Fd()))
	if usePager {
		cmd := exec.Command(pager)
		pagerStdin, err := cmd.StdinPipe()
		if err != nil {
			log.Fatal(err)
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			log.Fatal(err)
		}
		writeTo(pagerStdin, o, result)
		pagerStdin.Close()
		cmd.Wait()
	} else if o.output != "" {
		f, err := os.Create(o.output)
		if err != nil {
			log.Fatal(err)
		}
		writeTo(f, o, result)
		f.Close()
	} else {
		writeTo(os.Stdout, o, result)
	}
}

func main() {
	for _, e := range ignoreEnvs {
		os.Unsetenv(e)
	}

	var o options
	o.defaults()
	args := o.parse()
	if !o.passNone && len(o.input) == 0 {
		fmt.Print("Password: ")
		p, err := gopass.GetPasswd()
		if err != nil {
			os.Exit(1)
		}
		o.CollectConfig.Password = string(p)
	}

	log.SetFlags(0)
	log.SetPrefix("pgmetrics: ")

	// collect or load data
	var result *pgmetrics.Model
	if len(o.input) > 0 {
		f, err := os.Open(o.input)
		if err != nil {
			log.Fatal(err)
		}
		var obj pgmetrics.Model
		if err = json.NewDecoder(f).Decode(&obj); err != nil {
			log.Fatalf("%s: %v", o.input, err)
		}
		result = &obj
		f.Close()
	} else {
		result = collector.Collect(o.CollectConfig, args)
	}

	// process it
	process(result, o, args)
}
