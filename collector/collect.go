/*
 * Copyright 2025 RapidLoop, Inc.
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

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rapidloop/pgmetrics"
	"golang.org/x/mod/semver"
)

// Postgres version constants
const (
	pgv94 = 9_04_00
	pgv95 = 9_05_00
	pgv96 = 9_06_00
	pgv10 = 10_00_00
	pgv11 = 11_00_00
	pgv12 = 12_00_00
	pgv13 = 13_00_00
	pgv14 = 14_00_00
	pgv15 = 15_00_00
	pgv16 = 16_00_00
	pgv17 = 17_00_00
	pgv18 = 18_00_00
)

// See https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING
func makeKV(k, v string) string {
	var v2 string
	for _, ch := range v {
		if ch == '\\' || ch == '\'' {
			v2 += "\\"
		}
		v2 += string(ch)
	}
	if len(v2) == 0 || strings.IndexByte(v2, ' ') != -1 {
		return fmt.Sprintf("%s='%s' ", k, v2)
	}
	return fmt.Sprintf("%s=%s ", k, v2)
}

var rxIdent = regexp.MustCompile(`^[A-Za-z\200-\377_][A-Za-z\200-\377_0-9\$]*$`)

func isValidIdent(s string) bool {
	return rxIdent.MatchString(s)
}

// CollectConfig is a bunch of options passed to the Collect() function to
// specify which metrics to collect and how.
type CollectConfig struct {
	// general
	TimeoutSec          uint
	LockTimeoutMillisec uint
	NoSizes             bool

	// collection
	Schema          string
	ExclSchema      string
	Table           string
	ExclTable       string
	SQLLength       uint
	StmtsLimit      uint
	Omit            []string
	OnlyListedDBs   bool
	LogFile         string
	LogDir          string
	LogSpan         uint
	RDSDBIdentifier string
	AllDBs          bool
	AzureResourceID string
	Pgpool          bool // collect only pgpool information
	UseExtendedQP   bool // use extended query protocol instead of simple

	// connection
	Host     string
	Port     uint16
	User     string
	Password string
	Role     string
}

// DefaultCollectConfig returns a CollectConfig initialized with default values.
// Some environment variables are consulted.
func DefaultCollectConfig() CollectConfig {
	cc := CollectConfig{
		// ------------------ general
		TimeoutSec:          5,
		LockTimeoutMillisec: 50,

		// ------------------ collection
		SQLLength:  500,
		StmtsLimit: 100,
		LogSpan:    5,

		// ------------------ connection
	}

	// connection: host
	if h := os.Getenv("PGHOST"); len(h) > 0 {
		cc.Host = h
	} else {
		cc.Host = "/var/run/postgresql"
	}

	// connection: port
	if ps := os.Getenv("PGPORT"); len(ps) > 0 {
		if p, err := strconv.Atoi(ps); err == nil && p > 0 && p < 65536 {
			cc.Port = uint16(p)
		} else {
			cc.Port = 5432
		}
	} else {
		cc.Port = 5432
	}

	// connection: user
	if u := os.Getenv("PGUSER"); len(u) > 0 {
		cc.User = u
	} else if u, err := user.Current(); err == nil && u != nil {
		cc.User = u.Username
	} else {
		cc.User = ""
	}

	return cc
}

func getRegexp(r string) (rx *regexp.Regexp) {
	if len(r) > 0 {
		rx, _ = regexp.CompilePOSIX(r) // ignore errors, already checked
	}
	return
}

// Collect actually performs the metrics collection, based on the given options.
// If database names are specified, it connects to each in turn and accumulates
// results. If none are specified, the connection is attempted without a
// 'dbname' keyword (usually tries to connect to a database with same name
// as the user).
//
// Ideally, this should return (*pgmetrics.Model, error). But for now, it does
// a log.Fatal(). This will be rectified in the future, and
// backwards-compatibility will be broken when that happens. You've been warned.
func Collect(o CollectConfig, dbnames []string) *pgmetrics.Model {
	// form connection string
	var connstr string
	mode := "postgres"
	// Support supplying the connection string itself as an argument. If this
	// is specified, it takes precedence over other command-line options.
	if len(dbnames) == 1 {
		// see if this is actually a connection string
		cfg, err := pgx.ParseConfig(dbnames[0])
		if err == nil {
			// yes it is, use it
			connstr = cfg.ConnString() + " "
			if cfg.Database == "pgbouncer" {
				mode = "pgbouncer"
			}
			dbnames = dbnames[1:]
		}
	}
	if len(connstr) == 0 {
		// connection string was not specified, use command-line options
		if len(o.Host) > 0 {
			connstr += makeKV("host", o.Host)
		}
		connstr += makeKV("port", strconv.Itoa(int(o.Port)))
		if len(o.User) > 0 {
			connstr += makeKV("user", o.User)
		}
		if len(o.Password) > 0 {
			connstr += makeKV("password", o.Password)
		}
		// pgmetrics defaults to sslmode=disable if unset. Explicitly set
		// the environment variable PGSSLMODE before invoking pgmetrics if you want
		// a different behavior.
		if os.Getenv("PGSSLMODE") == "" {
			connstr += makeKV("sslmode", "disable")
		}
		if len(dbnames) == 1 && dbnames[0] == "pgbouncer" {
			mode = "pgbouncer"
		}
	}
	if o.Pgpool {
		mode = "pgpool"
	}

	// set timeouts (but not for pgbouncer, it does not like them)
	if mode != "pgbouncer" {
		connstr += makeKV("lock_timeout", strconv.Itoa(int(o.LockTimeoutMillisec)))
		connstr += makeKV("statement_timeout", strconv.Itoa(int(o.TimeoutSec)*1000))
	}

	// set application name
	connstr += makeKV("application_name", "pgmetrics")

	// Using simple protocol for maximum compatibility is the default for
	// pgmetrics. This is selected by adding default_query_exec_mode=simple_protocol
	// to the connection string. To use extended query protocol, nothing needs
	// to be added.
	if !o.UseExtendedQP {
		connstr += makeKV("default_query_exec_mode", "simple_protocol")
	}

	// if "all DBs" was specified, collect the names of databases first
	if o.AllDBs {
		dbnames = getDBNames(connstr, o)
	}

	// collect from 1 or more DBs
	c := &collector{
		dbnames: dbnames,
		mode:    mode,
	}
	if len(dbnames) == 0 {
		collectFromDB(connstr, c, o)
	} else {
		for _, dbname := range dbnames {
			collectFromDB(connstr+makeKV("dbname", dbname), c, o)
		}
	}
	if !arrayHas(o.Omit, "log") && c.local {
		// note: for rds we collect logs in the next step
		c.collectLogs(o)
	}

	// collect from RDS if database id is specified
	if len(o.RDSDBIdentifier) > 0 {
		c.collectFromRDS(o)
	}

	// collect from Azure if resource id is specified
	if len(o.AzureResourceID) > 0 {
		c.collectFromAzure(o)
	}

	return &c.result
}

func getConn(connstr string, o CollectConfig) *sql.DB {
	db, err := sql.Open("pgx", connstr)
	if err != nil {
		log.Fatalf("failed to open connection: %v", err)
	}

	// ensure only 1 conn
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)

	// set role, if specified
	if len(o.Role) > 0 {
		if !isValidIdent(o.Role) {
			log.Fatalf("bad format for role %q", o.Role)
		}
		t := time.Duration(o.TimeoutSec) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), t)
		defer cancel()
		if _, err := db.ExecContext(ctx, "SET ROLE "+o.Role); err != nil {
			log.Fatalf("failed to set role %q: %v", o.Role, err)
		}
	}

	return db
}

func collectFromDB(connstr string, c *collector, o CollectConfig) {
	db := getConn(connstr, o)
	c.collect(db, o)
	db.Close()
}

func getDBNames(connstr string, o CollectConfig) (dbnames []string) {
	db := getConn(connstr+makeKV("dbname", "postgres"), o)
	defer db.Close()

	timeout := time.Duration(o.TimeoutSec) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	q := `SELECT datname
		    FROM pg_database
		   WHERE (NOT datistemplate) AND (datname <> 'postgres')`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_database query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatalf("pg_database query failed: %v", err)
		}
		dbnames = append(dbnames, name)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_database query failed: %v", err)
	}
	return
}

type collector struct {
	db           *sql.DB
	result       pgmetrics.Model
	version      int    // integer form of server version
	local        bool   // have we connected to the server on the same machine?
	dataDir      string // the PGDATA dir, valid only if local
	beenHere     bool
	timeout      time.Duration
	rxSchema     *regexp.Regexp
	rxExclSchema *regexp.Regexp
	rxTable      *regexp.Regexp
	rxExclTable  *regexp.Regexp
	sqlLength    uint
	stmtsLimit   uint
	dbnames      []string
	curlogfile   string
	csvlog       bool
	logSpan      uint
	currLog      pgmetrics.LogEntry
	rxPrefix     *regexp.Regexp
	mode         string // "postgres", "pgbouncer" or "pgpool"
}

func (c *collector) collect(db *sql.DB, o CollectConfig) {
	if !c.beenHere {
		c.collectFirst(db, o)
		c.beenHere = true
	} else {
		c.collectNext(db, o)
	}
}

func (c *collector) collectFirst(db *sql.DB, o CollectConfig) {
	c.db = db
	c.timeout = time.Duration(o.TimeoutSec) * time.Second

	// Compile regexes for schema and table, if any. The values are already
	// checked for validity.
	c.rxSchema = getRegexp(o.Schema)
	c.rxExclSchema = getRegexp(o.ExclSchema)
	c.rxTable = getRegexp(o.Table)
	c.rxExclTable = getRegexp(o.ExclTable)

	// save limits
	c.sqlLength = o.SQLLength
	c.stmtsLimit = o.StmtsLimit
	c.logSpan = o.LogSpan

	// fill out some metadata fields
	c.result.Metadata.At = time.Now().Unix()
	c.result.Metadata.Version = pgmetrics.ModelSchemaVersion

	// collect either postgres, pgbouncer or pgpool metrics
	c.result.Metadata.Mode = c.mode
	switch c.mode {
	case "pgpool":
		c.getCurrentUser()
		c.collectPgpool()
	case "pgbouncer":
		c.collectPgBouncer()
	case "postgres":
		c.getCurrentUser()
		c.collectPostgres(o)
	default:
		log.Fatalf("unknown mode %q", c.mode)
	}
}

func (c *collector) collectPostgres(o CollectConfig) {
	// get settings and other configuration
	c.getSettings()
	if v, err := strconv.Atoi(c.setting("server_version_num")); err != nil {
		log.Fatalf("bad server_version_num: %v", err)
	} else {
		c.version = v
	}
	c.getLocal()
	if c.local {
		c.dataDir = c.setting("data_directory")
		if len(c.dataDir) == 0 {
			c.dataDir = os.Getenv("PGDATA")
		}
	}

	c.collectCluster(o)
	if c.local {
		// Only implemented for Linux for now.
		if runtime.GOOS == "linux" {
			c.collectSystem(o)
		}
	}
	c.collectDatabase(o)
}

func (c *collector) collectNext(db *sql.DB, o CollectConfig) {
	c.db = db
	c.collectDatabase(o)
}

// cluster-level info and stats
func (c *collector) collectCluster(o CollectConfig) {
	c.getPGSystemInfo()

	if c.version >= pgv96 {
		c.getControlSystemv96()
	}

	if c.version >= pgv95 {
		c.getLastXactv95()
	}

	if c.version >= pgv11 {
		c.getControlCheckpointv11()
	} else if c.version >= pgv10 {
		c.getControlCheckpointv10()
	} else if c.version >= pgv96 {
		c.getControlCheckpointv96()
	}

	if c.version >= pgv96 {
		c.getActivityv96()
	} else if c.version >= pgv94 {
		c.getActivityv94()
	} else {
		c.getActivityv93()
	}

	if c.version >= pgv10 {
		c.getBETypeCountsv10()
	}

	if c.version >= pgv94 {
		c.getWALArchiver()
	}

	if c.version >= pgv17 {
		c.getBGWriterv17()
	} else {
		c.getBGWriter()
	}

	if c.version >= pgv10 {
		c.getReplicationv10()
	} else {
		c.getReplicationv9()
	}

	if c.version >= pgv13 {
		c.getWalReceiverv13()
	} else if c.version >= pgv96 {
		c.getWalReceiverv96()
	}

	if c.version >= pgv10 {
		c.getAdminFuncv10()
	} else {
		c.getAdminFuncv9()
	}

	if c.version >= pgv17 {
		c.getVacuumProgressv17()
	} else if c.version >= pgv96 {
		c.getVacuumProgressv96()
	}

	c.getDatabases(!o.NoSizes, o.OnlyListedDBs, c.dbnames)
	c.getTablespaces(!o.NoSizes)

	if c.version >= pgv94 {
		c.getReplicationSlotsv94()
	}

	c.getRoles()

	if c.version >= pgv12 {
		c.getWALCountsv12()
	} else if c.version >= pgv11 {
		c.getWALCountsv11()
	} else {
		c.getWALCounts()
	}

	if c.version >= pgv96 {
		c.getNotification()
	}

	c.getLocks()

	if c.version >= pgv18 {
		c.getWALv18()
	} else if c.version >= pgv14 {
		c.getWAL()
	}

	// various pg_stat_progress_* views
	if c.version >= pgv12 {
		c.getProgressCluster()
		c.getProgressCreateIndex()
	}
	if c.version >= pgv13 {
		c.getProgressAnalyze()
		c.getProgressBasebackup()
	}
	if c.version >= pgv14 {
		c.getProgressCopy()
	}

	if c.version >= pgv17 {
		c.getCheckpointer()
	}

	if !arrayHas(o.Omit, "log") && c.local {
		c.getLogInfo()
	}
}

// info and stats for the current database
func (c *collector) collectDatabase(o CollectConfig) {
	currdb := c.getCurrentDatabase()
	for _, d := range c.result.Metadata.CollectedDBs {
		if currdb == d { // don't collect from same db twice
			return
		}
	}
	c.result.Metadata.CollectedDBs = append(c.result.Metadata.CollectedDBs, currdb)

	if !arrayHas(o.Omit, "tables") {
		c.getTables(!o.NoSizes)
		// partition information, added schema v1.2
		if c.version >= pgv10 {
			c.getPartitionInfo()
		}
		// parent information, added schema v1.2
		c.getParentInfo()
	}
	if !arrayHas(o.Omit, "tables") && !arrayHas(o.Omit, "indexes") {
		c.getIndexes(!o.NoSizes)
		if !arrayHas(o.Omit, "indexdefs") {
			c.getIndexDef()
		}
	}
	if !arrayHas(o.Omit, "sequences") {
		c.getSequences()
	}
	if !arrayHas(o.Omit, "functions") {
		c.getUserFunctions()
	}
	if !arrayHas(o.Omit, "extensions") {
		c.getExtensions()
	}
	if !arrayHas(o.Omit, "tables") && !arrayHas(o.Omit, "triggers") {
		c.getDisabledTriggers()
	}
	if !arrayHas(o.Omit, "statements") {
		c.getStatements(currdb)
	}
	if !arrayHas(o.Omit, "bloat") {
		c.getBloat()
	}

	// logical replication, added schema v1.2
	if c.version >= pgv10 {
		c.getPublications()
		c.getSubscriptions()
	}

	// citus, added in schema 1.9
	if !arrayHas(o.Omit, "citus") {
		c.getCitus(currdb, !o.NoSizes)
	}
}

func arrayHas(arr []string, val string) bool {
	for _, elem := range arr {
		if elem == val {
			return true
		}
	}
	return false
}

// schemaOK checks to see if this schema is OK to be collected, based on the
// --schema/--exclude-schema constraints. Also forces exclusion of pg_temp_*
// schemas.
func (c *collector) schemaOK(name string) bool {
	if strings.HasPrefix(name, "pg_temp_") {
		// exclude temporary tables, indexes on them
		return false
	}
	if c.rxSchema != nil {
		// exclude things that don't match --schema
		if !c.rxSchema.MatchString(name) {
			return false
		}
	}
	if c.rxExclSchema != nil {
		// exclude things that match --exclude-schema
		if c.rxExclSchema.MatchString(name) {
			return false
		}
	}
	return true
}

// tableOK checks to see if the schema and table are to be collected, based on
// --{,exclude-}{schema,table} options.
func (c *collector) tableOK(schema, table string) bool {
	// exclude things that don't match schema constraints
	if !c.schemaOK(schema) {
		return false
	}
	if c.rxTable != nil {
		// exclude things that don't match --table
		if !c.rxTable.MatchString(table) {
			return false
		}
	}
	if c.rxExclTable != nil {
		// exclude things that match --exclude-table
		if c.rxExclTable.MatchString(table) {
			return false
		}
	}
	return true
}

func (c *collector) getCurrentUser() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT current_user`
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.Metadata.Username); err != nil {
		log.Fatalf("current_user failed: %v", err)
	}
}

func (c *collector) setting(key string) string {
	if s, ok := c.result.Settings[key]; ok {
		return s.Setting
	}
	return ""
}

func (c *collector) getSettings() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT name, setting, COALESCE(boot_val,''), source,
			COALESCE(sourcefile,''), COALESCE(sourceline,0),
			pending_restart
		  FROM pg_settings
		  ORDER BY name ASC`
	if c.version < pgv95 { // pending_restart was added in pg9.5
		q = strings.Replace(q, "pending_restart", "FALSE", 1)
	}

	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_settings query failed: %v", err)
	}
	defer rows.Close()

	c.result.Settings = make(map[string]pgmetrics.Setting)
	for rows.Next() {
		var s pgmetrics.Setting
		var name, sf, sl string
		if err := rows.Scan(&name, &s.Setting, &s.BootVal, &s.Source, &sf, &sl, &s.Pending); err != nil {
			log.Fatalf("pg_settings query failed: %v", err)
		}
		if len(sf) > 0 {
			s.Source = sf
			if len(sl) > 0 {
				s.Source += ":" + sl
			}
		}
		if s.Setting == s.BootVal { // if not different from default, omit it
			s.BootVal = "" // will be omitted from json
			s.Source = ""  // will be omitted from json
		}
		c.result.Settings[name] = s
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_settings query failed: %v", err)
	}
}

func (c *collector) getWALArchiver() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT archived_count, 
			COALESCE(last_archived_wal, ''),
			COALESCE(EXTRACT(EPOCH FROM last_archived_time)::bigint, 0),
			failed_count,
			COALESCE(last_failed_wal, ''),
			COALESCE(EXTRACT(EPOCH FROM last_failed_time)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM stats_reset)::bigint, 0)
		  FROM pg_stat_archiver`
	a := &c.result.WALArchiving
	if err := c.db.QueryRowContext(ctx, q).Scan(&a.ArchivedCount, &a.LastArchivedWAL,
		&a.LastArchivedTime, &a.FailedCount, &a.LastFailedWAL, &a.LastFailedTime,
		&a.StatsReset); err != nil {
		log.Fatalf("pg_stat_archiver query failed: %v", err)
	}
}

// have we connected to a postgres server running on the local machine?
func (c *collector) getLocal() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// see also https://github.com/rapidloop/pgmetrics/issues/39
	q := `SELECT COALESCE(inet_client_addr() = inet_server_addr(), TRUE)
			OR (inet_server_addr() << '127.0.0.0/8' AND inet_client_addr() << '127.0.0.0/8')`
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.local); err != nil {
		c.local = false // don't fail on errors
	}
	c.result.Metadata.Local = c.local
}

func (c *collector) getBGWriterv17() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT buffers_clean, maxwritten_clean, buffers_alloc, stats_reset
		  FROM pg_stat_bgwriter`
	bg := &c.result.BGWriter
	var statsReset time.Time
	if err := c.db.QueryRowContext(ctx, q).Scan(&bg.BuffersClean,
		&bg.MaxWrittenClean, &bg.BuffersAlloc, &statsReset); err != nil {
		log.Fatalf("pg_stat_bgwriter query failed: %v", err)
		return
	}
	bg.StatsReset = statsReset.Unix()
}

func (c *collector) getBGWriter() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time,
			checkpoint_sync_time, buffers_checkpoint, buffers_clean, maxwritten_clean,
			buffers_backend, buffers_backend_fsync, buffers_alloc, stats_reset
		  FROM pg_stat_bgwriter`
	bg := &c.result.BGWriter
	var statsReset time.Time
	if err := c.db.QueryRowContext(ctx, q).Scan(&bg.CheckpointsTimed, &bg.CheckpointsRequested,
		&bg.CheckpointWriteTime, &bg.CheckpointSyncTime, &bg.BuffersCheckpoint,
		&bg.BuffersClean, &bg.MaxWrittenClean, &bg.BuffersBackend,
		&bg.BuffersBackendFsync, &bg.BuffersAlloc, &statsReset); err != nil {
		log.Fatalf("pg_stat_bgwriter query failed: %v", err)
		return
	}
	bg.StatsReset = statsReset.Unix()
}

func (c *collector) getReplicationv10() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT COALESCE(usename, ''), application_name,
			COALESCE(client_hostname::text, client_addr::text, ''), 
			COALESCE(EXTRACT(EPOCH FROM backend_start)::bigint, 0),
			backend_xmin, COALESCE(state, ''),
			COALESCE(sent_lsn::text, ''),
			COALESCE(write_lsn::text, ''),
			COALESCE(flush_lsn::text, ''),
			COALESCE(replay_lsn::text, ''),
			COALESCE(EXTRACT(EPOCH FROM write_lag)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM flush_lag)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM replay_lag)::bigint, 0),
			COALESCE(sync_priority, -1),
			COALESCE(sync_state, ''),
			@reply_time@,
			pid
		  FROM pg_stat_replication
		  ORDER BY pid ASC`
	if c.version >= pgv12 {
		// for pg v12+ also collect reply_time
		q = strings.Replace(q, "@reply_time@", `COALESCE(EXTRACT(EPOCH FROM reply_time)::bigint, 0)`, 1)
	} else {
		q = strings.Replace(q, "@reply_time@", `0`, 1)
	}
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: pg_stat_replication query failed: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var r pgmetrics.ReplicationOut
		var backendXmin sql.NullInt64
		if err := rows.Scan(&r.RoleName, &r.ApplicationName, &r.ClientAddr,
			&r.BackendStart, &backendXmin, &r.State, &r.SentLSN, &r.WriteLSN,
			&r.FlushLSN, &r.ReplayLSN, &r.WriteLag, &r.FlushLag, &r.ReplayLag,
			&r.SyncPriority, &r.SyncState, &r.ReplyTime, &r.PID); err != nil {
			log.Fatalf("pg_stat_replication query failed: %v", err)
		}
		r.BackendXmin = int(backendXmin.Int64)
		c.result.ReplicationOutgoing = append(c.result.ReplicationOutgoing, r)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_replication query failed: %v", err)
	}
}

func (c *collector) getReplicationv9() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT COALESCE(usename, ''), application_name,
			COALESCE(client_hostname::text, client_addr::text, ''), 
			COALESCE(EXTRACT(EPOCH FROM backend_start)::bigint, 0),
			backend_xmin, COALESCE(state, ''),
			COALESCE(sent_location::text, ''),
			COALESCE(write_location::text, ''),
			COALESCE(flush_location::text, ''),
			COALESCE(replay_location::text, ''),
			COALESCE(sync_priority, -1),
			COALESCE(sync_state, ''),
			pid
		  FROM pg_stat_replication
		  ORDER BY pid ASC`
	if c.version < pgv94 { // backend_xmin is only in v9.4+
		q = strings.Replace(q, "backend_xmin", "0", 1)
	}
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: pg_stat_replication query failed: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var r pgmetrics.ReplicationOut
		var backendXmin sql.NullInt64
		if err := rows.Scan(&r.RoleName, &r.ApplicationName, &r.ClientAddr,
			&r.BackendStart, &backendXmin, &r.State, &r.SentLSN, &r.WriteLSN,
			&r.FlushLSN, &r.ReplayLSN, &r.SyncPriority, &r.SyncState, &r.PID); err != nil {
			log.Fatalf("pg_stat_replication query failed: %v", err)
		}
		r.BackendXmin = int(backendXmin.Int64)
		c.result.ReplicationOutgoing = append(c.result.ReplicationOutgoing, r)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_replication query failed: %v", err)
	}
}

func (c *collector) getWalReceiverv13() {
	// skip if Aurora, because the function errors out with:
	// "Function pg_stat_get_wal_receiver() is currently not supported in Aurora"
	if c.isAWSAurora() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT status, receive_start_lsn, receive_start_tli,
			COALESCE(written_lsn::text, ''), COALESCE(flushed_lsn::text, ''),
			received_tli, last_msg_send_time, last_msg_receipt_time,
			COALESCE(latest_end_lsn::text, ''),
			COALESCE(EXTRACT(EPOCH FROM latest_end_time)::bigint, 0),
			COALESCE(slot_name, ''), conninfo, @sender_host@
		  FROM pg_stat_wal_receiver`
	if c.version >= pgv11 {
		q = strings.Replace(q, "@sender_host@", `COALESCE(sender_host, '')`, 1)
	} else {
		q = strings.Replace(q, "@sender_host@", `''`, 1)
	}
	var r pgmetrics.ReplicationIn
	var msgSend, msgRecv sql.NullTime
	if err := c.db.QueryRowContext(ctx, q).Scan(&r.Status, &r.ReceiveStartLSN,
		&r.ReceiveStartTLI, &r.WrittenLSN, &r.FlushedLSN, &r.ReceivedTLI,
		&msgSend, &msgRecv, &r.LatestEndLSN, &r.LatestEndTime, &r.SlotName,
		&r.Conninfo, &r.SenderHost); err != nil {
		if err == sql.ErrNoRows {
			return // not an error
		}
		log.Printf("warning: pg_stat_wal_receiver query failed: %v", err)
		return
	}

	if msgSend.Valid && msgRecv.Valid && msgRecv.Time.After(msgSend.Time) {
		r.Latency = int64(msgRecv.Time.Sub(msgSend.Time)) / 1000
	}
	if msgSend.Valid {
		r.LastMsgSendTime = msgSend.Time.Unix()
	}
	if msgRecv.Valid {
		r.LastMsgReceiptTime = msgRecv.Time.Unix()
	}
	c.result.ReplicationIncoming = &r
}

func (c *collector) getWalReceiverv96() {
	// skip if Aurora, because the function errors out with:
	// "Function pg_stat_get_wal_receiver() is currently not supported in Aurora"
	if c.isAWSAurora() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT status, receive_start_lsn, receive_start_tli, received_lsn, 
			received_tli, last_msg_send_time, last_msg_receipt_time,
			latest_end_lsn,
			COALESCE(EXTRACT(EPOCH FROM latest_end_time)::bigint, 0),
			COALESCE(slot_name, ''), conninfo
		  FROM pg_stat_wal_receiver`
	var r pgmetrics.ReplicationIn
	var msgSend, msgRecv sql.NullTime
	if err := c.db.QueryRowContext(ctx, q).Scan(&r.Status, &r.ReceiveStartLSN, &r.ReceiveStartTLI,
		&r.ReceivedLSN, &r.ReceivedTLI, &msgSend, &msgRecv,
		&r.LatestEndLSN, &r.LatestEndTime, &r.SlotName, &r.Conninfo); err != nil {
		if err == sql.ErrNoRows {
			return // not an error
		}
		log.Printf("warning: pg_stat_wal_receiver query failed: %v", err)
		return
	}

	if msgSend.Valid && msgRecv.Valid && msgRecv.Time.After(msgSend.Time) {
		r.Latency = int64(msgRecv.Time.Sub(msgSend.Time)) / 1000
	}
	if msgSend.Valid {
		r.LastMsgSendTime = msgSend.Time.Unix()
	}
	if msgRecv.Valid {
		r.LastMsgReceiptTime = msgRecv.Time.Unix()
	}
	c.result.ReplicationIncoming = &r
}

func (c *collector) getAdminFuncv9() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pg_is_in_recovery(),
			COALESCE(pg_last_xlog_receive_location()::text, ''),
			COALESCE(pg_last_xlog_replay_location()::text, ''),
			COALESCE(EXTRACT(EPOCH FROM pg_last_xact_replay_timestamp())::bigint, 0)`
	if c.isAWSAurora() {
		q = `SELECT pg_is_in_recovery(), '', '', 0`
	}
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.IsInRecovery,
		&c.result.LastWALReceiveLSN, &c.result.LastWALReplayLSN,
		&c.result.LastXActReplayTimestamp); err != nil {
		log.Printf("warning: admin functions query failed: %v", err)
		// don't return here, continue with the rest
	}

	if c.result.IsInRecovery {
		if !c.isAWSAurora() {
			qr := `SELECT pg_is_xlog_replay_paused()`
			if err := c.db.QueryRowContext(ctx, qr).Scan(&c.result.IsWalReplayPaused); err != nil {
				log.Fatalf("pg_is_xlog_replay_paused() failed: %v", err)
			}
		}
	} else {
		if c.version >= pgv96 && !c.isAWSAurora() { // don't attempt on Aurora
			qx := `SELECT pg_current_xlog_flush_location(),
					pg_current_xlog_insert_location(), pg_current_xlog_location()`
			if err := c.db.QueryRowContext(ctx, qx).Scan(&c.result.WALFlushLSN,
				&c.result.WALInsertLSN, &c.result.WALLSN); err != nil {
				log.Fatalf("error querying wal location functions: %v", err)
			}
		}
		// pg_current_xlog_* not available in < v9.6
	}
}

func (c *collector) getAdminFuncv10() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pg_is_in_recovery(),
			COALESCE(pg_last_wal_receive_lsn()::text, ''),
			COALESCE(pg_last_wal_replay_lsn()::text, ''),
			COALESCE(EXTRACT(EPOCH FROM pg_last_xact_replay_timestamp())::bigint, 0)`
	if c.isAWSAurora() {
		q = `SELECT pg_is_in_recovery(), '', '', 0`
	}
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.IsInRecovery,
		&c.result.LastWALReceiveLSN, &c.result.LastWALReplayLSN,
		&c.result.LastXActReplayTimestamp); err != nil {
		log.Printf("warning: admin functions query failed: %v", err)
		// don't return here, continue with the rest
	}

	if c.result.IsInRecovery {
		if !c.isAWSAurora() {
			qr := `SELECT pg_is_wal_replay_paused()`
			if err := c.db.QueryRowContext(ctx, qr).Scan(&c.result.IsWalReplayPaused); err != nil {
				log.Fatalf("pg_is_wal_replay_paused() failed: %v", err)
			}
		}
	} else {
		if (c.isAWSAurora() && c.setting("wal_level") == "logical") || !c.isAWSAurora() {
			qx := `SELECT pg_current_wal_flush_lsn(),
				pg_current_wal_insert_lsn(), pg_current_wal_lsn()`
			if err := c.db.QueryRowContext(ctx, qx).Scan(&c.result.WALFlushLSN,
				&c.result.WALInsertLSN, &c.result.WALLSN); err != nil {
				log.Fatalf("error querying wal location functions: %v", err)
			}
		}
	}
}

func (c *collector) fillTablespaceSize(t *pgmetrics.Tablespace) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pg_tablespace_size($1)`
	if err := c.db.QueryRowContext(ctx, q, t.Name).Scan(&t.Size); err != nil {
		t.Size = -1
	}
}

func (c *collector) fillDatabaseSize(d *pgmetrics.Database) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pg_database_size($1)`
	if err := c.db.QueryRowContext(ctx, q, d.Name).Scan(&d.Size); err != nil {
		d.Size = -1
	}
}

func (c *collector) getLastXactv95() {
	// available only if "track_commit_timestamp" is set to "on"
	if c.setting("track_commit_timestamp") != "on" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT xid, COALESCE(EXTRACT(EPOCH FROM timestamp)::bigint, 0)
			FROM pg_last_committed_xact()`
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.LastXactXid, &c.result.LastXactTimestamp); err != nil {
		log.Printf("warning: pg_last_committed_xact() failed: %v", err) // continue anyway
	}
}

func (c *collector) getPGSystemInfo() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT
			version(), -- version
			EXTRACT(EPOCH FROM pg_postmaster_start_time())::bigint, -- start time
			COALESCE(EXTRACT(EPOCH FROM pg_conf_load_time())::bigint, 0), -- conf load time
			COALESCE(host(inet_client_addr()) || ' ' || inet_client_port()::text ||
				' ' || host(inet_server_addr()) || ' ' || inet_server_port()::text, '') -- conn tuple`
	if err := c.db.QueryRowContext(ctx, q).Scan(
		&c.result.FullVersion,
		&c.result.StartTime,
		&c.result.ConfLoadTime,
		&c.result.ConnectionTuple); err != nil {
		log.Fatalf("system functions query failed: %v", err)
	}
}

func (c *collector) getControlSystemv96() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT system_identifier FROM pg_control_system()`
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.SystemIdentifier); err != nil {
		log.Fatalf("pg_control_system() failed: %v", err)
	}
}

func (c *collector) getControlCheckpointv96() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT checkpoint_location, prior_location, redo_location, timeline_id,
			next_xid, oldest_xid, oldest_active_xid,
			COALESCE(EXTRACT(EPOCH FROM checkpoint_time)::bigint, 0)
		  FROM pg_control_checkpoint()`
	var nextXid string // we'll strip out the epoch from next_xid
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.CheckpointLSN,
		&c.result.PriorLSN, &c.result.RedoLSN, &c.result.TimelineID, &nextXid,
		&c.result.OldestXid, &c.result.OldestActiveXid,
		&c.result.CheckpointTime); err != nil {
		log.Fatalf("pg_control_checkpoint() failed: %v", err)
	}

	if pos := strings.IndexByte(nextXid, ':'); pos > -1 {
		nextXid = nextXid[pos+1:]
	}
	if v, err := strconv.Atoi(nextXid); err != nil {
		log.Fatal("bad xid in pg_control_checkpoint()).next_xid")
	} else {
		c.result.NextXid = v
	}

	c.fixAuroraCheckpoint()
}

func (c *collector) getControlCheckpointv10() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT checkpoint_lsn, prior_lsn, redo_lsn, timeline_id,
			next_xid, oldest_xid, oldest_active_xid,
			COALESCE(EXTRACT(EPOCH FROM checkpoint_time)::bigint, 0)
		  FROM pg_control_checkpoint()`
	var nextXid string // we'll strip out the epoch from next_xid
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.CheckpointLSN, &c.result.PriorLSN,
		&c.result.RedoLSN, &c.result.TimelineID, &nextXid, &c.result.OldestXid,
		&c.result.OldestActiveXid, &c.result.CheckpointTime); err != nil {
		log.Fatalf("pg_control_checkpoint() failed: %v", err)
	}

	if pos := strings.IndexByte(nextXid, ':'); pos > -1 {
		nextXid = nextXid[pos+1:]
	}
	if v, err := strconv.Atoi(nextXid); err != nil {
		log.Fatal("bad xid in pg_control_checkpoint()).next_xid")
	} else {
		c.result.NextXid = v
	}

	c.fixAuroraCheckpoint()
}

func (c *collector) getControlCheckpointv11() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT checkpoint_lsn, redo_lsn, timeline_id,
			next_xid, oldest_xid, oldest_active_xid,
			COALESCE(EXTRACT(EPOCH FROM checkpoint_time)::bigint, 0)
		  FROM pg_control_checkpoint()`
	var nextXid string // we'll strip out the epoch from next_xid
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.CheckpointLSN,
		&c.result.RedoLSN, &c.result.TimelineID, &nextXid, &c.result.OldestXid,
		&c.result.OldestActiveXid, &c.result.CheckpointTime); err != nil {
		log.Fatalf("pg_control_checkpoint() failed: %v", err)
	}

	if pos := strings.IndexByte(nextXid, ':'); pos > -1 {
		nextXid = nextXid[pos+1:]
	}
	if v, err := strconv.Atoi(nextXid); err != nil {
		log.Fatal("bad xid in pg_control_checkpoint()).next_xid")
	} else {
		c.result.NextXid = v
	}

	c.fixAuroraCheckpoint()
}

func (c *collector) fixAuroraCheckpoint() {
	// AWS Aurora reports {checkpoint,prior}_location as invalid LSNs. Reset
	// them to empty strings instead.
	if c.result.CheckpointLSN == "FFFFFFFF/FFFFFF00" || c.result.PriorLSN == "FFFFFFFF/FFFFFF00" {
		c.result.CheckpointLSN = ""
		c.result.RedoLSN = ""
		c.result.PriorLSN = ""
	}
}

func (c *collector) getActivityv96() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT COALESCE(datname, ''), COALESCE(usename, ''),
			COALESCE(application_name, ''), COALESCE(pid, 0),
			COALESCE(client_hostname::text, client_addr::text, ''),
			COALESCE(EXTRACT(EPOCH FROM backend_start)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM xact_start)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM query_start)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM state_change)::bigint, 0),
			COALESCE(wait_event_type, ''), COALESCE(wait_event, ''),
			COALESCE(state, ''), COALESCE(backend_xid::text, ''),
			COALESCE(backend_xmin::text, ''), LEFT(COALESCE(query, ''), $1),
			@queryid@
		  FROM pg_stat_activity`
	if c.version >= pgv14 { // query_id only in pg >= 14
		q = strings.Replace(q, `@queryid@`, `COALESCE(query_id, 0)`, 1)
	} else {
		q = strings.Replace(q, `@queryid@`, `0`, 1)
	}
	if c.version >= pgv10 {
		q += " WHERE backend_type='client backend'"
	}
	q += " ORDER BY pid ASC"
	rows, err := c.db.QueryContext(ctx, q, c.sqlLength)
	if err != nil {
		log.Fatalf("pg_stat_activity query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var b pgmetrics.Backend
		var backendXid, backendXmin string
		if err := rows.Scan(&b.DBName, &b.RoleName, &b.ApplicationName,
			&b.PID, &b.ClientAddr, &b.BackendStart, &b.XactStart, &b.QueryStart,
			&b.StateChange, &b.WaitEventType, &b.WaitEvent, &b.State,
			&backendXid, &backendXmin, &b.Query, &b.QueryID); err != nil {
			log.Fatalf("pg_stat_activity query failed: %v", err)
		}
		b.BackendXid, _ = strconv.Atoi(backendXid)
		b.BackendXmin, _ = strconv.Atoi(backendXmin)
		c.result.Backends = append(c.result.Backends, b)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_activity query failed: %v", err)
	}
}

func (c *collector) getActivityv94() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT COALESCE(datname, ''), COALESCE(usename, ''),
			COALESCE(application_name, ''), COALESCE(pid, 0),
			COALESCE(client_hostname::text, client_addr::text, ''),
			COALESCE(EXTRACT(EPOCH FROM backend_start)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM xact_start)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM query_start)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM state_change)::bigint, 0),
			COALESCE(waiting, FALSE),
			COALESCE(state, ''), COALESCE(backend_xid, ''),
			COALESCE(backend_xmin, ''), COALESCE(query, '')
		  FROM pg_stat_activity
		  ORDER BY pid ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_stat_activity query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var b pgmetrics.Backend
		var waiting bool
		if err := rows.Scan(&b.DBName, &b.RoleName, &b.ApplicationName,
			&b.PID, &b.ClientAddr, &b.BackendStart, &b.XactStart, &b.QueryStart,
			&b.StateChange, &waiting, &b.State,
			&b.BackendXid, &b.BackendXmin, &b.Query); err != nil {
			log.Fatalf("pg_stat_activity query failed: %v", err)
		}
		if waiting {
			b.WaitEvent = "waiting"
			b.WaitEventType = "waiting"
		}
		c.result.Backends = append(c.result.Backends, b)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_activity query failed: %v", err)
	}
}

func (c *collector) getActivityv93() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT COALESCE(datname, ''), COALESCE(usename, ''),
			COALESCE(application_name, ''), COALESCE(pid, 0),
			COALESCE(client_hostname::text, client_addr::text, ''),
			COALESCE(EXTRACT(EPOCH FROM backend_start)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM xact_start)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM query_start)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM state_change)::bigint, 0),
			COALESCE(waiting, FALSE),
			COALESCE(state, ''), COALESCE(query, '')
		  FROM pg_stat_activity
		  ORDER BY pid ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_stat_activity query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var b pgmetrics.Backend
		var waiting bool
		if err := rows.Scan(&b.DBName, &b.RoleName, &b.ApplicationName,
			&b.PID, &b.ClientAddr, &b.BackendStart, &b.XactStart, &b.QueryStart,
			&b.StateChange, &waiting, &b.State, &b.Query); err != nil {
			log.Fatalf("pg_stat_activity query failed: %v", err)
		}
		if waiting {
			b.WaitEvent = "waiting"
			b.WaitEventType = "waiting"
		}
		c.result.Backends = append(c.result.Backends, b)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_activity query failed: %v", err)
	}
}

func (c *collector) getBETypeCountsv10() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT backend_type, count(*) FROM pg_stat_activity GROUP BY backend_type`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_stat_activity query failed: %v", err)
	}
	defer rows.Close()

	m := make(map[string]int)
	for rows.Next() {
		var bt sql.NullString
		var count int
		if err := rows.Scan(&bt, &count); err != nil {
			log.Fatalf("pg_stat_activity query failed: %v", err)
		}
		m[bt.String] = count
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_activity query failed: %v", err)
	}

	if len(m) > 0 {
		c.result.BackendTypeCounts = m
	}
}

// fillSize - get and fill in the database size also
// onlyListed - only collect for the databases listed in 'dbList'
// dbList - list of database names for onlyListed
// also: if onlyListed is true but dbList is empty, assume dbList contains
//
//	the name of the currently connected database
func (c *collector) getDatabases(fillSize, onlyListed bool, dbList []string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// query template
	q := `SELECT D.oid, D.datname, D.datdba, D.dattablespace, D.datconnlimit,
			age(D.datfrozenxid), S.numbackends, S.xact_commit, S.xact_rollback,
			S.blks_read, S.blks_hit, S.tup_returned, S.tup_fetched,
			S.tup_inserted, S.tup_updated, S.tup_deleted, S.conflicts,
			S.temp_files, S.temp_bytes, S.deadlocks, S.blk_read_time,
			S.blk_write_time,
			COALESCE(EXTRACT(EPOCH FROM S.stats_reset)::bigint, 0),
			@checksum_failures@, @checksum_last_failure@,
			S.session_time, S.active_time, S.idle_in_transaction_time,
			S.sessions, S.sessions_abandoned, S.sessions_fatal, S.sessions_killed,
			S.parallel_workers_to_launch, S.parallel_workers_launched
		  FROM pg_database AS D JOIN pg_stat_database AS S
			ON D.oid = S.datid
		  WHERE (NOT D.datistemplate) @only@
		  ORDER BY D.oid ASC`

	// fill the only clause and arguments
	onlyClause := ""
	var args []interface{}
	if onlyListed {
		if len(dbList) > 0 {
			onlyClause = "AND (D.datname = any($1))"
			args = append(args, dbList)
		} else {
			onlyClause = "AND (D.datname = current_database())"
		}
	}
	q = strings.Replace(q, "@only@", onlyClause, 1)

	// adjust query for older versions
	if c.version < pgv12 { // checksum_failures and checksum_last_failure only in pg >= 12
		q = strings.Replace(q, `@checksum_failures@`, `0`, 1)
		q = strings.Replace(q, `@checksum_last_failure@`, `0`, 1)
	} else {
		q = strings.Replace(q, `@checksum_failures@`, `COALESCE(S.checksum_failures, 0)`, 1)
		q = strings.Replace(q, `@checksum_last_failure@`, `COALESCE(EXTRACT(EPOCH from S.checksum_last_failure)::bigint, 0)`, 1)
	}
	if c.version < pgv14 {
		// these columns only in pg >= 14
		q = strings.Replace(q, `S.session_time`, `0`, 1)
		q = strings.Replace(q, `S.active_time`, `0`, 1)
		q = strings.Replace(q, `S.idle_in_transaction_time`, `0`, 1)
		q = strings.Replace(q, `S.sessions`, `0`, 1)
		q = strings.Replace(q, `S.sessions_abandoned`, `0`, 1)
		q = strings.Replace(q, `S.sessions_fatal`, `0`, 1)
		q = strings.Replace(q, `S.sessions_killed`, `0`, 1)
	}
	if c.version < pgv18 {
		// these columns only in pg >= 18
		q = strings.Replace(q, `S.parallel_workers_to_launch`, `0`, 1)
		q = strings.Replace(q, `S.parallel_workers_launched`, `0`, 1)
	}

	// do the query
	rows, err := c.db.QueryContext(ctx, q, args...)
	if err != nil {
		log.Fatalf("pg_stat_database query failed: %v", err)
	}
	defer rows.Close()

	// collect the result
	for rows.Next() {
		var d pgmetrics.Database
		if err := rows.Scan(&d.OID, &d.Name, &d.DatDBA, &d.DatTablespace,
			&d.DatConnLimit, &d.AgeDatFrozenXid, &d.NumBackends, &d.XactCommit,
			&d.XactRollback, &d.BlksRead, &d.BlksHit, &d.TupReturned,
			&d.TupFetched, &d.TupInserted, &d.TupUpdated, &d.TupDeleted,
			&d.Conflicts, &d.TempFiles, &d.TempBytes, &d.Deadlocks,
			&d.BlkReadTime, &d.BlkWriteTime, &d.StatsReset, &d.ChecksumFailures,
			&d.ChecksumLastFailure, &d.SessionTime, &d.ActiveTime,
			&d.IdleInTxTime, &d.Sessions, &d.SessionsAbandoned,
			&d.SessionsFatal, &d.SessionsKilled, &d.ParallelWorkersToLaunch,
			&d.ParallelWorkersLaunched); err != nil {
			log.Fatalf("pg_stat_database query failed: %v", err)
		}
		d.Size = -1 // will be filled in later if asked for
		c.result.Databases = append(c.result.Databases, d)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_database query failed: %v", err)
	}

	// fill in the size if asked for
	if !fillSize {
		return
	}
	for i := range c.result.Databases {
		c.fillDatabaseSize(&c.result.Databases[i])
	}
}

func (c *collector) getTablespaces(fillSize bool) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT oid, spcname, pg_get_userbyid(spcowner),
			pg_tablespace_location(oid)
		  FROM pg_tablespace
		  ORDER BY oid ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_tablespace query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var t pgmetrics.Tablespace
		if err := rows.Scan(&t.OID, &t.Name, &t.Owner, &t.Location); err != nil {
			log.Fatalf("pg_tablespace query failed: %v", err)
		}
		t.Size = -1 // will be filled in later if asked for
		if (t.Name == "pg_default" || t.Name == "pg_global") && t.Location == "" {
			t.Location = c.dataDir
		}
		c.result.Tablespaces = append(c.result.Tablespaces, t)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_tablespace query failed: %v", err)
	}

	if !fillSize {
		return
	}
	for i := range c.result.Tablespaces {
		c.fillTablespaceSize(&c.result.Tablespaces[i])
	}
}

func (c *collector) getCurrentDatabase() (dbname string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT current_database()`
	if err := c.db.QueryRowContext(ctx, q).Scan(&dbname); err != nil {
		log.Fatalf("current_database failed: %v", err)
	}
	return
}

func (c *collector) getTables(fillSize bool) {
	err := c.getTablesNoRetry(fillSize)
	if err == nil {
		return
	}

	if isLockTimeoutError(err) && fillSize {
		// retry without call to pg_table_size
		log.Print("warning: lock timeout during pg_table_size, skipping table size collection")
		err = c.getTablesNoRetry(false)
	}

	if err != nil {
		log.Fatalf("pg_stat(io)_user_tables query failed: %v", err)
	}
}

func (c *collector) getTablesNoRetry(fillSize bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT S.relid, S.schemaname, S.relname, current_database(),
			S.seq_scan, S.seq_tup_read,
			COALESCE(S.idx_scan, 0), COALESCE(S.idx_tup_fetch, 0), S.n_tup_ins,
			S.n_tup_upd, S.n_tup_del, S.n_tup_hot_upd, S.n_live_tup,
			S.n_dead_tup, S.n_mod_since_analyze,
			COALESCE(EXTRACT(EPOCH FROM S.last_vacuum)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM S.last_autovacuum)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM S.last_analyze)::bigint, 0),
			COALESCE(EXTRACT(EPOCH FROM S.last_autoanalyze)::bigint, 0),
			S.vacuum_count, S.autovacuum_count, S.analyze_count,
			S.autoanalyze_count, IO.heap_blks_read, IO.heap_blks_hit,
			COALESCE(IO.idx_blks_read, 0), COALESCE(IO.idx_blks_hit, 0),
			COALESCE(IO.toast_blks_read, 0), COALESCE(IO.toast_blks_hit, 0),
			COALESCE(IO.tidx_blks_read, 0), COALESCE(IO.tidx_blks_hit, 0),
			C.relkind, C.relpersistence, C.relnatts, age(C.relfrozenxid),
			C.relispartition, C.reltablespace, COALESCE(array_to_string(C.relacl, E'\n'), ''),
			S.n_ins_since_vacuum,
			@last_seq_scan@, @last_idx_scan@, @n_tup_newpage_upd@,
            CASE WHEN $1 THEN COALESCE(pg_table_size(S.relid), -1) ELSE -1 END,
            @total_times@
		  FROM pg_stat_user_tables AS S
			JOIN pg_statio_user_tables AS IO
			ON S.relid = IO.relid
			JOIN pg_class AS C
			ON C.oid = S.relid
		  ORDER BY S.relid ASC`
	if c.version < pgv94 { // n_mod_since_analyze only in v9.4+
		q = strings.Replace(q, "S.n_mod_since_analyze", "0", 1)
	}
	if c.version < pgv10 { // relispartition only in v10+
		q = strings.Replace(q, "C.relispartition", "false", 1)
	}
	if c.version < pgv13 { // n_ins_since_vacuum only in pg >= 13
		q = strings.Replace(q, "S.n_ins_since_vacuum", "0", 1)
	}
	if c.version < pgv16 { // last_seq_scan, last_idx_scan, n_tup_newpage_upd only in pg >= 16
		q = strings.Replace(q, "@last_seq_scan@", "0", 1)
		q = strings.Replace(q, "@last_idx_scan@", "0", 1)
		q = strings.Replace(q, "@n_tup_newpage_upd@", "0", 1)
	} else {
		q = strings.Replace(q, "@last_seq_scan@", "COALESCE(EXTRACT(EPOCH FROM S.last_seq_scan)::bigint, 0)", 1)
		q = strings.Replace(q, "@last_idx_scan@", "COALESCE(EXTRACT(EPOCH FROM S.last_idx_scan)::bigint, 0)", 1)
		q = strings.Replace(q, "@n_tup_newpage_upd@", "COALESCE(S.n_tup_newpage_upd, 0)", 1)
	}
	if c.version < pgv18 { // total_{auto,}{analyze,vacuum}_time only in pg >= 18
		q = strings.Replace(q, "@total_times@", "0, 0, 0, 0", 1)
	} else {
		q = strings.Replace(q, "@total_times@",
			"S.total_vacuum_time, S.total_autovacuum_time, S.total_analyze_time, S.total_autoanalyze_time",
			1)
	}
	rows, err := c.db.QueryContext(ctx, q, fillSize)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var t pgmetrics.Table
		var tblspcOID int
		if err := rows.Scan(&t.OID, &t.SchemaName, &t.Name, &t.DBName,
			&t.SeqScan, &t.SeqTupRead, &t.IdxScan, &t.IdxTupFetch, &t.NTupIns,
			&t.NTupUpd, &t.NTupDel, &t.NTupHotUpd, &t.NLiveTup, &t.NDeadTup,
			&t.NModSinceAnalyze, &t.LastVacuum, &t.LastAutovacuum,
			&t.LastAnalyze, &t.LastAutoanalyze, &t.VacuumCount,
			&t.AutovacuumCount, &t.AnalyzeCount, &t.AutoanalyzeCount,
			&t.HeapBlksRead, &t.HeapBlksHit, &t.IdxBlksRead, &t.IdxBlksHit,
			&t.ToastBlksRead, &t.ToastBlksHit, &t.TidxBlksRead, &t.TidxBlksHit,
			&t.RelKind, &t.RelPersistence, &t.RelNAtts, &t.AgeRelFrozenXid,
			&t.RelIsPartition, &tblspcOID, &t.ACL, &t.NInsSinceVacuum,
			&t.LastSeqScan, &t.LastIdxScan, &t.NTupNewpageUpd,
			&t.Size, &t.TotalVacuumTime, &t.TotalAutovacuumTime,
			&t.TotalAnalyzeTime, &t.TotalAutoanalyzeTime); err != nil {
			return err
		}
		t.Bloat = -1 // will be filled in later
		if tblspcOID != 0 {
			for _, ts := range c.result.Tablespaces {
				if ts.OID == tblspcOID {
					t.TablespaceName = ts.Name
					break
				}
			}
		}
		if c.tableOK(t.SchemaName, t.Name) {
			c.result.Tables = append(c.result.Tables, t)
		}
	}
	return rows.Err()
}

func isLockTimeoutError(err error) bool {
	if pgerr, ok := err.(*pgconn.PgError); ok {
		// see https://www.postgresql.org/docs/current/errcodes-appendix.html
		return pgerr.Code == "55P03"
	}
	return false
}

func (c *collector) getIndexes(fillSize bool) {
	err := c.getIndexesNoRetry(fillSize)
	if err == nil {
		return
	}

	if isLockTimeoutError(err) && fillSize {
		// retry without call to pg_total_relation_size
		log.Print("warning: lock timeout during pg_total_relation_size, skipping index size collection")
		err = c.getIndexesNoRetry(false)
	}

	if err != nil {
		log.Fatalf("pg_stat_user_indexes query failed: %v", err)
	}
}

func (c *collector) getIndexesNoRetry(fillSize bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT S.relid, S.indexrelid, S.schemaname, S.relname, S.indexrelname,
			current_database(), S.idx_scan, S.idx_tup_read, S.idx_tup_fetch,
			pg_stat_get_blocks_fetched(S.indexrelid) - pg_stat_get_blocks_hit(S.indexrelid) AS idx_blks_read,
			pg_stat_get_blocks_hit(S.indexrelid) AS idx_blks_hit,
			C.relnatts, AM.amname, C.reltablespace, @last_idx_scan@,
			CASE WHEN $1 THEN COALESCE(pg_total_relation_size(S.indexrelid), -1) ELSE -1 END
		FROM pg_stat_user_indexes AS S
			JOIN pg_class AS C
			ON S.indexrelid = C.oid
			JOIN pg_am AS AM
			ON C.relam = AM.oid
		ORDER BY S.relid ASC`
	if c.version < pgv16 { // last_idx_scan only in pg >= 16
		q = strings.Replace(q, "@last_idx_scan@", "0", 1)
	} else {
		q = strings.Replace(q, "@last_idx_scan@", "COALESCE(EXTRACT(EPOCH FROM S.last_idx_scan)::bigint, 0)", 1)
	}
	rows, err := c.db.QueryContext(ctx, q, fillSize)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var idx pgmetrics.Index
		var tblspcOID int
		if err := rows.Scan(&idx.TableOID, &idx.OID, &idx.SchemaName,
			&idx.TableName, &idx.Name, &idx.DBName, &idx.IdxScan,
			&idx.IdxTupRead, &idx.IdxTupFetch, &idx.IdxBlksRead,
			&idx.IdxBlksHit, &idx.RelNAtts, &idx.AMName, &tblspcOID,
			&idx.LastIdxScan, &idx.Size); err != nil {
			return err
		}
		idx.Bloat = -1 // will be filled in later
		if tblspcOID != 0 {
			for _, ts := range c.result.Tablespaces {
				if ts.OID == tblspcOID {
					idx.TablespaceName = ts.Name
					break
				}
			}
		}
		if c.tableOK(idx.SchemaName, idx.TableName) {
			c.result.Indexes = append(c.result.Indexes, idx)
		}
	}
	return rows.Err()
}

// getIndexDef gets the definition of all indexes. Used to be collected along
// with getIndexes(), but pg_get_indexdef() will wait for access exclusive
// locks on the table in question. By collecting this separately, we'll
// silently fail the collection of just the index defs, and let the collection
// of index stats succeed.
func (c *collector) getIndexDef() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT indexrelid, pg_get_indexdef(indexrelid) FROM pg_stat_user_indexes`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return // ignore errors silently, ok to fail to get the defn
	}
	defer rows.Close()

	for rows.Next() {
		var oid int
		var defn string
		if err := rows.Scan(&oid, &defn); err != nil {
			break // abort silently
		}
		for i := range c.result.Indexes {
			if c.result.Indexes[i].OID == oid {
				c.result.Indexes[i].Definition = defn
				break
			}
		}
	}
	// ignore checking for rows.Err()
}

func (c *collector) getSequences() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT relid, schemaname, relname, current_database(), blks_read,
			blks_hit
		  FROM pg_statio_user_sequences
		  ORDER BY relid ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_statio_user_sequences query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var s pgmetrics.Sequence
		if err := rows.Scan(&s.OID, &s.SchemaName, &s.Name, &s.DBName,
			&s.BlksRead, &s.BlksHit); err != nil {
			log.Fatalf("pg_statio_user_sequences query failed: %v", err)
		}
		if c.schemaOK(s.SchemaName) {
			c.result.Sequences = append(c.result.Sequences, s)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_statio_user_sequences query failed: %v", err)
	}
}

func (c *collector) getUserFunctions() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT funcid, schemaname, funcname, current_database(), calls,
			total_time, self_time
		  FROM pg_stat_user_functions
		  ORDER BY funcid ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_stat_user_functions query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var f pgmetrics.UserFunction
		if err := rows.Scan(&f.OID, &f.SchemaName, &f.Name, &f.DBName,
			&f.Calls, &f.TotalTime, &f.SelfTime); err != nil {
			log.Fatalf("pg_stat_user_functions query failed: %v", err)
		}
		if c.schemaOK(f.SchemaName) {
			c.result.UserFunctions = append(c.result.UserFunctions, f)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_user_functions query failed: %v", err)
	}
}

func (c *collector) getVacuumProgressv17() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pid, datname, COALESCE(relid, 0), COALESCE(phase, ''),
			COALESCE(heap_blks_total, 0), COALESCE(heap_blks_scanned, 0),
			COALESCE(heap_blks_vacuumed, 0), COALESCE(index_vacuum_count, 0),
			COALESCE(max_dead_tuple_bytes, 0), COALESCE(dead_tuple_bytes, 0),
			COALESCE(num_dead_item_ids, 0), COALESCE(indexes_total, 0),
			COALESCE(indexes_processed, 0), @delay_time@
		  FROM pg_stat_progress_vacuum
		  ORDER BY pid ASC`
	if c.version < pgv18 { // delay_time only in pg >= 18
		q = strings.Replace(q, "@delay_time@", "0", 1)
	} else {
		q = strings.Replace(q, "@delay_time@", "COALESCE(delay_time, 0)", 1)
	}

	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_stat_progress_vacuum query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var p pgmetrics.VacuumProgressBackend
		if err := rows.Scan(&p.PID, &p.DBName, &p.TableOID, &p.Phase, &p.HeapBlksTotal,
			&p.HeapBlksScanned, &p.HeapBlksVacuumed, &p.IndexVacuumCount,
			&p.MaxDeadTupleBytes, &p.DeadTupleBytes, &p.NumDeadItemIDs,
			&p.IndexesTotal, &p.IndexesProcessed, &p.DelayTime); err != nil {
			log.Fatalf("pg_stat_progress_vacuum query failed: %v", err)
		}
		if t := c.result.TableByOID(p.TableOID); t != nil {
			p.TableName = t.Name
		}
		c.result.VacuumProgress = append(c.result.VacuumProgress, p)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_progress_vacuum query failed: %v", err)
	}
}

func (c *collector) getVacuumProgressv96() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pid, datname, COALESCE(relid, 0), COALESCE(phase, ''),
			COALESCE(heap_blks_total, 0), COALESCE(heap_blks_scanned, 0),
			COALESCE(heap_blks_vacuumed, 0), COALESCE(index_vacuum_count, 0),
			COALESCE(max_dead_tuples, 0), COALESCE(num_dead_tuples, 0)
		  FROM pg_stat_progress_vacuum
		  ORDER BY pid ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_stat_progress_vacuum query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var p pgmetrics.VacuumProgressBackend
		if err := rows.Scan(&p.PID, &p.DBName, &p.TableOID, &p.Phase, &p.HeapBlksTotal,
			&p.HeapBlksScanned, &p.HeapBlksVacuumed, &p.IndexVacuumCount,
			&p.MaxDeadTuples, &p.NumDeadTuples); err != nil {
			log.Fatalf("pg_stat_progress_vacuum query failed: %v", err)
		}
		if t := c.result.TableByOID(p.TableOID); t != nil {
			p.TableName = t.Name
		}
		c.result.VacuumProgress = append(c.result.VacuumProgress, p)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_progress_vacuum query failed: %v", err)
	}
}

func (c *collector) getExtensions() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT e.name AS name, current_database(),
		  	COALESCE(e.default_version, ''),
		  	x.extversion,
		  	COALESCE(e.comment, ''),
		  	x.extnamespace::regnamespace
		  FROM pg_available_extensions() e(name, default_version, comment)
		  LEFT JOIN pg_extension x ON e.name = x.extname
		  WHERE x.extversion IS NOT NULL
		  ORDER BY name ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_available_extensions query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var e pgmetrics.Extension
		if err := rows.Scan(&e.Name, &e.DBName, &e.DefaultVersion,
			&e.InstalledVersion, &e.Comment, &e.SchemaName); err != nil {
			log.Fatalf("pg_available_extensions query failed: %v", err)
		}
		c.result.Extensions = append(c.result.Extensions, e)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_available_extensions query failed: %v", err)
	}
}

func (c *collector) getRoles() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT R.oid, R.rolname, R.rolsuper, R.rolinherit, R.rolcreaterole,
			R.rolcreatedb, R.rolcanlogin, R.rolreplication, R.rolbypassrls,
			R.rolconnlimit,
			COALESCE(EXTRACT(EPOCH FROM R.rolvaliduntil), 0),
			ARRAY(SELECT pg_get_userbyid(M.roleid) FROM pg_auth_members AS M
					WHERE M.member = R.oid)
		  FROM pg_roles AS R
		  ORDER BY R.oid ASC`
	if c.version < pgv95 { // RLS is only available in v9.5+
		q = strings.Replace(q, "R.rolbypassrls", "FALSE", 1)
	}
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_roles/pg_auth_members query failed: %v", err)
	}
	defer rows.Close()

	m := pgtype.NewMap()
	for rows.Next() {
		var r pgmetrics.Role
		var validUntil float64
		if err := rows.Scan(&r.OID, &r.Name, &r.Rolsuper, &r.Rolinherit,
			&r.Rolcreaterole, &r.Rolcreatedb, &r.Rolcanlogin, &r.Rolreplication,
			&r.Rolbypassrls, &r.Rolconnlimit, &validUntil,
			m.SQLScanner(&r.MemberOf)); err != nil {
			log.Fatalf("pg_roles/pg_auth_members query failed: %v", err)
		}
		if !math.IsInf(validUntil, 0) {
			r.Rolvaliduntil = int64(validUntil)
		}
		c.result.Roles = append(c.result.Roles, r)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_roles/pg_auth_members query failed: %v", err)
	}
}

func (c *collector) getReplicationSlotsv94() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT slot_name, COALESCE(plugin, ''), slot_type,
			COALESCE(database, ''), active, xmin, catalog_xmin,
			restart_lsn, confirmed_flush_lsn, temporary,
			@wal_status@, @safe_wal_size@, two_phase, @conflicting@
		  FROM pg_replication_slots
		  ORDER BY slot_name ASC`
	if c.version < pgv96 { // confirmed_flush_lsn only in pg >= 9.6
		q = strings.Replace(q, "confirmed_flush_lsn", `''`, 1)
	}
	if c.version < pgv10 { // temporary only in pg >= 10
		q = strings.Replace(q, "temporary", `FALSE`, 1)
	}
	if c.version < pgv13 { // wal_status and safe_wal_size only in pg >= 13
		q = strings.Replace(q, "@wal_status@", `''`, 1)
		q = strings.Replace(q, "@safe_wal_size@", `0::bigint`, 1)
	} else {
		q = strings.Replace(q, "@wal_status@", `COALESCE(wal_status, '')`, 1)
		q = strings.Replace(q, "@safe_wal_size@", `COALESCE(safe_wal_size, 0)::bigint`, 1)
	}
	if c.version < pgv14 { // two_phase only in pg >= 14
		q = strings.Replace(q, "two_phase", `FALSE`, 1)
	}
	if c.version < pgv16 { // conflicting only in pg >= 16
		q = strings.Replace(q, "@conflicting@", `FALSE`, 1)
	} else {
		q = strings.Replace(q, "@conflicting@", `COALESCE(conflicting, FALSE)`, 1)
	}
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: pg_replication_slots query failed: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var rs pgmetrics.ReplicationSlot
		var xmin, cXmin sql.NullInt64
		var rlsn, cflsn sql.NullString
		if err := rows.Scan(&rs.SlotName, &rs.Plugin, &rs.SlotType,
			&rs.DBName, &rs.Active, &xmin, &cXmin, &rlsn, &cflsn,
			&rs.Temporary, &rs.WALStatus, &rs.SafeWALSize, &rs.TwoPhase,
			&rs.Conflicting); err != nil {
			log.Fatalf("pg_replication_slots query failed: %v", err)
		}
		rs.Xmin = int(xmin.Int64)
		rs.CatalogXmin = int(cXmin.Int64)
		rs.RestartLSN = rlsn.String
		rs.ConfirmedFlushLSN = cflsn.String
		c.result.ReplicationSlots = append(c.result.ReplicationSlots, rs)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_replication_slots query failed: %v", err)
	}
}

func (c *collector) getDisabledTriggers() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT T.oid, T.tgrelid, T.tgname, P.proname
		  FROM pg_trigger AS T JOIN pg_proc AS P ON T.tgfoid = P.oid
		  WHERE tgenabled = 'D'
		  ORDER BY T.oid ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_trigger/pg_proc query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tg pgmetrics.Trigger
		var tgrelid int
		if err := rows.Scan(&tg.OID, &tgrelid, &tg.Name, &tg.ProcName); err != nil {
			log.Fatalf("pg_trigger/pg_proc query failed: %v", err)
		}
		if t := c.result.TableByOID(tgrelid); t != nil {
			tg.DBName = t.DBName
			tg.SchemaName = t.SchemaName
			tg.TableName = t.Name
		}
		if c.schemaOK(tg.SchemaName) {
			c.result.DisabledTriggers = append(c.result.DisabledTriggers, tg)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_trigger/pg_proc query failed: %v", err)
	}
}

func (c *collector) getStatements(currdb string) {
	// Even if PSS is installed only in one database, querying it gives queries
	// from across all databases. Fetching this information once is enough.
	if len(c.result.Statements) > 0 {
		return
	}

	// Try to fetch only if PSS extension is installed.
	var version string
	var schema string
	for _, e := range c.result.Extensions {
		if e.Name == "pg_stat_statements" && e.DBName == currdb {
			version = "v" + e.InstalledVersion
			schema = e.SchemaName
			break
		}
	}
	if !semver.IsValid(version) {
		return
	}

	// Collect based on pss version, not pg version. This allows for cases when
	// postgres is upgraded, but not the extension.
	if semver.Compare(version, "v1.12") >= 0 { // pg v18
		c.getStatementsv112(schema)
	} else if semver.Compare(version, "v1.11") >= 0 { // pg v17
		c.getStatementsv111(schema)
	} else if semver.Compare(version, "v1.10") >= 0 { // pg v15, pg v16
		c.getStatementsv110(schema)
	} else if semver.Compare(version, "v1.9") >= 0 { // pg v14
		c.getStatementsv19(schema)
	} else if semver.Compare(version, "v1.8") >= 0 { // pg v13
		c.getStatementsv18(schema)
	} else {
		c.getStatementsPrev18(schema)
	}
}

func (c *collector) getStatementsv112(schema string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT userid, dbid, queryid, LEFT(COALESCE(query, ''), $1), calls,
			total_exec_time, min_exec_time, max_exec_time, stddev_exec_time,
			rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied,
			shared_blks_written, local_blks_hit, local_blks_read,
			local_blks_dirtied, local_blks_written, temp_blks_read,
			temp_blks_written, shared_blk_read_time, shared_blk_write_time,
			plans, total_plan_time, min_plan_time, max_plan_time,
			stddev_plan_time, wal_records, wal_fpi, wal_bytes::bigint,
			toplevel, temp_blk_read_time, temp_blk_write_time,
			jit_functions, jit_generation_time, jit_inlining_count,
			jit_inlining_time, jit_optimization_count, jit_optimization_time,
			jit_emission_count, jit_emission_time, local_blk_read_time,
			local_blk_write_time, jit_deform_count, jit_deform_time,
			stats_since, minmax_stats_since, wal_buffers_full,
			parallel_workers_to_launch, parallel_workers_launched
		  FROM @schema@.pg_stat_statements
		  ORDER BY total_exec_time DESC
		  LIMIT $2`
	q = strings.Replace(q, "@schema@", schema, -1)
	rows, err := c.db.QueryContext(ctx, q, c.sqlLength, c.stmtsLimit)
	if err != nil {
		log.Printf("warning: pg_stat_statements query failed: %v", err)
		return
	}
	defer rows.Close()

	c.result.Statements = make([]pgmetrics.Statement, 0, c.stmtsLimit)
	for rows.Next() {
		var s pgmetrics.Statement
		var queryID sql.NullInt64
		var statsSince, minMaxStatsSince time.Time
		if err := rows.Scan(&s.UserOID, &s.DBOID, &queryID, &s.Query,
			&s.Calls, &s.TotalTime, &s.MinTime, &s.MaxTime, &s.StddevTime,
			&s.Rows, &s.SharedBlksHit, &s.SharedBlksRead, &s.SharedBlksDirtied,
			&s.SharedBlksWritten, &s.LocalBlksHit, &s.LocalBlksRead,
			&s.LocalBlksDirtied, &s.LocalBlksWritten, &s.TempBlksRead,
			&s.TempBlksWritten, &s.BlkReadTime, &s.BlkWriteTime, &s.Plans,
			&s.TotalPlanTime, &s.MinPlanTime, &s.MaxPlanTime, &s.StddevPlanTime,
			&s.WALRecords, &s.WALFPI, &s.WALBytes, &s.TopLevel,
			&s.TempBlkReadTime, &s.TempBlkWriteTime, &s.JITFuntions,
			&s.JITGenerationTime, &s.JITInliningCount, &s.JITInliningTime,
			&s.JITOptimizationCount, &s.JITOptimizationTime, &s.JITEmissionCount,
			&s.JITEmissionTime, &s.LocalBlkReadTime, &s.LocalBlkWriteTime,
			&s.JITDeformCount, &s.JITDeformTime, &statsSince, &minMaxStatsSince,
			&s.WALBuffersFull, &s.ParallelWorkersToLaunch, &s.ParallelWorkersLaunched,
		); err != nil {
			log.Fatalf("pg_stat_statements scan failed: %v", err)
		}
		// UserName
		if r := c.result.RoleByOID(s.UserOID); r != nil {
			s.UserName = r.Name
		}
		// DBName
		if d := c.result.DatabaseByOID(s.DBOID); d != nil {
			s.DBName = d.Name
		}
		// Query ID, set to 0 if null
		s.QueryID = queryID.Int64
		// StatsSince
		s.StatsSince = statsSince.Unix()
		// MinMaxStatsSince
		s.MinMaxStatsSince = minMaxStatsSince.Unix()
		c.result.Statements = append(c.result.Statements, s)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_statements failed: %v", err)
	}
}

func (c *collector) getStatementsv111(schema string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT userid, dbid, queryid, LEFT(COALESCE(query, ''), $1), calls,
			total_exec_time, min_exec_time, max_exec_time, stddev_exec_time,
			rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied,
			shared_blks_written, local_blks_hit, local_blks_read,
			local_blks_dirtied, local_blks_written, temp_blks_read,
			temp_blks_written, shared_blk_read_time, shared_blk_write_time,
			plans, total_plan_time, min_plan_time, max_plan_time,
			stddev_plan_time, wal_records, wal_fpi, wal_bytes::bigint,
			toplevel, temp_blk_read_time, temp_blk_write_time,
			jit_functions, jit_generation_time, jit_inlining_count,
			jit_inlining_time, jit_optimization_count, jit_optimization_time,
			jit_emission_count, jit_emission_time, local_blk_read_time,
			local_blk_write_time, jit_deform_count, jit_deform_time,
			stats_since, minmax_stats_since
		  FROM @schema@.pg_stat_statements
		  ORDER BY total_exec_time DESC
		  LIMIT $2`
	q = strings.Replace(q, "@schema@", schema, -1)
	rows, err := c.db.QueryContext(ctx, q, c.sqlLength, c.stmtsLimit)
	if err != nil {
		log.Printf("warning: pg_stat_statements query failed: %v", err)
		return
	}
	defer rows.Close()

	c.result.Statements = make([]pgmetrics.Statement, 0, c.stmtsLimit)
	for rows.Next() {
		var s pgmetrics.Statement
		var queryID sql.NullInt64
		var statsSince, minMaxStatsSince time.Time
		if err := rows.Scan(&s.UserOID, &s.DBOID, &queryID, &s.Query,
			&s.Calls, &s.TotalTime, &s.MinTime, &s.MaxTime, &s.StddevTime,
			&s.Rows, &s.SharedBlksHit, &s.SharedBlksRead, &s.SharedBlksDirtied,
			&s.SharedBlksWritten, &s.LocalBlksHit, &s.LocalBlksRead,
			&s.LocalBlksDirtied, &s.LocalBlksWritten, &s.TempBlksRead,
			&s.TempBlksWritten, &s.BlkReadTime, &s.BlkWriteTime, &s.Plans,
			&s.TotalPlanTime, &s.MinPlanTime, &s.MaxPlanTime, &s.StddevPlanTime,
			&s.WALRecords, &s.WALFPI, &s.WALBytes, &s.TopLevel,
			&s.TempBlkReadTime, &s.TempBlkWriteTime, &s.JITFuntions,
			&s.JITGenerationTime, &s.JITInliningCount, &s.JITInliningTime,
			&s.JITOptimizationCount, &s.JITOptimizationTime, &s.JITEmissionCount,
			&s.JITEmissionTime, &s.LocalBlkReadTime, &s.LocalBlkWriteTime,
			&s.JITDeformCount, &s.JITDeformTime, &statsSince, &minMaxStatsSince,
		); err != nil {
			log.Fatalf("pg_stat_statements scan failed: %v", err)
		}
		// UserName
		if r := c.result.RoleByOID(s.UserOID); r != nil {
			s.UserName = r.Name
		}
		// DBName
		if d := c.result.DatabaseByOID(s.DBOID); d != nil {
			s.DBName = d.Name
		}
		// Query ID, set to 0 if null
		s.QueryID = queryID.Int64
		// StatsSince
		s.StatsSince = statsSince.Unix()
		// MinMaxStatsSince
		s.MinMaxStatsSince = minMaxStatsSince.Unix()
		c.result.Statements = append(c.result.Statements, s)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_statements failed: %v", err)
	}
}

func (c *collector) getStatementsv110(schema string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT userid, dbid, queryid, LEFT(COALESCE(query, ''), $1), calls,
			total_exec_time, min_exec_time, max_exec_time, stddev_exec_time,
			rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied,
			shared_blks_written, local_blks_hit, local_blks_read,
			local_blks_dirtied, local_blks_written, temp_blks_read,
			temp_blks_written, blk_read_time, blk_write_time,
			plans, total_plan_time, min_plan_time, max_plan_time,
			stddev_plan_time, wal_records, wal_fpi, wal_bytes::bigint,
			toplevel, temp_blk_read_time, temp_blk_write_time,
			jit_functions, jit_generation_time, jit_inlining_count,
			jit_inlining_time, jit_optimization_count, jit_optimization_time,
			jit_emission_count, jit_emission_time
		  FROM @schema@.pg_stat_statements
		  ORDER BY total_exec_time DESC
		  LIMIT $2`
	q = strings.Replace(q, "@schema@", schema, -1)
	rows, err := c.db.QueryContext(ctx, q, c.sqlLength, c.stmtsLimit)
	if err != nil {
		log.Printf("warning: pg_stat_statements query failed: %v", err)
		return
	}
	defer rows.Close()

	c.result.Statements = make([]pgmetrics.Statement, 0, c.stmtsLimit)
	for rows.Next() {
		var s pgmetrics.Statement
		var queryID sql.NullInt64
		if err := rows.Scan(&s.UserOID, &s.DBOID, &queryID, &s.Query,
			&s.Calls, &s.TotalTime, &s.MinTime, &s.MaxTime, &s.StddevTime,
			&s.Rows, &s.SharedBlksHit, &s.SharedBlksRead, &s.SharedBlksDirtied,
			&s.SharedBlksWritten, &s.LocalBlksHit, &s.LocalBlksRead,
			&s.LocalBlksDirtied, &s.LocalBlksWritten, &s.TempBlksRead,
			&s.TempBlksWritten, &s.BlkReadTime, &s.BlkWriteTime, &s.Plans,
			&s.TotalPlanTime, &s.MinPlanTime, &s.MaxPlanTime, &s.StddevPlanTime,
			&s.WALRecords, &s.WALFPI, &s.WALBytes, &s.TopLevel,
			&s.TempBlkReadTime, &s.TempBlkWriteTime, &s.JITFuntions,
			&s.JITGenerationTime, &s.JITInliningCount, &s.JITInliningTime,
			&s.JITOptimizationCount, &s.JITOptimizationTime, &s.JITEmissionCount,
			&s.JITEmissionTime); err != nil {
			log.Fatalf("pg_stat_statements scan failed: %v", err)
		}
		// UserName
		if r := c.result.RoleByOID(s.UserOID); r != nil {
			s.UserName = r.Name
		}
		// DBName
		if d := c.result.DatabaseByOID(s.DBOID); d != nil {
			s.DBName = d.Name
		}
		// Query ID, set to 0 if null
		s.QueryID = queryID.Int64
		c.result.Statements = append(c.result.Statements, s)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_statements failed: %v", err)
	}
}

func (c *collector) getStatementsv19(schema string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT userid, dbid, queryid, LEFT(COALESCE(query, ''), $1), calls,
			total_exec_time, min_exec_time, max_exec_time, stddev_exec_time,
			rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied,
			shared_blks_written, local_blks_hit, local_blks_read,
			local_blks_dirtied, local_blks_written, temp_blks_read,
			temp_blks_written, blk_read_time, blk_write_time,
			plans, total_plan_time, min_plan_time, max_plan_time,
			stddev_plan_time, wal_records, wal_fpi, wal_bytes::bigint,
			toplevel
		  FROM @schema@.pg_stat_statements
		  ORDER BY total_exec_time DESC
		  LIMIT $2`
	q = strings.Replace(q, "@schema@", schema, -1)
	rows, err := c.db.QueryContext(ctx, q, c.sqlLength, c.stmtsLimit)
	if err != nil {
		log.Printf("warning: pg_stat_statements query failed: %v", err)
		return
	}
	defer rows.Close()

	c.result.Statements = make([]pgmetrics.Statement, 0, c.stmtsLimit)
	for rows.Next() {
		var s pgmetrics.Statement
		var queryID sql.NullInt64
		if err := rows.Scan(&s.UserOID, &s.DBOID, &queryID, &s.Query,
			&s.Calls, &s.TotalTime, &s.MinTime, &s.MaxTime, &s.StddevTime,
			&s.Rows, &s.SharedBlksHit, &s.SharedBlksRead, &s.SharedBlksDirtied,
			&s.SharedBlksWritten, &s.LocalBlksHit, &s.LocalBlksRead,
			&s.LocalBlksDirtied, &s.LocalBlksWritten, &s.TempBlksRead,
			&s.TempBlksWritten, &s.BlkReadTime, &s.BlkWriteTime, &s.Plans,
			&s.TotalPlanTime, &s.MinPlanTime, &s.MaxPlanTime, &s.StddevPlanTime,
			&s.WALRecords, &s.WALFPI, &s.WALBytes, &s.TopLevel); err != nil {
			log.Fatalf("pg_stat_statements scan failed: %v", err)
		}
		// UserName
		if r := c.result.RoleByOID(s.UserOID); r != nil {
			s.UserName = r.Name
		}
		// DBName
		if d := c.result.DatabaseByOID(s.DBOID); d != nil {
			s.DBName = d.Name
		}
		// Query ID, set to 0 if null
		s.QueryID = queryID.Int64
		c.result.Statements = append(c.result.Statements, s)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_statements failed: %v", err)
	}
}

func (c *collector) getStatementsv18(schema string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT userid, dbid, queryid, LEFT(COALESCE(query, ''), $1), calls,
			total_exec_time, min_exec_time, max_exec_time, stddev_exec_time,
			rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied,
			shared_blks_written, local_blks_hit, local_blks_read,
			local_blks_dirtied, local_blks_written, temp_blks_read,
			temp_blks_written, blk_read_time, blk_write_time,
			plans, total_plan_time, min_plan_time, max_plan_time,
			stddev_plan_time, wal_records, wal_fpi, wal_bytes::bigint
		  FROM @schema@.pg_stat_statements
		  ORDER BY total_exec_time DESC
		  LIMIT $2`
	q = strings.Replace(q, "@schema@", schema, -1)
	rows, err := c.db.QueryContext(ctx, q, c.sqlLength, c.stmtsLimit)
	if err != nil {
		log.Printf("warning: pg_stat_statements query failed: %v", err)
		return
	}
	defer rows.Close()

	c.result.Statements = make([]pgmetrics.Statement, 0, c.stmtsLimit)
	for rows.Next() {
		var s pgmetrics.Statement
		var queryID sql.NullInt64
		if err := rows.Scan(&s.UserOID, &s.DBOID, &queryID, &s.Query,
			&s.Calls, &s.TotalTime, &s.MinTime, &s.MaxTime, &s.StddevTime,
			&s.Rows, &s.SharedBlksHit, &s.SharedBlksRead, &s.SharedBlksDirtied,
			&s.SharedBlksWritten, &s.LocalBlksHit, &s.LocalBlksRead,
			&s.LocalBlksDirtied, &s.LocalBlksWritten, &s.TempBlksRead,
			&s.TempBlksWritten, &s.BlkReadTime, &s.BlkWriteTime, &s.Plans,
			&s.TotalPlanTime, &s.MinPlanTime, &s.MaxPlanTime, &s.StddevPlanTime,
			&s.WALRecords, &s.WALFPI, &s.WALBytes); err != nil {
			log.Fatalf("pg_stat_statements scan failed: %v", err)
		}
		// UserName
		if r := c.result.RoleByOID(s.UserOID); r != nil {
			s.UserName = r.Name
		}
		// DBName
		if d := c.result.DatabaseByOID(s.DBOID); d != nil {
			s.DBName = d.Name
		}
		// Query ID, set to 0 if null
		s.QueryID = queryID.Int64
		c.result.Statements = append(c.result.Statements, s)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_statements failed: %v", err)
	}
}

func (c *collector) getStatementsPrev18(schema string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT userid, dbid, queryid, LEFT(COALESCE(query, ''), $1), calls, total_time,
			min_time, max_time, stddev_time, rows, shared_blks_hit,
			shared_blks_read, shared_blks_dirtied, shared_blks_written,
			local_blks_hit, local_blks_read, local_blks_dirtied,
			local_blks_written, temp_blks_read, temp_blks_written,
			blk_read_time, blk_write_time
		  FROM @schema@.pg_stat_statements
		  ORDER BY total_time DESC
		  LIMIT $2`
	q = strings.Replace(q, "@schema@", schema, -1)
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf(q, schema), c.sqlLength, c.stmtsLimit)
	if err != nil {
		// If we get an error about "min_time" we probably have an old (v1.2)
		// version of pg_stat_statements which does not have min_time, max_time
		// and stddev_time. Even older versions (1.1 and below) do not have
		// queryid, but we don't support that (postgres v9.3 and below).
		// We can't check the extension version upfront since it might not have
		// been collected (--omit=extensions).
		if strings.Contains(err.Error(), "min_time") {
			q = strings.Replace(q, "min_time", "0", 1)
			q = strings.Replace(q, "max_time", "0", 1)
			q = strings.Replace(q, "stddev_time", "0", 1)
			rows, err = c.db.QueryContext(ctx, q, c.sqlLength, c.stmtsLimit)
		}
		// If we still have errors, silently give up on querying
		// pg_stat_statements.
		if err != nil {
			log.Printf("warning: pg_stat_statements query failed: %v", err)
			return
		}
	}
	defer rows.Close()

	c.result.Statements = make([]pgmetrics.Statement, 0, c.stmtsLimit)
	for rows.Next() {
		var s pgmetrics.Statement
		var queryID sql.NullInt64
		if err := rows.Scan(&s.UserOID, &s.DBOID, &queryID, &s.Query,
			&s.Calls, &s.TotalTime, &s.MinTime, &s.MaxTime, &s.StddevTime,
			&s.Rows, &s.SharedBlksHit, &s.SharedBlksRead, &s.SharedBlksDirtied,
			&s.SharedBlksWritten, &s.LocalBlksHit, &s.LocalBlksRead,
			&s.LocalBlksDirtied, &s.LocalBlksWritten, &s.TempBlksRead,
			&s.TempBlksWritten, &s.BlkReadTime, &s.BlkWriteTime); err != nil {
			log.Fatalf("pg_stat_statements scan failed: %v", err)
		}
		// UserName
		if r := c.result.RoleByOID(s.UserOID); r != nil {
			s.UserName = r.Name
		}
		// DBName
		if d := c.result.DatabaseByOID(s.DBOID); d != nil {
			s.DBName = d.Name
		}
		// Query ID, set to 0 if null
		s.QueryID = queryID.Int64
		c.result.Statements = append(c.result.Statements, s)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_statements failed: %v", err)
	}
}

func (c *collector) getWALSegmentSize() (out int) {
	out = 16 * 1024 * 1024 // default to 16MB
	if c.version >= pgv11 {
		if v, err := strconv.Atoi(c.setting("wal_segment_size")); err == nil {
			out = v
		}
	} else {
		v1, err1 := strconv.Atoi(c.setting("wal_segment_size"))
		v2, err2 := strconv.Atoi(c.setting("wal_block_size"))
		if err1 == nil && err2 == nil {
			out = v1 * v2
		}
	}
	return
}

func (c *collector) isAWS() bool {
	return len(c.setting("rds.extensions")) > 0
}

func (c *collector) isAWSAurora() bool {
	s := c.setting("rds.extensions")
	return strings.Contains(s, "aurora_stat_utils") || strings.Contains(s, "apg_plan_mgmt")
}

// getWALCountsv12 gets the WAL file and archive ready counts using the
// following functions (respectively):
//
//	pg_ls_waldir
//	pg_ls_archive_statusdir
func (c *collector) getWALCountsv12() {
	q1 := `SELECT name FROM pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}$'`
	q2 := `SELECT COUNT(*) FROM pg_ls_archive_statusdir() WHERE name ~ '^[0-9A-F]{24}.ready$'`

	c.getWALCountsActual(q1, q2)
}

// getWALCountsv11 gets the WAL file and archive ready counts using the
// following functions (respectively):
//
//	pg_ls_waldir
//	pg_ls_dir (if not aws)
func (c *collector) getWALCountsv11() {
	q1 := `SELECT name FROM pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}$'`
	q2 := `SELECT COUNT(*) FROM pg_ls_dir('pg_wal/archive_status') WHERE pg_ls_dir ~ '^[0-9A-F]{24}.ready$'`

	// no one has perms for pg_ls_dir in AWS RDS, so don't try to get archive
	// status counts
	if c.isAWS() {
		q2 = ""
		c.result.WALReadyCount = -1
	}

	c.getWALCountsActual(q1, q2)
}

// getWALCounts gets the WAL file and archive ready counts using the
// following functions (respectively):
//
//	pg_ls_dir (if not aws)
//	pg_ls_dir (if not aws)
func (c *collector) getWALCounts() {
	// no one has perms for pg_ls_dir in AWS RDS, so don't try
	if c.isAWS() {
		c.result.WALCount = -1
		c.result.WALReadyCount = -1
		return
	}

	q1 := `SELECT pg_ls_dir FROM pg_ls_dir('pg_xlog') WHERE pg_ls_dir ~ '^[0-9A-F]{24}$'`
	q2 := `SELECT COUNT(*) FROM pg_ls_dir('pg_xlog/archive_status') WHERE pg_ls_dir ~ '^[0-9A-F]{24}.ready$'`
	if c.version >= pgv10 {
		q1 = strings.Replace(q1, "pg_xlog", "pg_wal", -1)
		q2 = strings.Replace(q2, "pg_xlog", "pg_wal", -1)
	}

	c.getWALCountsActual(q1, q2)
}

// getWALCountsActual actually executes the given queries to get the WAL file
// and archive ready counts.
func (c *collector) getWALCountsActual(q1, q2 string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// see postgres source include/access/xlog_internal.h
	walSegmentSize := uint64(c.getWALSegmentSize())
	xLogSegmentsPerXLogID := 0x100000000 / walSegmentSize

	// go through all the WAL filenames (ignore errors, need superuser)
	c.result.WALCount = -1
	c.result.HighestWALSegment = 0
	count, highest := 0, uint64(0)
	if rows, err := c.db.QueryContext(ctx, q1); err == nil {
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil || len(name) != 24 {
				count = -1
				break
			}
			count++ // count the number of wal files
			logno, err1 := strconv.ParseUint(name[8:16], 16, 64)
			segno, err2 := strconv.ParseUint(name[16:], 16, 64)
			if err1 != nil || err2 != nil {
				count = -1
				break
			}
			logsegno := logno*xLogSegmentsPerXLogID + segno
			if logsegno > highest { // remember the highest vluae
				highest = logsegno
			}
		}
		if err := rows.Err(); err == nil && count != -1 {
			c.result.WALCount = count
			c.result.HighestWALSegment = highest
		}
		rows.Close()
	}

	// count the number of WAL files that are ready for archiving, if we have
	// been given a query
	c.result.WALReadyCount = -1
	if q2 != "" {
		_ = c.db.QueryRowContext(ctx, q2).Scan(&c.result.WALReadyCount)
		// ignore errors, needs superuser
	}
}

func (c *collector) getNotification() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pg_notification_queue_usage()`
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.NotificationQueueUsage); err != nil {
		log.Fatalf("pg_notification_queue_usage failed: %v", err)
	}
}

func (c *collector) getLocks() {
	c.getLockRows()
	if c.version >= pgv96 {
		c.getBlockers96()
	} else {
		c.getBlockers()
	}
}

func (c *collector) getLockRows() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `
SELECT COALESCE(D.datname, ''), L.locktype, L.mode, L.granted,
       COALESCE(L.pid, 0), COALESCE(L.relation, 0), @waitstart@
  FROM pg_locks L LEFT OUTER JOIN pg_database D ON L.database = D.oid`
	if c.version >= pgv14 { // waitstart only in pg >= 14
		q = strings.Replace(q, `@waitstart@`, `COALESCE(EXTRACT(EPOCH FROM waitstart)::bigint, 0)`, 1)
	} else {
		q = strings.Replace(q, `@waitstart@`, `0`, 1)
	}
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_locks query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var l pgmetrics.Lock
		if err := rows.Scan(&l.DBName, &l.LockType, &l.Mode, &l.Granted,
			&l.PID, &l.RelationOID, &l.WaitStart); err != nil {
			log.Fatalf("pg_locks query failed: %v", err)
		}
		c.result.Locks = append(c.result.Locks, l)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_locks query failed: %v", err)
	}
}

func (c *collector) getBlockers96() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `
WITH P AS (SELECT DISTINCT pid FROM pg_locks WHERE NOT granted)
SELECT pid, pg_blocking_pids(pid) FROM P`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_locks query failed: %v", err)
	}
	defer rows.Close()

	m := pgtype.NewMap()
	c.result.BlockingPIDs = make(map[int][]int)
	for rows.Next() {
		var pid int
		var blockers []int
		if err := rows.Scan(&pid, m.SQLScanner(&blockers)); err != nil {
			log.Fatalf("pg_locks query failed: %v", err)
		}
		c.result.BlockingPIDs[pid] = blockers
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_locks query failed: %v", err)
	}
}

func (c *collector) getBlockers() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Based on a query from https://wiki.postgresql.org/wiki/Lock_Monitoring
	q := `
SELECT DISTINCT blocked_locks.pid AS blocked_pid, blocking_locks.pid AS blocking_pid
 FROM  pg_catalog.pg_locks blocked_locks
  JOIN pg_catalog.pg_locks blocking_locks 
        ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid
 WHERE NOT blocked_locks.GRANTED`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_locks query failed: %v", err)
	}
	defer rows.Close()

	c.result.BlockingPIDs = make(map[int][]int)
	for rows.Next() {
		var pid, blocker int
		if err := rows.Scan(&pid, &blocker); err != nil {
			log.Fatalf("pg_locks query failed: %v", err)
		}
		c.result.BlockingPIDs[pid] = append(c.result.BlockingPIDs[pid], blocker)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_locks query failed: %v", err)
	}
}

func (c *collector) getPublications() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `WITH pc AS (SELECT pubname, COUNT(*) AS c FROM pg_publication_tables GROUP BY 1)
			SELECT p.oid, p.pubname, current_database(), puballtables, pubinsert,
				pubupdate, pubdelete, pc.c
			FROM pg_publication p JOIN pc ON p.pubname = pc.pubname`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return // don't fail on errors
	}
	defer rows.Close()

	for rows.Next() {
		var p pgmetrics.Publication
		if err := rows.Scan(&p.OID, &p.Name, &p.DBName, &p.AllTables, &p.Insert,
			&p.Update, &p.Delete, &p.TableCount); err != nil {
			log.Fatalf("pg_publication/pg_publication_tables query failed: %v", err)
		}
		c.result.Publications = append(c.result.Publications, p)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_publication/pg_publication_tables query failed: %v", err)
	}
}

func (c *collector) getSubscriptions() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	var q string
	if c.version >= pgv15 {
		q = `WITH
				sc AS (SELECT srsubid, COUNT(*) AS c FROM pg_subscription_rel GROUP BY 1),
				swc AS (SELECT subid, COUNT(*) AS c FROM pg_stat_subscription GROUP BY 1),
				sss AS (SELECT subid, apply_error_count, sync_error_count FROM pg_stat_subscription_stats)
			SELECT
				s.oid, s.subname, current_database(), subenabled,
				array_length(subpublications, 1) AS pubcount, sc.c AS tabcount,
				swc.c AS workercount,
				COALESCE(ss.received_lsn::text, ''),
				COALESCE(ss.latest_end_lsn::text, ''),
				ss.last_msg_send_time, ss.last_msg_receipt_time,
				COALESCE(EXTRACT(EPOCH FROM ss.latest_end_time)::bigint, 0),
				sss.apply_error_count,
				sss.sync_error_count
			FROM
				pg_subscription s
				JOIN sc ON s.oid = sc.srsubid
				JOIN pg_stat_subscription ss ON s.oid = ss.subid
				JOIN swc ON s.oid = swc.subid
				JOIN sss ON s.oid = sss.subid
			WHERE
				ss.relid IS NULL`
	} else {
		q = `WITH
				sc AS (SELECT srsubid, COUNT(*) AS c FROM pg_subscription_rel GROUP BY 1),
				swc AS (SELECT subid, COUNT(*) AS c FROM pg_stat_subscription GROUP BY 1)
			SELECT
				s.oid, s.subname, current_database(), subenabled,
				array_length(subpublications, 1) AS pubcount, sc.c AS tabcount,
				swc.c AS workercount,
				COALESCE(ss.received_lsn::text, ''),
				COALESCE(ss.latest_end_lsn::text, ''),
				ss.last_msg_send_time, ss.last_msg_receipt_time,
				COALESCE(EXTRACT(EPOCH FROM ss.latest_end_time)::bigint, 0),
				0, 0
			FROM
				pg_subscription s
				JOIN sc ON s.oid = sc.srsubid
				JOIN pg_stat_subscription ss ON s.oid = ss.subid
				JOIN swc ON s.oid = swc.subid
			WHERE
				ss.relid IS NULL`
	}
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return // don't fail on errors
	}
	defer rows.Close()

	for rows.Next() {
		var s pgmetrics.Subscription
		var msgSend, msgRecv sql.NullTime
		if err := rows.Scan(&s.OID, &s.Name, &s.DBName, &s.Enabled, &s.PubCount,
			&s.TableCount, &s.WorkerCount, &s.ReceivedLSN, &s.LatestEndLSN,
			&msgSend, &msgRecv, &s.LatestEndTime, &s.ApplyErrorCount, &s.SyncErrorCount); err != nil {
			log.Fatalf("pg_subscription query failed: %v", err)
		}
		if msgSend.Valid {
			s.LastMsgSendTime = msgSend.Time.Unix()
		}
		if msgRecv.Valid {
			s.LastMsgReceiptTime = msgRecv.Time.Unix()
		}
		if msgSend.Valid && msgRecv.Valid {
			s.Latency = int64(msgRecv.Time.Sub(msgSend.Time)) / 1000
		}
		c.result.Subscriptions = append(c.result.Subscriptions, s)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_subscription query failed: %v", err)
	}
}

func (c *collector) getPartitionInfo() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT c.oid, inhparent::regclass, COALESCE(pg_get_expr(c.relpartbound, inhrelid), '')
			FROM pg_class c JOIN pg_inherits i ON c.oid = inhrelid
			WHERE c.relispartition`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_class query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var oid int
		var parent, pcv string
		if err := rows.Scan(&oid, &parent, &pcv); err != nil {
			log.Fatalf("pg_class query failed: %v", err)
		}
		if t := c.result.TableByOID(oid); t != nil {
			t.ParentName = parent
			t.PartitionCV = pcv
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_class query failed: %v", err)
	}
}

func (c *collector) getParentInfo() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT c.oid, i.inhparent::regclass
			FROM pg_class c JOIN pg_inherits i ON c.oid=i.inhrelid`
	if c.version >= pgv10 { // exclude partition children in v10+
		q += ` WHERE NOT c.relispartition`
	}
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_class/pg_inherits query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var oid int
		var parent string
		if err := rows.Scan(&oid, &parent); err != nil {
			log.Fatalf("pg_class/pg_inherits query failed: %v", err)
		}
		if t := c.result.TableByOID(oid); t != nil {
			t.ParentName = parent
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_class/pg_inherits query failed: %v", err)
	}
}

func (c *collector) getBloat() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, sqlBloat)
	if err != nil {
		log.Fatalf("bloat query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var dbname, schemaname, tablename, indexname string
		var dummy [13]string // we don't want to edit sqlBloat!
		var wastedbytes, wastedibytes int64
		if err := rows.Scan(&dbname, &schemaname, &tablename, &dummy[0],
			&dummy[1], &dummy[2], &dummy[3], &dummy[4], &wastedbytes, &dummy[5],
			&indexname, &dummy[6], &dummy[7], &dummy[8], &dummy[9], &dummy[10],
			&wastedibytes, &dummy[11], &dummy[12]); err != nil {
			log.Fatalf("bloat query failed: %v", err)
		}
		if t := c.result.TableByName(dbname, schemaname, tablename); t != nil && t.Bloat == -1 {
			t.Bloat = wastedbytes
		}
		if indexname != "?" {
			if i := c.result.IndexByName(dbname, schemaname, indexname); i != nil && i.Bloat == -1 {
				i.Bloat = wastedibytes
			}
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("bloat query failed: %v", err)
	}
}

func (c *collector) getWAL() {
	// skip if Aurora, because the function errors out with:
	// "Function pg_stat_get_wal() is currently not supported for Aurora"
	if c.isAWSAurora() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// pg_stat_wal has only 1 row
	q := `SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write,
			     wal_sync, wal_write_time, wal_sync_time,
			     COALESCE(EXTRACT(EPOCH FROM stats_reset)::bigint, 0)
		  FROM   pg_stat_wal
		  LIMIT  1`

	var w pgmetrics.WAL
	err := c.db.QueryRowContext(ctx, q).Scan(&w.Records, &w.FPI, &w.Bytes,
		&w.BuffersFull, &w.Write, &w.Sync, &w.WriteTime, &w.SyncTime,
		&w.StatsReset)
	if err != nil {
		log.Fatalf("pg_stat_wal query failed: %v", err)
	}
	c.result.WAL = &w
}

func (c *collector) getWALv18() {
	// skip if Aurora, because the function errors out with:
	// "Function pg_stat_get_wal() is currently not supported for Aurora"
	if c.isAWSAurora() {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// In PostgreSQL 18, wal_write, wal_sync, wal_write_time, wal_sync_time
	// columns were removed from pg_stat_wal and moved to pg_stat_io
	// pg_stat_wal has only 1 row
	q := `SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full,
			     COALESCE(EXTRACT(EPOCH FROM stats_reset)::bigint, 0)
		  FROM   pg_stat_wal
		  LIMIT  1`

	var w pgmetrics.WAL
	err := c.db.QueryRowContext(ctx, q).Scan(&w.Records, &w.FPI, &w.Bytes,
		&w.BuffersFull, &w.StatsReset)
	if err != nil {
		log.Fatalf("pg_stat_wal query failed: %v", err)
	}
	c.result.WAL = &w
}

func (c *collector) getProgressAnalyze() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pid, datname, COALESCE(relid::int, 0::int), COALESCE(phase, ''),
				 COALESCE(sample_blks_total, 0::bigint),
				 COALESCE(sample_blks_scanned, 0::bigint),
				 COALESCE(ext_stats_total, 0::bigint),
				 COALESCE(ext_stats_computed, 0::bigint),
				 COALESCE(child_tables_total, 0::bigint),
				 COALESCE(child_tables_done, 0::bigint),
				 COALESCE(current_child_table_relid::int, 0::int),
				 @delay_time@
		    FROM pg_stat_progress_analyze
		ORDER BY pid ASC`
	if c.version < pgv18 { // delay_time only in pg >= 18
		q = strings.Replace(q, "@delay_time@", "0", 1)
	} else {
		q = strings.Replace(q, "@delay_time@", "COALESCE(delay_time, 0)", 1)
	}

	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: pg_stat_progress_analyze query failed: %v", err)
		return
	}
	defer rows.Close()

	var out []pgmetrics.AnalyzeProgressBackend
	for rows.Next() {
		var r pgmetrics.AnalyzeProgressBackend
		if err := rows.Scan(&r.PID, &r.DBName, &r.TableOID, &r.Phase,
			&r.SampleBlocksTotal, &r.SampleBlocksScanned, &r.ExtStatsTotal,
			&r.ExtStatsComputed, &r.ChildTablesTotal, &r.ChildTablesDone,
			&r.CurrentChildTableRelOID, &r.DelayTime); err != nil {
			log.Fatalf("pg_stat_progress_analyze query scan failed: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_progress_analyze query rows failed: %v", err)
	}

	c.result.AnalyzeProgress = out
}

func (c *collector) getProgressBasebackup() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pid, COALESCE(phase, ''),
				 COALESCE(backup_total, 0::bigint),
				 COALESCE(backup_streamed, 0::bigint),
				 COALESCE(tablespaces_total, 0::bigint),
				 COALESCE(tablespaces_streamed, 0::bigint)
		    FROM pg_stat_progress_basebackup
		ORDER BY pid ASC`

	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: pg_stat_progress_basebackup query failed: %v", err)
		return
	}
	defer rows.Close()

	var out []pgmetrics.BasebackupProgressBackend
	for rows.Next() {
		var r pgmetrics.BasebackupProgressBackend
		if err := rows.Scan(&r.PID, &r.Phase, &r.BackupTotal, &r.BackupStreamed,
			&r.TablespacesTotal, &r.TablespacesStreamed); err != nil {
			log.Fatalf("pg_stat_progress_basebackup query scan failed: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_progress_basebackup query rows failed: %v", err)
	}

	c.result.BasebackupProgress = out
}

func (c *collector) getProgressCluster() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pid, datname, relid::int, COALESCE(command, ''),
				 COALESCE(phase, ''),
				 COALESCE(cluster_index_relid::int, 0),
				 COALESCE(heap_tuples_scanned, 0::bigint),
				 COALESCE(heap_tuples_written, 0::bigint),
				 COALESCE(heap_blks_total, 0::bigint),
				 COALESCE(heap_blks_scanned, 0::bigint),
				 COALESCE(index_rebuild_count::int, 0::int)
		    FROM pg_stat_progress_cluster
		ORDER BY pid ASC`

	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: pg_stat_progress_cluster query failed: %v", err)
		return
	}
	defer rows.Close()

	var out []pgmetrics.ClusterProgressBackend
	for rows.Next() {
		var r pgmetrics.ClusterProgressBackend
		if err := rows.Scan(&r.PID, &r.DBName, &r.TableOID, &r.Command, &r.Phase,
			&r.ClusterIndexOID, &r.HeapTuplesScanned, &r.HeapTuplesWritten,
			&r.HeapBlksTotal, &r.HeapBlksScanned, &r.IndexRebuildCount); err != nil {
			log.Fatalf("pg_stat_progress_cluster query scan failed: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_progress_cluster query rows failed: %v", err)
	}

	c.result.ClusterProgress = out
}

func (c *collector) getProgressCopy() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pid, datname, relid::int, COALESCE(command, ''),
				 COALESCE(type, ''),
				 COALESCE(bytes_processed, 0::bigint),
				 COALESCE(bytes_total, 0::bigint),
				 COALESCE(tuples_processed, 0::bigint),
				 COALESCE(tuples_excluded, 0::bigint)
		    FROM pg_stat_progress_copy
		ORDER BY pid ASC`

	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: pg_stat_progress_copy query failed: %v", err)
		return
	}
	defer rows.Close()

	var out []pgmetrics.CopyProgressBackend
	for rows.Next() {
		var r pgmetrics.CopyProgressBackend
		if err := rows.Scan(&r.PID, &r.DBName, &r.TableOID, &r.Command, &r.Type,
			&r.BytesProcessed, &r.BytesTotal, &r.TuplesProcessed,
			&r.TuplesExcluded); err != nil {
			log.Fatalf("pg_stat_progress_copy query scan failed: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_progress_copy query rows failed: %v", err)
	}

	c.result.CopyProgress = out
}

func (c *collector) getProgressCreateIndex() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pid, datname, relid::int, index_relid::int, command, phase,
				 COALESCE(lockers_total, 0::bigint),
				 COALESCE(lockers_done, 0::bigint),
				 COALESCE(current_locker_pid::int, 0::int),
				 COALESCE(blocks_total, 0::bigint),
				 COALESCE(blocks_done, 0::bigint),
				 COALESCE(tuples_total, 0::bigint),
				 COALESCE(tuples_done, 0::bigint),
				 COALESCE(partitions_total, 0::bigint),
				 COALESCE(partitions_done, 0::bigint)
		    FROM pg_stat_progress_create_index
		ORDER BY pid ASC`

	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: pg_stat_progress_create_index query failed: %v", err)
		return
	}
	defer rows.Close()

	var out []pgmetrics.CreateIndexProgressBackend
	for rows.Next() {
		var r pgmetrics.CreateIndexProgressBackend
		if err := rows.Scan(&r.PID, &r.DBName, &r.TableOID, &r.IndexOID,
			&r.Command, &r.Phase, &r.LockersTotal, &r.LockersDone,
			&r.CurrentLockerPID, &r.BlocksTotal, &r.BlocksDone,
			&r.TuplesTotal, &r.TuplesDone, &r.PartitionsTotal,
			&r.PartitionsDone); err != nil {
			log.Fatalf("pg_stat_progress_create_index query scan failed: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_progress_create_index query rows failed: %v", err)
	}

	c.result.CreateIndexProgress = out
}

func (c *collector) getCheckpointer() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT num_timed, num_requested, restartpoints_timed,
				restartpoints_req, restartpoints_done, write_time, sync_time,
				buffers_written, COALESCE(EXTRACT(EPOCH FROM stats_reset)::bigint, 0),
				num_done, slru_written
		    FROM pg_stat_checkpointer`
	if c.version < pgv18 { // num_done and slru_written only in pg >= v18
		q = strings.Replace(q, "num_done", "0", 1)
		q = strings.Replace(q, "slru_written", "0", 1)
	}

	var ckp pgmetrics.Checkpointer
	err := c.db.QueryRowContext(ctx, q).Scan(&ckp.NumTimed, &ckp.NumRequested,
		&ckp.RestartpointsTimed, &ckp.RestartpointsRequested, &ckp.RestartpointsDone,
		&ckp.WriteTime, &ckp.SyncTime, &ckp.BuffersWritten, &ckp.StatsReset,
		&ckp.NumDone, &ckp.SLRUWritten)
	if err != nil {
		log.Fatalf("pg_stat_checkpointer query failed: %v", err)
	}

	c.result.Checkpointer = &ckp
}

//------------------------------------------------------------------------------
// PgBouncer

func (c *collector) collectPgBouncer() {
	c.result.PgBouncer = &pgmetrics.PgBouncer{}
	c.getPBPools()
	c.getPBServers()
	c.getPBClients()
	c.getPBStats()
	c.getPBDatabases()
}

/*
 * PgBouncer "SHOW POOLS" changes across recent PgBouncer versions:
 *
 * 1.15: (12) database, user, cl_active, cl_waiting, sv_active, sv_idle,
 *            sv_used, sv_tested, sv_login, maxwait, maxwait_us, pool_mode
 * 1.16: (13) database, user, cl_active, cl_waiting, cl_cancel_req, sv_active,
 *            sv_idle, sv_used, sv_tested, sv_login, maxwait, maxwait_us, pool_mode
 * 1.17: same as 1.16
 * 1.18: (16) database, user, cl_active, cl_waiting, cl_active_cancel_req,
 *            cl_waiting_cancel_req, sv_active, sv_active_cancel,
 *            sv_being_canceled, sv_idle, sv_used, sv_tested, sv_login, maxwait,
 *            maxwait_us, pool_mode
 * 1.19: same as 1.18
 * 1.20: same as 1.19
 * 1.21: same as 1.20
 * 1.22: same as 1.21
 */

func (c *collector) getPBPools() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW POOLS")
	if err != nil {
		log.Fatalf("pgbouncer: show pools query failed: %v", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	for rows.Next() {
		var pool pgmetrics.PgBouncerPool
		var maxWaitUs float64
		if ncols == 12 {
			err = rows.Scan(&pool.Database, &pool.UserName, &pool.ClActive,
				&pool.ClWaiting, &pool.SvActive, &pool.SvIdle, &pool.SvUsed,
				&pool.SvTested, &pool.SvLogin, &pool.MaxWait, &maxWaitUs,
				&pool.Mode)
		} else if ncols == 13 {
			err = rows.Scan(&pool.Database, &pool.UserName, &pool.ClActive,
				&pool.ClWaiting, &pool.ClCancelReq, &pool.SvActive, &pool.SvIdle,
				&pool.SvUsed, &pool.SvTested, &pool.SvLogin, &pool.MaxWait,
				&maxWaitUs, &pool.Mode)
		} else if ncols == 16 {
			err = rows.Scan(&pool.Database, &pool.UserName, &pool.ClActive,
				&pool.ClWaiting, &pool.ClActiveCancelReq, &pool.ClWaitingCancelReq,
				&pool.SvActive, &pool.SvActiveCancel, &pool.SvBeingCanceled,
				&pool.SvIdle, &pool.SvUsed, &pool.SvTested, &pool.SvLogin,
				&pool.MaxWait, &maxWaitUs, &pool.Mode)
		} else {
			log.Fatalf("pgbouncer: unsupported number of columns %d in 'SHOW POOLS'", ncols)
		}
		if err != nil {
			log.Fatalf("pgbouncer: show pools query failed: %v", err)
		}
		pool.MaxWait += maxWaitUs / 1e6
		c.result.PgBouncer.Pools = append(c.result.PgBouncer.Pools, pool)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pgbouncer: show pools query failed: %v", err)
	}
}

/*
 * PgBouncer "SHOW SERVERS" changes across recent PgBouncer versions:
 *
 * 1.15: (17) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls
 * 1.16: same as 1.15
 * 1.17: same as 1.16
 * 1.18: (18) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls, application_name
 * 1.19: same as 1.18
 * 1.20: same as 1.19
 * 1.21: (19) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls, application_name, prepared_statements
 * 1.22: same as 1.21
 * 1.23: (20) type, user, database, replication, state, addr, port, local_addr,
 *            local_port, connect_time, request_time, wait, wait_us,
 *            close_needed, ptr, link, remote_pid, tls, application_name,
 *            prepared_statements
 */

func (c *collector) getPBServers() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW SERVERS")
	if err != nil {
		log.Fatalf("pgbouncer: show servers query failed: %v", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	for rows.Next() {
		var s [17]sql.NullString
		var state string
		var wait, waitUs float64
		if ncols == 16 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12])
		} else if ncols == 17 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13])
		} else if ncols == 18 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13], &s[14])
		} else if ncols == 19 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13], &s[14], &s[15])
		} else if ncols == 20 {
			err = rows.Scan(&s[0], &s[1], &s[2], &s[3], &state, &s[4], &s[5], &s[6],
				&s[7], &s[8], &s[9], &wait, &waitUs, &s[10], &s[11], &s[12],
				&s[13], &s[14], &s[15], &s[16])
		} else {
			log.Fatalf("pgbouncer: unsupported number of columns %d in 'SHOW SERVERS'", ncols)
		}
		if err != nil {
			log.Fatalf("pgbouncer: show servers query failed: %v", err)
		}
		wait += waitUs / 1e6 // convert usec -> sec
		if wait > c.result.PgBouncer.SCMaxWait {
			c.result.PgBouncer.SCMaxWait = wait
		}
		switch state {
		case "active":
			c.result.PgBouncer.SCActive++
		case "idle":
			c.result.PgBouncer.SCIdle++
		case "used":
			c.result.PgBouncer.SCUsed++
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pgbouncer: show servers query failed: %v", err)
	}
}

/*
 * PgBouncer "SHOW CLIENTS" changes across recent PgBouncer versions:
 *
 * 1.15: (17) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls
 * 1.16: same as 1.15
 * 1.17: same as 1.16
 * 1.18: (18) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls, application_name
 * 1.19: same as 1.18
 * 1.20: same as 1.19
 * 1.21: (19) type, user, database, state, addr, port, local_addr, local_port,
 *            connect_time, request_time, wait, wait_us, close_needed, ptr,
 *            link, remote_pid, tls, application_name, prepared_statements
 * 1.22: same as 1.21
 * 1.23: (20) type, user, database, replication, state, addr, port, local_addr,
 *            local_port, connect_time, request_time, wait, wait_us,
 *            close_needed, ptr, link, remote_pid, tls, application_name,
 *            prepared_statements
 */

func (c *collector) getPBClients() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW CLIENTS")
	if err != nil {
		log.Fatalf("pgbouncer: show clients query failed: %v", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	var totalWait float64
	for rows.Next() {
		var s [17]sql.NullString
		var state string
		var wait, waitUs float64
		if ncols == 16 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12])
		} else if ncols == 17 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13])
		} else if ncols == 18 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13], &s[14])
		} else if ncols == 19 {
			err = rows.Scan(&s[0], &s[1], &s[2], &state, &s[3], &s[4], &s[5],
				&s[6], &s[7], &s[8], &wait, &waitUs, &s[9], &s[10], &s[11], &s[12],
				&s[13], &s[14], &s[15])
		} else if ncols == 20 {
			err = rows.Scan(&s[0], &s[1], &s[2], &s[3], &state, &s[4], &s[5], &s[6],
				&s[7], &s[8], &s[9], &wait, &waitUs, &s[10], &s[11], &s[12], &s[13],
				&s[14], &s[15], &s[16])
		} else {
			log.Fatalf("pgbouncer: unsupported number of columns %d in 'SHOW CLIENTS'", ncols)
		}
		if err != nil {
			log.Fatalf("pgbouncer: show clients query failed: %v", err)
		}
		wait += waitUs / 1e6 // convert usec -> sec
		switch state {
		case "active":
			c.result.PgBouncer.CCActive++
		case "waiting":
			c.result.PgBouncer.CCWaiting++
			if wait > c.result.PgBouncer.CCMaxWait {
				c.result.PgBouncer.CCMaxWait = wait
			}
			totalWait += wait
		case "idle":
			c.result.PgBouncer.CCIdle++
		case "used":
			c.result.PgBouncer.CCUsed++
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pgbouncer: show clients query failed: %v", err)
	}
	if c.result.PgBouncer.CCWaiting > 0 {
		c.result.PgBouncer.CCAvgWait = totalWait / float64(c.result.PgBouncer.CCWaiting)
	}
}

/*
 * PgBouncer "SHOW STATS" changes across recent PgBouncer versions:
 *
 * 1.15: (15) database, total_xact_count, total_query_count, total_received,
 *            total_sent, total_xact_time, total_query_time, total_wait_time,
 *            avg_xact_count, avg_query_count, avg_recv, avg_sent,
 *            avg_xact_time, avg_query_time, avg_wait_time
 * 1.16: same as 1.15
 * 1.17: same as 1.16
 * 1.18: same as 1.17
 * 1.19: same as 1.18
 * 1.20: same as 1.19
 * 1.21: same as 1.20
 * 1.22: same as 1.21
 * 1.23: (17) database, total_server_assignment_count, total_xact_count,
 *            total_query_count, total_received, total_sent, total_xact_time,
 *            total_query_time, total_wait_time, avg_server_assignment_count,
 *            avg_xact_count, avg_query_count, avg_recv, avg_sent,
 *            avg_xact_time, avg_query_time, avg_wait_time
 */

func (c *collector) getPBStats() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW STATS")
	if err != nil {
		log.Fatalf("pgbouncer: show stats query failed: %v", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	for rows.Next() {
		var stat pgmetrics.PgBouncerStat
		var err error
		if ncols == 15 {
			err = rows.Scan(&stat.Database, &stat.TotalXactCount, &stat.TotalQueryCount,
				&stat.TotalReceived, &stat.TotalSent, &stat.TotalXactTime,
				&stat.TotalQueryTime, &stat.TotalWaitTime, &stat.AvgXactCount,
				&stat.AvgQueryCount, &stat.AvgReceived, &stat.AvgSent, &stat.AvgXactTime,
				&stat.AvgQueryTime, &stat.AvgWaitTime)
		} else if ncols == 17 {
			err = rows.Scan(&stat.Database, &stat.TotalServerAssignmentCount,
				&stat.TotalXactCount, &stat.TotalQueryCount,
				&stat.TotalReceived, &stat.TotalSent, &stat.TotalXactTime,
				&stat.TotalQueryTime, &stat.TotalWaitTime, &stat.AvgXactCount,
				&stat.AvgQueryCount, &stat.AvgReceived, &stat.AvgSent, &stat.AvgXactTime,
				&stat.AvgQueryTime, &stat.AvgWaitTime, &stat.AvgServerAssignmentCount)
		} else {
			log.Fatalf("pgbouncer: unsupported number of columns %d in 'SHOW STATS'", ncols)
		}
		if err != nil {
			log.Fatalf("pgbouncer: show stats query failed: %v", err)
		}
		// convert usec -> sec
		stat.TotalXactTime /= 1e6
		stat.TotalQueryTime /= 1e6
		stat.TotalWaitTime /= 1e6
		stat.AvgXactTime /= 1e6
		stat.AvgQueryTime /= 1e6
		stat.AvgWaitTime /= 1e6
		c.result.PgBouncer.Stats = append(c.result.PgBouncer.Stats, stat)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pgbouncer: show stats query failed: %v", err)
	}
}

/*
 * PgBouncer "SHOW DATABASES" changes across recent PgBouncer versions:
 *
 * 1.15: (12) name, host, port, database, force_user, pool_size, reserve_pool,
 *            pool_mode, max_connections, current_connections, paused, disabled
 * 1.16: (13) name, host, port, database, force_user, pool_size, min_pool_size,
 *            reserve_pool, pool_mode, max_connections, current_connections,
 *            paused, disabled
 * 1.17: same as 1.16
 * 1.18: same as 1.17
 * 1.19: same as 1.18
 * 1.20: same as 1.19
 * 1.21: same as 1.20
 * 1.22: same as 1.21
 * 1.23: (14) name, host, port, database, force_user, pool_size, min_pool_size,
 *            reserve_pool, server_lifetime, pool_mode, max_connections,
 *            current_connections, paused, disabled
 */

func (c *collector) getPBDatabases() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		log.Fatalf("pgbouncer: show databases query failed: %v", err)
	}
	defer rows.Close()

	var ncols int
	if cols, err := rows.Columns(); err == nil {
		ncols = len(cols)
	}

	for rows.Next() {
		var db pgmetrics.PgBouncerDatabase
		var host, user sql.NullString
		var s [5]sql.NullString
		var paused, disabled int
		if ncols == 12 {
			err = rows.Scan(&db.Database, &host, &db.Port, &db.SourceDatabase,
				&user, &s[0], &s[1], &s[2], &db.MaxConn, &db.CurrConn, &paused,
				&disabled)
		} else if ncols == 13 {
			err = rows.Scan(&db.Database, &host, &db.Port, &db.SourceDatabase,
				&user, &s[0], &s[1], &s[2], &s[3], &db.MaxConn, &db.CurrConn,
				&paused, &disabled)
		} else if ncols == 14 {
			err = rows.Scan(&db.Database, &host, &db.Port, &db.SourceDatabase,
				&user, &s[0], &s[1], &s[2], &s[3], &s[4], &db.MaxConn, &db.CurrConn,
				&paused, &disabled)
		} else {
			log.Fatalf("pgbouncer: unsupported number of columns %d in 'SHOW DATABASES'", ncols)
		}
		if err != nil {
			log.Fatalf("pgbouncer: show databases query failed: %v", err)
		}
		db.Host = host.String
		db.Paused = paused == 1
		db.Disabled = disabled == 1
		db.User = user.String
		c.result.PgBouncer.Databases = append(c.result.PgBouncer.Databases, db)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pgbouncer: show databases query failed: %v", err)
	}
}

//------------------------------------------------------------------------------
// log file collection

func (c *collector) getLogInfo() {
	// csv if log_destination has 'csvlog' and logging_collector is 'on'
	c.csvlog = strings.Contains(c.setting("log_destination"), "csvlog") &&
		c.setting("logging_collector") == "on"

	// pg_current_logfile is only available in v10 and above
	if c.version < pgv10 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// format is 'csvlog' or 'stderr'
	var f = "stderr"
	if c.csvlog {
		f = "csvlog"
	}

	q := `SELECT COALESCE(pg_current_logfile($1),'')`
	_ = c.db.QueryRowContext(ctx, q, f).Scan(&c.curlogfile)
	// ignore any errors.
}

func fileExists(f string) bool {
	if fi, err := os.Stat(f); err == nil && fi != nil && fi.Mode().IsRegular() {
		return true
	} else if os.IsPermission(err) {
		log.Printf("access denied trying to open log file %s", f)
	}
	return false
}

func getRecentFile(d string) (f string) {
	files, err := os.ReadDir(d)
	if err != nil {
		return
	}
	var max time.Time
	var fname string
	for _, entry := range files {
		finfo, err := entry.Info()
		if err != nil {
			continue
		}
		if t := finfo.ModTime(); t.After(max) {
			max = t
			fname = finfo.Name()
		}
	}
	if len(fname) > 0 {
		f = filepath.Join(d, fname)
	}
	return
}

func (c *collector) collectLogs(o CollectConfig) {
	// need log_file_prefix first
	if !c.getPrefix() {
		return // already logged
	}

	// try to guess the log file(s) location:
	var logfiles []string

	// 1. use the user-supplied filename (--log-file)
	if len(o.LogFile) > 0 {
		if !fileExists(o.LogFile) {
			log.Printf("warning: failed to locate/read specified log file %s", o.LogFile)
			return
		}
		logfiles = []string{o.LogFile}
	}

	// 2. use the files from the user-supplied dirname (--log-dir)
	if len(logfiles) == 0 && len(o.LogDir) > 0 {
		files, err := os.ReadDir(o.LogDir)
		if err != nil {
			log.Printf("warning: failed to read specified log dir: %v", err)
			return
		}
		for _, f := range files {
			if n := f.Name(); !f.IsDir() && !strings.HasSuffix(n, ".gz") && !strings.HasSuffix(n, ".bz2") {
				// if file does not end in .gz or .bz2, we'll try to read it
				logfiles = append(logfiles, filepath.Join(o.LogDir, n))
			}
		}
	}

	// 3. if pg_current_logfile is available, try that
	if len(logfiles) == 0 && len(c.curlogfile) > 0 {
		if fileExists(c.curlogfile) {
			// c.curlogfile can be an absolute path..
			logfiles = []string{c.curlogfile}
		} else if f := filepath.Join(c.dataDir, c.curlogfile); fileExists(f) {
			// ..or relative to $PGDATA
			logfiles = []string{f}
		}
	}

	// 4. use the recent most file from /var/log/postgresql
	if len(logfiles) == 0 {
		if f := getRecentFile("/var/log/postgresql"); len(f) > 0 {
			logfiles = []string{f}
		}
	}

	// no log file found, warn the user
	if len(logfiles) == 0 {
		log.Print("warning: failed to guess log file location/access denied, specify explicitly with --log-file or --log-dir")
		return
	}

	//log.Printf("debug: found log files %v, using span %d", logfiles, c.logSpan)
	c.readLogs(logfiles)
}

func (c *collector) getPrefix() bool {
	var prefix string
	if s, ok := c.result.Settings["log_line_prefix"]; ok {
		prefix = s.Setting
	} else {
		log.Print("failed to get log_line_prefix setting, cannot read log file")
		return false
	}

	rxPrefix, err := compilePrefix(prefix)
	if err != nil {
		log.Print(err)
		return false
	}

	c.rxPrefix = rxPrefix
	return true
}

func (c *collector) collectFromRDS(o CollectConfig) {
	dbid := o.RDSDBIdentifier
	var err error
	defer func() {
		if err != nil {
			log.Printf("warning: failed to collect from AWS RDS: %v", err)
		}
	}()

	ac, err := newAwsCollector()
	if err != nil {
		return
	}

	rds := &pgmetrics.RDS{}
	if err = ac.collect(dbid, rds); err != nil {
		return
	}
	c.result.RDS = rds

	if !arrayHas(o.Omit, "log") {
		if !c.getPrefix() {
			return // already logged
		}
		window := time.Duration(c.logSpan) * time.Minute
		start := time.Now().Add(-window)

		err = ac.collectLogs(dbid, start, func(lines []byte) {
			if err := c.processLogBuf(start, lines); err != nil {
				log.Printf("warning: %v", err)
			}
		})
	}
}

func (c *collector) collectFromAzure(o CollectConfig) {
	timeout := time.Duration(o.TimeoutSec) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var azure pgmetrics.Azure
	if err := collectAzure(ctx, o.AzureResourceID, &azure); err != nil {
		log.Printf("warning: failed to collect from Azure: %v", err)
	} else {
		c.result.Azure = &azure
	}
}

//------------------------------------------------------------------------------
// The following query for bloat was taken from the venerable check_postgres
// script (https://bucardo.org/check_postgres/), which is:
//
// Copyright (c) 2007-2017 Greg Sabino Mullane
//------------------------------------------------------------------------------

const sqlBloat = `
SELECT
  current_database() AS db, schemaname, tablename, reltuples::bigint AS tups, relpages::bigint AS pages, otta,
  ROUND(CASE WHEN otta=0 OR sml.relpages=0 OR sml.relpages=otta THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
  CASE WHEN relpages < otta THEN 0 ELSE relpages::bigint - otta END AS wastedpages,
  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::bigint END AS wastedbytes,
  CASE WHEN relpages < otta THEN '0 bytes'::text ELSE (bs*(relpages-otta))::bigint::text || ' bytes' END AS wastedsize,
  iname, ituples::bigint AS itups, ipages::bigint AS ipages, iotta,
  ROUND(CASE WHEN iotta=0 OR ipages=0 OR ipages=iotta THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
  CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
  CASE WHEN ipages < iotta THEN 0 ELSE (bs*(ipages-iotta))::bigint END AS wastedibytes,
  CASE WHEN ipages < iotta THEN '0 bytes' ELSE (bs*(ipages-iotta))::bigint::text || ' bytes' END AS wastedisize,
  CASE WHEN relpages < otta THEN
    CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta::bigint) END
    ELSE CASE WHEN ipages < iotta THEN bs*(relpages-otta::bigint)
      ELSE bs*(relpages-otta::bigint + ipages-iotta::bigint) END
  END AS totalwastedbytes
FROM (
  SELECT
    nn.nspname AS schemaname,
    cc.relname AS tablename,
    COALESCE(cc.reltuples,0) AS reltuples,
    COALESCE(cc.relpages,0) AS relpages,
    COALESCE(bs,0) AS bs,
    COALESCE(CEIL((cc.reltuples*((datahdr+ma-
      (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)),0) AS otta,
    COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
  FROM
     pg_class cc
  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname <> 'information_schema'
  LEFT JOIN
  (
    SELECT
      ma,bs,foo.nspname,foo.relname,
      (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
      (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM (
      SELECT
        ns.nspname, tbl.relname, hdr, ma, bs,
        SUM((1-coalesce(null_frac,0))*coalesce(avg_width, 2048)) AS datawidth,
        MAX(coalesce(null_frac,0)) AS maxfracsum,
        hdr+(
          SELECT 1+count(*)/8
          FROM pg_stats s2
          WHERE null_frac<>0 AND s2.schemaname = ns.nspname AND s2.tablename = tbl.relname
        ) AS nullhdr
      FROM pg_attribute att
      JOIN pg_class tbl ON att.attrelid = tbl.oid
      JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
      LEFT JOIN pg_stats s ON s.schemaname=ns.nspname
      AND s.tablename = tbl.relname
      AND s.inherited=false
      AND s.attname=att.attname,
      (
        SELECT
          (SELECT current_setting('block_size')::numeric) AS bs,
            CASE WHEN SUBSTRING(SPLIT_PART(v, ' ', 2) FROM '#"[0-9]+.[0-9]+#"%' for '#')
              IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
          CASE WHEN v ~ 'mingw32' OR v ~ '64-bit' THEN 8 ELSE 4 END AS ma
        FROM (SELECT version() AS v) AS foo
      ) AS constants
      WHERE att.attnum > 0 AND tbl.relkind='r'
      GROUP BY 1,2,3,4,5
    ) AS foo
  ) AS rs
  ON cc.relname = rs.relname AND nn.nspname = rs.nspname
  LEFT JOIN pg_index i ON indrelid = cc.oid
  LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
) AS sml
 WHERE sml.relpages - otta > 10 OR ipages - iotta > 15
`
