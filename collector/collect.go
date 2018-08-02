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

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"os/user"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rapidloop/pgmetrics"
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

// CollectConfig is a bunch of options passed to the Collect() function to
// specify which metrics to collect and how.
type CollectConfig struct {
	// general
	TimeoutSec uint
	NoSizes    bool

	// collection
	Schema     string
	ExclSchema string
	Table      string
	ExclTable  string
	SQLLength  uint
	StmtsLimit uint
	Omit       []string

	// connection
	Host     string
	Port     uint16
	User     string
	Password string
}

// DefaultCollectConfig returns a CollectConfig initialized with default values.
// Some environment variables are consulted.
func DefaultCollectConfig() CollectConfig {
	cc := CollectConfig{
		// ------------------ general
		TimeoutSec: 5,
		//NoSizes: false,

		// ------------------ collection
		//Schema: "",
		//ExclSchema: "",
		//Table: "",
		//ExclTable: "",
		//Omit: nil,
		SQLLength:  500,
		StmtsLimit: 100,

		// ------------------ connection
		//Password: "",
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
	if os.Getenv("PGSSLMODE") == "" {
		connstr += makeKV("sslmode", "disable")
	}
	connstr += makeKV("application_name", "pgmetrics")
	connstr += makeKV("lock_timeout", "50") // 50 msec. Just fail fast on locks.
	connstr += makeKV("statement_timeout", strconv.Itoa(int(o.TimeoutSec)*1000))

	// collect from 1 or more DBs
	c := &collector{}
	if len(dbnames) == 0 {
		collectFromDB(connstr, c, o)
	} else {
		for _, dbname := range dbnames {
			collectFromDB(connstr+makeKV("dbname", dbname), c, o)
		}
	}

	return &c.result
}

func collectFromDB(connstr string, c *collector, o CollectConfig) {
	// connect
	db, err := sql.Open("postgres", connstr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// ping
	t := time.Duration(o.TimeoutSec) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}

	// collect
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	c.collect(db, o)
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

	// save sql length and statement limits
	c.sqlLength = o.SQLLength
	c.stmtsLimit = o.StmtsLimit

	// current time is the report start time
	c.result.Metadata.At = time.Now().Unix()
	c.result.Metadata.Version = pgmetrics.ModelSchemaVersion

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
	c.getStartTime()

	if c.version >= 90600 {
		c.getControlSystemv96()
	}

	if c.version >= 90500 {
		c.getLastXactv95()
	}

	if c.version >= 110000 {
		c.getControlCheckpointv11()
	} else if c.version >= 100000 {
		c.getControlCheckpointv10()
	} else if c.version >= 90600 {
		c.getControlCheckpointv96()
	}

	if c.version >= 90600 {
		c.getActivityv96()
	} else if c.version >= 90400 {
		c.getActivityv94()
	} else {
		c.getActivityv93()
	}

	if c.version >= 90400 {
		c.getWALArchiver()
	}

	c.getBGWriter()

	if c.version >= 100000 {
		c.getReplicationv10()
	} else {
		c.getReplicationv9()
	}

	if c.version >= 90600 {
		c.getWalReceiverv96()
	}

	if c.version >= 100000 {
		c.getAdminFuncv10()
	} else {
		c.getAdminFuncv9()
	}

	if c.version >= 90600 {
		c.getVacuumProgress()
	}

	c.getDatabases(!o.NoSizes)
	c.getTablespaces(!o.NoSizes)

	if c.version >= 90400 {
		c.getReplicationSlotsv94()
	}

	c.getRoles()
	c.getWALCounts()

	if c.version >= 90600 {
		c.getNotification()
	}
}

// info and stats for the current database
func (c *collector) collectDatabase(o CollectConfig) {
	c.getCurrentDatabase()
	if !arrayHas(o.Omit, "tables") {
		c.getTables(!o.NoSizes)
	}
	if !arrayHas(o.Omit, "tables") && !arrayHas(o.Omit, "indexes") {
		c.getIndexes(!o.NoSizes)
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
		c.getStatements()
	}
	c.getBloat()
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
// --schema/--exclude-schema constraints.
func (c *collector) schemaOK(name string) bool {
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
			COALESCE(sourcefile,''), COALESCE(sourceline,0)
		  FROM pg_settings
		  ORDER BY name ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_settings query failed: %v", err)
	}
	defer rows.Close()

	c.result.Settings = make(map[string]pgmetrics.Setting)
	for rows.Next() {
		var s pgmetrics.Setting
		var name, sf, sl string
		if err := rows.Scan(&name, &s.Setting, &s.BootVal, &s.Source, &sf, &sl); err != nil {
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
			EXTRACT(EPOCH FROM stats_reset)::bigint
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

	q := `SELECT COALESCE(inet_client_addr() = inet_server_addr(), TRUE)`
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.local); err != nil {
		c.local = false // don't fail on errors
	}
	c.result.Metadata.Local = c.local
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

	q := `SELECT usename, application_name,
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
			COALESCE(sync_state, '')
		  FROM pg_stat_replication
		  ORDER BY pid ASC`
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
			&r.SyncPriority, &r.SyncState); err != nil {
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

	q := `SELECT usename, application_name,
			COALESCE(client_hostname::text, client_addr::text, ''), 
			COALESCE(EXTRACT(EPOCH FROM backend_start)::bigint, 0),
			backend_xmin, COALESCE(state, ''),
			COALESCE(sent_location::text, ''),
			COALESCE(write_location::text, ''),
			COALESCE(flush_location::text, ''),
			COALESCE(replay_location::text, ''),
			COALESCE(sync_priority, -1),
			COALESCE(sync_state, '')
		  FROM pg_stat_replication
		  ORDER BY pid ASC`
	if c.version < 90400 { // backend_xmin is only in v9.4+
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
			&r.FlushLSN, &r.ReplayLSN, &r.SyncPriority, &r.SyncState); err != nil {
			log.Fatalf("pg_stat_replication query failed: %v", err)
		}
		r.BackendXmin = int(backendXmin.Int64)
		c.result.ReplicationOutgoing = append(c.result.ReplicationOutgoing, r)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_replication query failed: %v", err)
	}
}

func (c *collector) getWalReceiverv96() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT status, receive_start_lsn, receive_start_tli, received_lsn, 
			received_tli, last_msg_send_time, last_msg_receipt_time,
			latest_end_lsn,
			COALESCE(EXTRACT(EPOCH FROM latest_end_time)::bigint, 0),
			COALESCE(slot_name, ''), conninfo
		  FROM pg_stat_wal_receiver`
	var r pgmetrics.ReplicationIn
	var msgSend, msgRecv pq.NullTime
	if err := c.db.QueryRowContext(ctx, q).Scan(&r.Status, &r.ReceiveStartLSN, &r.ReceiveStartTLI,
		&r.ReceivedLSN, &r.ReceivedTLI, &msgSend, &msgRecv,
		&r.LatestEndLSN, &r.LatestEndTime, &r.SlotName, &r.Conninfo); err != nil {
		if err == sql.ErrNoRows {
			return // not an error
		}
		log.Printf("warning: pg_stat_wal_receiver query failed: %v", err)
		return
	}

	if msgSend.Valid && msgRecv.Valid {
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
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.IsInRecovery,
		&c.result.LastWALReceiveLSN, &c.result.LastWALReplayLSN,
		&c.result.LastXActReplayTimestamp); err != nil {
		log.Printf("warning: admin functions query failed: %v", err)
		return
	}

	if c.result.IsInRecovery {
		qr := `SELECT pg_is_xlog_replay_paused()`
		if err := c.db.QueryRowContext(ctx, qr).Scan(&c.result.IsWalReplayPaused); err != nil {
			log.Fatalf("pg_is_xlog_replay_paused() failed: %v", err)
		}
	} else {
		if c.version >= 90600 {
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
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.IsInRecovery,
		&c.result.LastWALReceiveLSN, &c.result.LastWALReplayLSN,
		&c.result.LastXActReplayTimestamp); err != nil {
		log.Printf("warning: admin functions query failed: %v", err)
		return
	}

	if c.result.IsInRecovery {
		qr := `SELECT pg_is_wal_replay_paused()`
		if err := c.db.QueryRowContext(ctx, qr).Scan(&c.result.IsWalReplayPaused); err != nil {
			log.Fatalf("pg_is_wal_replay_paused() failed: %v", err)
		}
	} else {
		qx := `SELECT pg_current_wal_flush_lsn(),
				pg_current_wal_insert_lsn(), pg_current_wal_lsn()`
		if err := c.db.QueryRowContext(ctx, qx).Scan(&c.result.WALFlushLSN,
			&c.result.WALInsertLSN, &c.result.WALLSN); err != nil {
			log.Fatalf("error querying wal location functions: %v", err)
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

func (c *collector) fillTableSize(t *pgmetrics.Table) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pg_table_size($1)`
	if err := c.db.QueryRowContext(ctx, q, t.OID).Scan(&t.Size); err != nil {
		t.Size = -1
	}
}

func (c *collector) fillIndexSize(idx *pgmetrics.Index) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT pg_total_relation_size($1)`
	if err := c.db.QueryRowContext(ctx, q, idx.OID).Scan(&idx.Size); err != nil {
		idx.Size = -1
	}
}

func (c *collector) getLastXactv95() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT xid, COALESCE(EXTRACT(EPOCH FROM timestamp)::bigint, 0)
			FROM pg_last_committed_xact()`
	c.db.QueryRowContext(ctx, q).Scan(&c.result.LastXactXid, &c.result.LastXactTimestamp)
	// ignore errors, works only if "track_commit_timestamp" is "on"
}

func (c *collector) getStartTime() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT EXTRACT(EPOCH FROM pg_postmaster_start_time())::bigint`
	if err := c.db.QueryRowContext(ctx, q).Scan(&c.result.StartTime); err != nil {
		log.Fatalf("pg_postmaster_start_time() failed: %v", err)
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
			COALESCE(state, ''), COALESCE(backend_xid, ''),
			COALESCE(backend_xmin, ''), LEFT(COALESCE(query, ''), $1)
		  FROM pg_stat_activity`
	if c.version >= 100000 {
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
		if err := rows.Scan(&b.DBName, &b.RoleName, &b.ApplicationName,
			&b.PID, &b.ClientAddr, &b.BackendStart, &b.XactStart, &b.QueryStart,
			&b.StateChange, &b.WaitEventType, &b.WaitEvent, &b.State,
			&b.BackendXid, &b.BackendXmin, &b.Query); err != nil {
			log.Fatalf("pg_stat_activity query failed: %v", err)
		}
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

func (c *collector) getDatabases(fillSize bool) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT D.oid, D.datname, D.datdba, D.dattablespace, D.datconnlimit,
			age(D.datfrozenxid), S.numbackends, S.xact_commit, S.xact_rollback,
			S.blks_read, S.blks_hit, S.tup_returned, S.tup_fetched,
			S.tup_inserted, S.tup_updated, S.tup_deleted, S.conflicts,
			S.temp_files, S.temp_bytes, S.deadlocks, S.blk_read_time,
			S.blk_write_time,
			COALESCE(EXTRACT(EPOCH FROM S.stats_reset)::bigint, 0)
		  FROM pg_database AS D JOIN pg_stat_database AS S
			ON D.oid = S.datid
		  WHERE NOT D.datistemplate
		  ORDER BY D.oid ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_stat_database query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var d pgmetrics.Database
		if err := rows.Scan(&d.OID, &d.Name, &d.DatDBA, &d.DatTablespace,
			&d.DatConnLimit, &d.AgeDatFrozenXid, &d.NumBackends, &d.XactCommit,
			&d.XactRollback, &d.BlksRead, &d.BlksHit, &d.TupReturned,
			&d.TupFetched, &d.TupInserted, &d.TupUpdated, &d.TupDeleted,
			&d.Conflicts, &d.TempFiles, &d.TempBytes, &d.Deadlocks,
			&d.BlkReadTime, &d.BlkWriteTime, &d.StatsReset); err != nil {
			log.Fatalf("pg_stat_database query failed: %v", err)
		}
		d.Size = -1 // will be filled in later if asked for
		c.result.Databases = append(c.result.Databases, d)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_database query failed: %v", err)
	}

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

func (c *collector) getCurrentDatabase() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT current_database()`
	var dbname string
	if err := c.db.QueryRowContext(ctx, q).Scan(&dbname); err != nil {
		log.Fatalf("current_database failed: %v", err)
	}
	c.result.Metadata.CollectedDBs = append(c.result.Metadata.CollectedDBs, dbname)
}

func (c *collector) getTables(fillSize bool) {
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
			COALESCE(IO.tidx_blks_read, 0), COALESCE(IO.tidx_blks_hit, 0)
		  FROM pg_stat_user_tables AS S
			JOIN pg_statio_user_tables AS IO
			ON S.relid = IO.relid
		  ORDER BY S.relid ASC`
	if c.version < 90400 { // n_mod_since_analyze only in v9.4+
		q = strings.Replace(q, "S.n_mod_since_analyze", "0", 1)
	}
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_stat(io)_user_tables query failed: %v", err)
	}
	defer rows.Close()

	startIdx := len(c.result.Tables)
	for rows.Next() {
		var t pgmetrics.Table
		if err := rows.Scan(&t.OID, &t.SchemaName, &t.Name, &t.DBName,
			&t.SeqScan, &t.SeqTupRead, &t.IdxScan, &t.IdxTupFetch, &t.NTupIns,
			&t.NTupUpd, &t.NTupDel, &t.NTupHotUpd, &t.NLiveTup, &t.NDeadTup,
			&t.NModSinceAnalyze, &t.LastVacuum, &t.LastAutovacuum,
			&t.LastAnalyze, &t.LastAutoanalyze, &t.VacuumCount,
			&t.AutovacuumCount, &t.AnalyzeCount, &t.AutoanalyzeCount,
			&t.HeapBlksRead, &t.HeapBlksHit, &t.IdxBlksRead, &t.IdxBlksHit,
			&t.ToastBlksRead, &t.ToastBlksHit, &t.TidxBlksRead, &t.TidxBlksHit); err != nil {
			log.Fatalf("pg_stat(io)_user_tables query failed: %v", err)
		}
		t.Size = -1  // will be filled in later if asked for
		t.Bloat = -1 // will be filled in later
		if c.tableOK(t.SchemaName, t.Name) {
			c.result.Tables = append(c.result.Tables, t)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat(io)_user_tables query failed: %v", err)
	}

	if !fillSize {
		return
	}
	for i := startIdx; i < len(c.result.Tables); i++ {
		c.fillTableSize(&c.result.Tables[i])
	}
}

func (c *collector) getIndexes(fillSize bool) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT S.relid, S.indexrelid, S.schemaname, S.relname, S.indexrelname,
			current_database(), S.idx_scan, S.idx_tup_read, S.idx_tup_fetch,
			pg_stat_get_blocks_fetched(S.indexrelid) - pg_stat_get_blocks_hit(S.indexrelid) AS idx_blks_read,
			pg_stat_get_blocks_hit(S.indexrelid) AS idx_blks_hit
		  FROM pg_stat_user_indexes AS S
		  ORDER BY s.relid ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_stat_user_indexes query failed: %v", err)
	}
	defer rows.Close()

	startIdx := len(c.result.Indexes)
	for rows.Next() {
		var idx pgmetrics.Index
		if err := rows.Scan(&idx.TableOID, &idx.OID, &idx.SchemaName,
			&idx.TableName, &idx.Name, &idx.DBName, &idx.IdxScan,
			&idx.IdxTupRead, &idx.IdxTupFetch, &idx.IdxBlksRead,
			&idx.IdxBlksHit); err != nil {
			log.Fatalf("pg_stat_user_indexes query failed: %v", err)
		}
		idx.Size = -1  // will be filled in later if asked for
		idx.Bloat = -1 // will be filled in later
		if c.tableOK(idx.SchemaName, idx.TableName) {
			c.result.Indexes = append(c.result.Indexes, idx)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("pg_stat_user_indexes query failed: %v", err)
	}

	if !fillSize {
		return
	}
	for i := startIdx; i < len(c.result.Indexes); i++ {
		c.fillIndexSize(&c.result.Indexes[i])
	}
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

func (c *collector) getVacuumProgress() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT datname, relid, phase, heap_blks_total, heap_blks_scanned,
			heap_blks_vacuumed, index_vacuum_count, max_dead_tuples,
			num_dead_tuples
		  FROM pg_stat_progress_vacuum
		  ORDER BY pid ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_stat_progress_vacuum query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var p pgmetrics.VacuumProgressBackend
		if err := rows.Scan(&p.DBName, &p.TableOID, &p.Phase, &p.HeapBlksTotal,
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

	q := `SELECT name, current_database(), COALESCE(default_version, ''),
			COALESCE(installed_version, ''), comment
		  FROM pg_available_extensions
		  WHERE installed_version IS NOT NULL
		  ORDER BY name ASC`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_available_extensions query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var e pgmetrics.Extension
		if err := rows.Scan(&e.Name, &e.DBName, &e.DefaultVersion,
			&e.InstalledVersion, &e.Comment); err != nil {
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
	if c.version < 90500 { // RLS is only available in v9.5+
		q = strings.Replace(q, "R.rolbypassrls", "FALSE", 1)
	}
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("pg_roles/pg_auth_members query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var r pgmetrics.Role
		var validUntil float64
		if err := rows.Scan(&r.OID, &r.Name, &r.Rolsuper, &r.Rolinherit,
			&r.Rolcreaterole, &r.Rolcreatedb, &r.Rolcanlogin, &r.Rolreplication,
			&r.Rolbypassrls, &r.Rolconnlimit, &validUntil,
			pq.Array(&r.MemberOf)); err != nil {
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
			restart_lsn, confirmed_flush_lsn, temporary
		  FROM pg_replication_slots
		  ORDER BY slot_name ASC`
	if c.version < 90600 { // confirmed_flush_lsn only in v9.6+
		q = strings.Replace(q, "confirmed_flush_lsn", "''", 1)
	}
	if c.version < 100000 { // temporary only in v10+
		q = strings.Replace(q, "temporary", "FALSE", 1)
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
			&rs.Temporary); err != nil {
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

func (c *collector) getStatements() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT userid, dbid, queryid, LEFT(query, $1), calls, total_time,
			min_time, max_time, stddev_time, rows, shared_blks_hit,
			shared_blks_read, shared_blks_dirtied, shared_blks_written,
			local_blks_hit, local_blks_read, local_blks_dirtied,
			local_blks_written, temp_blks_read, temp_blks_written,
			blk_read_time, blk_write_time
		  FROM pg_stat_statements
		  ORDER BY total_time DESC
		  LIMIT $2`
	rows, err := c.db.QueryContext(ctx, q, c.sqlLength, c.stmtsLimit)
	if err != nil {
		// ignore any errors, pg_stat_statements extension may not be
		// available
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

func (c *collector) getWALCounts() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q1 := `SELECT COUNT(*) FROM pg_ls_dir('pg_xlog') WHERE pg_ls_dir ~ '^[0-9A-F]{24}'`
	q2 := `SELECT COUNT(*) FROM pg_ls_dir('pg_xlog/archive_status') WHERE pg_ls_dir ~ '^[0-9A-F]{24}.ready'`

	if c.version >= 100000 {
		q1 = strings.Replace(q1, "pg_xlog", "pg_wal", -1)
		q2 = strings.Replace(q2, "pg_xlog", "pg_wal", -1)
	}

	if err := c.db.QueryRowContext(ctx, q1).Scan(&c.result.WALCount); err != nil {
		c.result.WALCount = -1 // ignore errors, need superuser
	}
	if err := c.db.QueryRowContext(ctx, q2).Scan(&c.result.WALReadyCount); err != nil {
		c.result.WALReadyCount = -1 // ignore errors, need superuser
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
		var dummy [14]string // we don't want to edit sqlBloat!
		var wasted int64
		if err := rows.Scan(&dbname, &schemaname, &tablename, &dummy[0],
			&dummy[1], &dummy[2], &dummy[3], &dummy[4], &dummy[5], &dummy[6],
			&indexname, &dummy[7], &dummy[8], &dummy[9], &dummy[10], &dummy[11],
			&dummy[12], &dummy[13], &wasted); err != nil {
			log.Fatalf("bloat query failed: %v", err)
		}
		if t := c.result.TableByName(dbname, schemaname, tablename); t != nil && t.Bloat == -1 {
			t.Bloat = wasted
		}
		if indexname != "?" {
			if i := c.result.IndexByName(dbname, schemaname, indexname); i != nil && i.Bloat == -1 {
				i.Bloat = wasted
			}
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("bloat query failed: %v", err)
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
  CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
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
