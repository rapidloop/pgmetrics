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

package pgmetrics

// ModelSchemaVersion is the schema version of the "Model" data structure
// defined below. It is in the "semver" notation. Version history:
//    1.3 - locks information
//    1.2 - more table and index attributes
//    1.1 - added NotificationQueueUsage and Statements
//    1.0 - initial release
const ModelSchemaVersion = "1.3"

// Model contains the entire information collected by a single run of
// pgmetrics. It can be converted to and from json without loss of
// precision.
type Model struct {
	Metadata Metadata `json:"meta"` // metadata about this object

	StartTime        int64  `json:"start_time"`        // of postmaster
	SystemIdentifier string `json:"system_identifier"` // from pg_control

	// Checkpoint information
	CheckpointLSN   string `json:"checkpoint_lsn"`
	PriorLSN        string `json:"prior_lsn"`
	RedoLSN         string `json:"redo_lsn"`
	TimelineID      int    `json:"timeline_id"`
	NextXid         int    `json:"next_xid"`
	OldestXid       int    `json:"oldest_xid"`
	OldestActiveXid int    `json:"oldest_active_xid"`
	CheckpointTime  int64  `json:"checkpoint_time"`

	// wal
	WALFlushLSN  string `json:"wal_flush_lsn"`
	WALInsertLSN string `json:"wal_insert_lsn"`
	WALLSN       string `json:"wal_lsn"`

	// Recovery
	IsInRecovery            bool   `json:"is_in_recovery"`
	IsWalReplayPaused       bool   `json:"is_wal_replay_paused"`
	LastWALReceiveLSN       string `json:"last_wal_receive_lsn"`
	LastWALReplayLSN        string `json:"last_wal_replay_lsn"`
	LastXActReplayTimestamp int64  `json:"last_xact_replay_timestamp"`

	// last committed transaction (needs track_commit_timestamp = on)
	LastXactXid       int   `json:"last_xact_xid"`
	LastXactTimestamp int64 `json:"last_xact_time"`

	// wal - settings, archival stats
	WALArchiving  WALArchiving `json:"wal_archiving"`
	WALCount      int          `json:"wal_count"`
	WALReadyCount int          `json:"wal_ready_count"`

	// NotificationQueueUsage is the fraction of the asynchronous notification
	// queue currently occupied. Postgres v9.6 and above only. Added in
	// schema version 1.1.
	NotificationQueueUsage float64 `json:"notification_queue_usage"`

	// replication
	ReplicationOutgoing []ReplicationOut  `json:"replication_outgoing,omitempty"`
	ReplicationIncoming *ReplicationIn    `json:"replication_incoming,omitempty"`
	ReplicationSlots    []ReplicationSlot `json:"replication_slots,omitempty"`

	// other cluster-level stats
	BGWriter       BGWriter                `json:"bg_writer"`
	Backends       []Backend               `json:"backends"`
	VacuumProgress []VacuumProgressBackend `json:"vacuum_progress,omitempty"`

	// structural cluster-level information
	Roles       []Role       `json:"roles"`
	Databases   []Database   `json:"databases,omitempty"`
	Tablespaces []Tablespace `json:"tablespaces,omitempty"`

	// Database-specific
	Tables           []Table        `json:"tables,omitempty"`
	Indexes          []Index        `json:"indexes,omitempty"`
	Sequences        []Sequence     `json:"sequences,omitempty"`
	UserFunctions    []UserFunction `json:"user_functions,omitempty"`
	Extensions       []Extension    `json:"extensions,omitempty"`
	DisabledTriggers []Trigger      `json:"disabled_triggers,omitempty"`
	Statements       []Statement    `json:"statements,omitempty"`

	// System-level
	System *SystemMetrics `json:"system,omitempty"`

	// settings
	Settings map[string]Setting `json:"settings"` // all settings and their values

	// following fields present only in schema 1.2 and later

	// Logical replication (database-specific)
	Publications  []Publication  `json:"publications,omitempty"`
	Subscriptions []Subscription `json:"subscriptions,omitempty"`

	// following fields present only in schema 1.3 and later

	// Lock information
	Locks        []Lock        `json:"locks,omitempty"`
	BlockingPIDs map[int][]int `json:"blocking_pids,omitempty"`
}

// DatabaseByOID iterates over the databases in the model and returns the reference
// to a Database that has the given oid. If there is no such database, it returns nil.
func (m *Model) DatabaseByOID(oid int) *Database {
	for i, d := range m.Databases {
		if d.OID == oid {
			return &m.Databases[i]
		}
	}
	return nil
}

// RoleByOID iterates over the roles in the model and returns the reference
// to a Role that has the given oid. If there is no such role, it returns nil.
func (m *Model) RoleByOID(oid int) *Role {
	for i, r := range m.Roles {
		if r.OID == oid {
			return &m.Roles[i]
		}
	}
	return nil
}

// TableByName iterates over the tables in the model and returns the reference
// to a Table that has the given database, schema and table names. If there is
// no such table, it returns nil.
func (m *Model) TableByName(db, schema, table string) *Table {
	for i, t := range m.Tables {
		if t.DBName == db && t.SchemaName == schema && t.Name == table {
			return &m.Tables[i]
		}
	}
	return nil
}

// TableByOID iterates over the tables in the model and returns the reference
// to a Table that has the given oid. If there is no such table, it returns nil.
func (m *Model) TableByOID(oid int) *Table {
	for i, t := range m.Tables {
		if t.OID == oid {
			return &m.Tables[i]
		}
	}
	return nil
}

// IndexByName iterates over the indexes in the model and returns the reference
// to an Index that has the given database, schema and index names. If there is
// no such index, it returns nil.
func (m *Model) IndexByName(db, schema, index string) *Index {
	for i, idx := range m.Indexes {
		if idx.DBName == db && idx.SchemaName == schema && idx.Name == index {
			return &m.Indexes[i]
		}
	}
	return nil
}

// Metadata contains information about how to interpret the other fields in
// "Model" data structure.
type Metadata struct {
	Version      string   `json:"version"`       // schema version, "semver" format
	At           int64    `json:"at"`            // time when this report was started
	CollectedDBs []string `json:"collected_dbs"` // names of dbs we collected db-level stats from
	Local        bool     `json:"local"`         // was connected to a local postgres server?
}

type SystemMetrics struct {
	CPUModel   string  `json:"cpu_model,omitempty"` // model of the CPU
	NumCores   int     `json:"num_cores"`           // number of cores
	LoadAvg    float64 `json:"loadavg"`             // 1-minute load average from the OS
	MemUsed    int64   `json:"memused"`             // used RAM, in bytes
	MemFree    int64   `json:"memfree"`             // free RAM, in bytes
	MemBuffers int64   `json:"membuffers"`          // RAM used for buffers, in bytes
	MemCached  int64   `json:"memcached"`           // RAM used for cache, in bytes
	SwapUsed   int64   `json:"swapused"`            // used swap memory in bytes, 0 if no swap
	SwapFree   int64   `json:"swapfree"`            // free swap memory in bytes, 0 if no swap
	Hostname   string  `json:"hostname"`            // hostname from the OS
}

type Backend struct {
	DBName          string `json:"db_name"`
	RoleName        string `json:"role_name"`
	ApplicationName string `json:"application_name"`
	PID             int    `json:"pid"`
	ClientAddr      string `json:"client_addr"`
	BackendStart    int64  `json:"backend_start"`
	XactStart       int64  `json:"xact_start"`
	QueryStart      int64  `json:"query_start"`
	StateChange     int64  `json:"state_change"`
	WaitEventType   string `json:"wait_event_type"`
	WaitEvent       string `json:"wait_event"`
	State           string `json:"state"`
	BackendXid      int    `json:"backend_xid"`
	BackendXmin     int    `json:"backend_xmin"`
	Query           string `json:"query"`
}

type ReplicationSlot struct {
	SlotName          string `json:"slot_name"`
	Plugin            string `json:"plugin"`
	SlotType          string `json:"slot_type"`
	DBName            string `json:"db_name"`
	Active            bool   `json:"active"`
	Xmin              int    `json:"xmin"`
	CatalogXmin       int    `json:"catalog_xmin"`
	RestartLSN        string `json:"restart_lsn"`
	ConfirmedFlushLSN string `json:"confirmed_flush_lsn"`
	Temporary         bool   `json:"temporary"`
}

type Role struct {
	OID            int      `json:"oid"`
	Name           string   `json:"name"`
	Rolsuper       bool     `json:"rolsuper"`
	Rolinherit     bool     `json:"rolinherit"`
	Rolcreaterole  bool     `json:"rolcreaterole"`
	Rolcreatedb    bool     `json:"rolcreatedb"`
	Rolcanlogin    bool     `json:"rolcanlogin"`
	Rolreplication bool     `json:"rolreplication"`
	Rolbypassrls   bool     `json:"rolbypassrls"`
	Rolconnlimit   int      `json:"rolconnlimit"`
	Rolvaliduntil  int64    `json:"rolvaliduntil"`
	MemberOf       []string `json:"memberof"`
}

type Tablespace struct {
	OID         int    `json:"oid"`
	Name        string `json:"name"`
	Owner       string `json:"owner"`
	Location    string `json:"location"`
	Size        int64  `json:"size"`
	DiskUsed    int64  `json:"disk_used"`
	DiskTotal   int64  `json:"disk_total"`
	InodesUsed  int64  `json:"inodes_used"`
	InodesTotal int64  `json:"inodes_total"`
}

type Database struct {
	OID             int     `json:"oid"`
	Name            string  `json:"name"`
	DatDBA          int     `json:"datdba"`
	DatTablespace   int     `json:"dattablespace"`
	DatConnLimit    int     `json:"datconnlimit"`
	AgeDatFrozenXid int     `json:"age_datfrozenxid"`
	NumBackends     int     `json:"numbackends"`
	XactCommit      int64   `json:"xact_commit"`
	XactRollback    int64   `json:"xact_rollback"`
	BlksRead        int64   `json:"blks_read"`
	BlksHit         int64   `json:"blks_hit"`
	TupReturned     int64   `json:"tup_returned"`
	TupFetched      int64   `json:"tup_fetched"`
	TupInserted     int64   `json:"tup_inserted"`
	TupUpdated      int64   `json:"tup_updated"`
	TupDeleted      int64   `json:"tup_deleted"`
	Conflicts       int64   `json:"conflicts"`
	TempFiles       int64   `json:"temp_files"`
	TempBytes       int64   `json:"temp_bytes"`
	Deadlocks       int64   `json:"deadlocks"`
	BlkReadTime     float64 `json:"blk_read_time"`
	BlkWriteTime    float64 `json:"blk_write_time"`
	StatsReset      int64   `json:"stats_reset"`
	Size            int64   `json:"size"`
}

type Table struct {
	OID              int    `json:"oid"`
	DBName           string `json:"db_name"`
	SchemaName       string `json:"schema_name"`
	Name             string `json:"name"`
	SeqScan          int64  `json:"seq_scan"`
	SeqTupRead       int64  `json:"seq_tup_read"`
	IdxScan          int64  `json:"idx_scan"`
	IdxTupFetch      int64  `json:"idx_tup_fetch"`
	NTupIns          int64  `json:"n_tup_ins"`
	NTupUpd          int64  `json:"n_tup_upd"`
	NTupDel          int64  `json:"n_tup_del"`
	NTupHotUpd       int64  `json:"n_tup_hot_upd"`
	NLiveTup         int64  `json:"n_live_tup"`
	NDeadTup         int64  `json:"n_dead_tup"`
	NModSinceAnalyze int64  `json:"n_mod_since_analyze"`
	LastVacuum       int64  `json:"last_vacuum"`
	LastAutovacuum   int64  `json:"last_autovacuum"`
	LastAnalyze      int64  `json:"last_analyze"`
	LastAutoanalyze  int64  `json:"last_autoanalyze"`
	VacuumCount      int64  `json:"vacuum_count"`
	AutovacuumCount  int64  `json:"autovacuum_count"`
	AnalyzeCount     int64  `json:"analyze_count"`
	AutoanalyzeCount int64  `json:"autoanalyze_count"`
	HeapBlksRead     int64  `json:"heap_blks_read"`
	HeapBlksHit      int64  `json:"heap_blks_hit"`
	IdxBlksRead      int64  `json:"idx_blks_read"`
	IdxBlksHit       int64  `json:"idx_blks_hit"`
	ToastBlksRead    int64  `json:"toast_blks_read"`
	ToastBlksHit     int64  `json:"toast_blks_hit"`
	TidxBlksRead     int64  `json:"tidx_blks_read"`
	TidxBlksHit      int64  `json:"tidx_blks_hit"`
	Size             int64  `json:"size"`
	Bloat            int64  `json:"bloat"`
	// following fields present only in schema 1.2 and later
	RelKind         string `json:"relkind"`
	RelPersistence  string `json:"relpersistence"`
	RelNAtts        int    `json:"relnatts"`
	AgeRelFrozenXid int    `json:"age_relfrozenxid"`
	RelIsPartition  bool   `json:"relispartition"`
	TablespaceName  string `json:"tablespace_name"`
	ParentName      string `json:"parent_name"`
	PartitionCV     string `json:"partition_cv"` // partition constraint value
}

type Index struct {
	OID         int    `json:"oid"`
	DBName      string `json:"db_name"`
	SchemaName  string `json:"schema_name"`
	TableOID    int    `json:"table_oid"`
	TableName   string `json:"table_name"`
	Name        string `json:"name"`
	IdxScan     int64  `json:"idx_scan"`
	IdxTupRead  int64  `json:"idx_tup_read"`
	IdxTupFetch int64  `json:"idx_tup_fetch"`
	IdxBlksRead int64  `json:"idx_blks_read"`
	IdxBlksHit  int64  `json:"idx_blks_hit"`
	Size        int64  `json:"size"`
	Bloat       int64  `json:"bloat"`
	// following fields present only in schema 1.2 and later
	RelNAtts       int    `json:"relnatts"`
	AMName         string `json:"amname"`
	TablespaceName string `json:"tablespace_name"`
}

type Sequence struct {
	OID        int    `json:"oid"`
	DBName     string `json:"db_name"`
	SchemaName string `json:"schema_name"`
	Name       string `json:"name"`
	BlksRead   int64  `json:"blks_read"`
	BlksHit    int64  `json:"blks_hit"`
}

type UserFunction struct {
	OID        int     `json:"oid"`
	SchemaName string  `json:"schema_name"`
	DBName     string  `json:"db_name"`
	Name       string  `json:"name"`
	Calls      int64   `json:"calls"`
	TotalTime  float64 `json:"total_time"`
	SelfTime   float64 `json:"self_time"`
}

type VacuumProgressBackend struct {
	DBName           string `json:"db_name"`
	TableOID         int    `json:"table_oid"`
	TableName        string `json:"table_name"`
	Phase            string `json:"phase"`
	HeapBlksTotal    int64  `json:"heap_blks_total"`
	HeapBlksScanned  int64  `json:"heap_blks_scanned"`
	HeapBlksVacuumed int64  `json:"heap_blks_vacuumed"`
	IndexVacuumCount int64  `json:"index_vacuum_count"`
	MaxDeadTuples    int64  `json:"max_dead_tuples"`
	NumDeadTuples    int64  `json:"num_dead_tuples"`
}

type Extension struct {
	Name             string `json:"name"`
	DBName           string `json:"db_name"`
	DefaultVersion   string `json:"default_version"`
	InstalledVersion string `json:"installed_version"`
	Comment          string `json:"comment"`
}

type Setting struct {
	Setting string `json:"setting"`
	BootVal string `json:"bootval,omitempty"`
	Source  string `json:"source,omitempty"`
}

type WALArchiving struct {
	ArchivedCount    int    `json:"archived_count"`
	LastArchivedWAL  string `json:"last_archived_wal"`
	LastArchivedTime int64  `json:"last_archived_time"`
	FailedCount      int    `json:"failed_count"`
	LastFailedWAL    string `json:"last_failed_wal"`
	LastFailedTime   int64  `json:"last_failed_time"`
	StatsReset       int64  `json:"stats_reset"`
}

type BGWriter struct {
	CheckpointsTimed     int64   `json:"checkpoints_timed"`
	CheckpointsRequested int64   `json:"checkpoints_req"`
	CheckpointWriteTime  float64 `json:"checkpoint_write_time"`
	CheckpointSyncTime   float64 `json:"checkpoint_sync_time"`
	BuffersCheckpoint    int64   `json:"buffers_checkpoint"`
	BuffersClean         int64   `json:"buffers_clean"`
	MaxWrittenClean      int64   `json:"maxwritten_clean"`
	BuffersBackend       int64   `json:"buffers_backend"`
	BuffersBackendFsync  int64   `json:"buffers_backend_fsync"`
	BuffersAlloc         int64   `json:"buffers_alloc"`
	StatsReset           int64   `json:"stats_reset"`
}

type ReplicationOut struct {
	RoleName        string `json:"role_name"`
	ApplicationName string `json:"application_name"`
	ClientAddr      string `json:"client_addr"`
	BackendStart    int64  `json:"backend_start"`
	BackendXmin     int    `json:"backend_xmin"`
	State           string `json:"state"`
	SentLSN         string `json:"sent_lsn"`
	WriteLSN        string `json:"write_lsn"`
	FlushLSN        string `json:"flush_lsn"`
	ReplayLSN       string `json:"replay_lsn"`
	WriteLag        int    `json:"write_lag"`  // only in 10.x
	FlushLag        int    `json:"flush_lag"`  // only in 10.x
	ReplayLag       int    `json:"replay_lag"` // only in 10.x
	SyncPriority    int    `json:"sync_priority"`
	SyncState       string `json:"sync_state"`
}

type ReplicationIn struct {
	Status             string `json:"status"`
	ReceiveStartLSN    string `json:"receive_start_lsn"`
	ReceiveStartTLI    int    `json:"receive_start_tli"`
	ReceivedLSN        string `json:"received_lsn"`
	ReceivedTLI        int    `json:"received_tli"`
	LastMsgSendTime    int64  `json:"last_msg_send_time"`
	LastMsgReceiptTime int64  `json:"last_msg_receipt_time"`
	Latency            int64  `json:"latency_micros"`
	LatestEndLSN       string `json:"latest_end_lsn"`
	LatestEndTime      int64  `json:"latest_end_time"`
	SlotName           string `json:"slot_name"`
	Conninfo           string `json:"conninfo"`
}

type Trigger struct {
	OID        int    `json:"oid"`
	DBName     string `json:"db_name"`
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	Name       string `json:"name"`
	ProcName   string `json:"proc_name"`
}

// Statement represents a row of the pg_stat_statements view. Added in schema
// version 1.1.
type Statement struct {
	UserOID           int     `json:"useroid"`             // OID of user who executed the statement
	UserName          string  `json:"user"`                // Name of the user corresponding to useroid (might be empty)
	DBOID             int     `json:"db_oid"`              // OID of database in which the statement was executed
	DBName            string  `json:"db_name"`             // Name of the database corresponding to db_oid
	QueryID           int64   `json:"queryid"`             // Internal hash code, computed from the statement's parse tree
	Query             string  `json:"query"`               // Text of a representative statement
	Calls             int64   `json:"calls"`               // Number of times executed
	TotalTime         float64 `json:"total_time"`          // Total time spent in the statement, in milliseconds
	MinTime           float64 `json:"min_time"`            // Minimum time spent in the statement, in milliseconds
	MaxTime           float64 `json:"max_time"`            // Maximum time spent in the statement, in milliseconds
	StddevTime        float64 `json:"stddev_time"`         // Population standard deviation of time spent in the statement, in milliseconds
	Rows              int64   `json:"rows"`                // Total number of rows retrieved or affected by the statement
	SharedBlksHit     int64   `json:"shared_blks_hit"`     // Total number of shared block cache hits by the statement
	SharedBlksRead    int64   `json:"shared_blks_read"`    // Total number of shared blocks read by the statement
	SharedBlksDirtied int64   `json:"shared_blks_dirtied"` // Total number of shared blocks dirtied by the statement
	SharedBlksWritten int64   `json:"shared_blks_written"` // Total number of shared blocks written by the statement
	LocalBlksHit      int64   `json:"local_blks_hit"`      // Total number of local block cache hits by the statement
	LocalBlksRead     int64   `json:"local_blks_read"`     // Total number of local blocks read by the statement
	LocalBlksDirtied  int64   `json:"local_blks_dirtied"`  // Total number of local blocks dirtied by the statement
	LocalBlksWritten  int64   `json:"local_blks_written"`  // Total number of local blocks written by the statement
	TempBlksRead      int64   `json:"temp_blks_read"`      // Total number of temp blocks read by the statement
	TempBlksWritten   int64   `json:"temp_blks_written"`   // Total number of temp blocks written by the statement
	BlkReadTime       float64 `json:"blk_read_time"`       // Total time the statement spent reading blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)
	BlkWriteTime      float64 `json:"blk_write_time"`      // Total time the statement spent writing blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)
}

// Publication represents a single v10+ publication. Added in schema 1.2.
type Publication struct {
	OID        int    `json:"oid"`
	Name       string `json:"name"`
	DBName     string `json:"db_name"`
	AllTables  bool   `json:"alltables"`
	Insert     bool   `json:"insert"`
	Update     bool   `json:"update"`
	Delete     bool   `json:"delete"`
	TableCount int    `json:"table_count"`
}

// Subscription represents a single v10+ subscription. Added in schema 1.2.
type Subscription struct {
	OID                int    `json:"oid"`
	Name               string `json:"name"`
	DBName             string `json:"db_name"`
	Enabled            bool   `json:"enabled"`
	PubCount           int    `json:"pub_count"`
	TableCount         int    `json:"table_count"`
	WorkerCount        int    `json:"worker_count"`
	ReceivedLSN        string `json:"received_lsn"`
	LatestEndLSN       string `json:"latest_end_lsn"`
	LastMsgSendTime    int64  `json:"last_msg_send_time"`
	LastMsgReceiptTime int64  `json:"last_msg_receipt_time"`
	LatestEndTime      int64  `json:"latest_end_time"`
	Latency            int64  `json:"latency_micros"`
}

// Lock represents a single row from pg_locks. Added in schema 1.3.
type Lock struct {
	LockType    string `json:"locktype"`
	DBName      string `json:"db_name,omitempty"`
	PID         int    `json:"pid"`
	Mode        string `json:"mode"`
	Granted     bool   `json:"granted"`
	RelationOID int    `json:"relation_oid,omitempty"`
}
