/*
 * Copyright 2021 RapidLoop, Inc.
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
	"fmt"
	"log"
	"strings"

	"github.com/rapidloop/pgmetrics"
)

func (c *collector) getCitus(currdb string, fillSize bool) {
	// check if citus extension is present in current database
	found := false
	for _, e := range c.result.Extensions {
		if e.Name == "citus" && e.DBName == currdb {
			found = true
			break
		}
	}
	if !found {
		return
	}

	// setup result
	if c.result.Citus == nil {
		c.result.Citus = make(map[string]*pgmetrics.Citus)
	}
	if c.result.Citus[currdb] == nil {
		c.result.Citus[currdb] = &pgmetrics.Citus{}
	}

	// get version
	c.getCitusVersion(currdb)

	// get size (if not explicitly disabled)
	if fillSize {
		c.getCitusTableSizes(currdb)
	}

	c.getCitusNodes(currdb)          // pg_dist_node
	c.getCitusStatements(currdb)     // citus_stat_statements
	c.getCitusDistActivity(currdb)   // citus_dist_stat_activity
	c.getCitusWorkerActivity(currdb) // citus_worker_stat_activity
	c.getCitusLocks(currdb)          // citus_lock_waits
}

func (c *collector) getCitusVersion(currdb string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	var cv string
	q := `SELECT citus_version()`
	if err := c.db.QueryRowContext(ctx, q).Scan(&cv); err != nil {
		log.Printf("warning: citus_version() in db %q failed:: %v", currdb, err)
		return
	}
	c.result.Citus[currdb].Version = cv
}

func (c *collector) getCitusTableSizes(currdb string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT logicalrelid::oid, citus_table_size(logicalrelid) FROM pg_dist_partition`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: pg_dist_partition/citus_table_size query failed: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var oid int
		var size int64
		if err := rows.Scan(&oid, &size); err != nil {
			log.Printf("warning: pg_dist_partition/citus_table_size query failed: %v", err)
			return
		}
		for i, t := range c.result.Tables { // update sizes
			if t.OID == oid && t.DBName == currdb {
				c.result.Tables[i].Size = size
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("warning: pg_dist_partition/citus_table_size query failed: %v", err)
	}
}

func (c *collector) getCitusNodes(currdb string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT nodeid, groupid, nodename, nodeport, COALESCE(noderack, ''),
                 isactive, noderole, nodecluster, shouldhaveshards
            FROM pg_dist_node`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: pg_dist_node query failed: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var node pgmetrics.CitusNode
		if err := rows.Scan(&node.ID, &node.GroupID, &node.Name, &node.Port,
			&node.Rack, &node.IsActive, &node.Role, &node.Cluster,
			&node.ShouldHaveShards); err != nil {
			log.Printf("warning: pg_dist_node query failed: %v", err)
			return
		}
		c.result.Citus[currdb].Nodes = append(c.result.Citus[currdb].Nodes, node)
	}
	if err := rows.Err(); err != nil {
		log.Printf("warning: pg_dist_node query failed: %v", err)
	}
}

// citus_stat_statements
func (c *collector) getCitusStatements(currdb string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT queryid, userid, dbid, query, executor, partition_key, calls
            FROM citus_stat_statements`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		if strings.Contains(err.Error(), "Citus Enterprise") {
			err = nil // silently ignore this "error"
		} else {
			log.Printf("warning: citus_stat_statements query failed: %v", err)
		}
		return
	}
	defer rows.Close()

	for rows.Next() {
		var s pgmetrics.CitusStatement
		if err := rows.Scan(&s.QueryID, &s.UserOID, &s.DBOID, &s.Query,
			&s.Executor, &s.PartitionKey, &s.Calls); err != nil {
			log.Printf("warning: citus_stat_statements query failed: %v", err)
			return
		}
		c.result.Citus[currdb].Statements = append(c.result.Citus[currdb].Statements, s)
	}
	if err := rows.Err(); err != nil {
		log.Printf("warning: citus_stat_statements query failed: %v", err)
	}
}

func (c *collector) getCitusBackends(table string) []pgmetrics.CitusBackend {
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
			COALESCE(backend_xmin, ''), LEFT(COALESCE(query, ''), $1),
			COALESCE(query_hostname, ''), COALESCE(query_hostport, 0),
			COALESCE(master_query_host_name, ''),
			COALESCE(master_query_host_port, 0),
			COALESCE(transaction_number, 0),
			COALESCE(EXTRACT(EPOCH FROM transaction_stamp)::bigint, 0)
		  FROM %s ORDER BY pid ASC`
	q = fmt.Sprintf(q, table)
	rows, err := c.db.QueryContext(ctx, q, c.sqlLength)
	if err != nil {
		log.Printf("warning: %s query failed: %v", table, err)
		return nil
	}
	defer rows.Close()

	var out []pgmetrics.CitusBackend
	for rows.Next() {
		var b pgmetrics.CitusBackend
		if err := rows.Scan(&b.DBName, &b.RoleName, &b.ApplicationName,
			&b.PID, &b.ClientAddr, &b.BackendStart, &b.XactStart, &b.QueryStart,
			&b.StateChange, &b.WaitEventType, &b.WaitEvent, &b.State,
			&b.BackendXid, &b.BackendXmin, &b.Query, &b.QueryHostname,
			&b.QueryPort, &b.MasterQueryHostname, &b.MasterQueryPort,
			&b.TxNumber, &b.TxStamp); err != nil {
			log.Printf("warning: %s query failed: %v", table, err)
			return nil
		}
		out = append(out, b)
	}
	if err := rows.Err(); err != nil {
		log.Printf("warning: %s query failed: %v", table, err)
		return nil
	}
	return out
}

// citus_dist_stat_activity
func (c *collector) getCitusDistActivity(currdb string) {
	c.result.Citus[currdb].Backends = c.getCitusBackends("citus_dist_stat_activity")
}

// citus_worker_stat_activity
func (c *collector) getCitusWorkerActivity(currdb string) {
	c.result.Citus[currdb].WorkerBackends = c.getCitusBackends("citus_worker_stat_activity")
}

// citus_lock_waits
func (c *collector) getCitusLocks(currdb string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	q := `SELECT waiting_pid, blocking_pid, blocked_statement,
			current_statement_in_blocking_process, waiting_node_id,
			blocking_node_id, waiting_node_name, blocking_node_name,
			waiting_node_port, blocking_node_port
          FROM citus_lock_waits`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		log.Printf("warning: citus_lock_waits query failed: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var l pgmetrics.CitusLock
		if err := rows.Scan(&l.WaitingPID, &l.BlockingPID, &l.BlockedStmt,
			&l.CurrStmt, &l.WaitingNodeID, &l.BlockingNodeID,
			&l.WaitingNodeName, &l.BlockingNodeName, &l.WaitingNodePort,
			&l.BlockingNodePort); err != nil {
			log.Printf("warning: citus_lock_waits query failed: %v", err)
			return
		}
		c.result.Citus[currdb].Locks = append(c.result.Citus[currdb].Locks, l)
	}
	if err := rows.Err(); err != nil {
		log.Printf("warning: citus_lock_waits query failed: %v", err)
	}
}
