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

package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/rapidloop/pgmetrics"
)

// model2csv writes the CSV representation of the model into the given CSV writer.
func model2csv(m *pgmetrics.Model, w *csv.Writer) (err error) {
	defer func() {
		// return panic(error) as our error value
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			}
		}
	}()

	// meta data
	struct2csv("pgmetrics.meta.", m.Metadata, w)

	// top-level fields
	struct2csv("pgmetrics.", *m, w)

	// wal archiving
	struct2csv("pgmetrics.wal_archiving.", m.WALArchiving, w)

	// replication out
	rec2csv("pgmetrics.repl_out.count", strconv.Itoa(len(m.ReplicationOutgoing)), w)
	for i, rout := range m.ReplicationOutgoing {
		head := fmt.Sprintf("pgmetrics.repl_out.%d.", i)
		struct2csv(head, rout, w)
	}

	// replication in
	if m.ReplicationIncoming != nil {
		struct2csv("pgmetrics.repl_in.", *m.ReplicationIncoming, w)
	}

	// replication slots
	rec2csv("pgmetrics.repl_slot.count", strconv.Itoa(len(m.ReplicationSlots)), w)
	for _, rs := range m.ReplicationSlots {
		head := fmt.Sprintf("pgmetrics.repl_slot.%s.", rs.SlotName)
		struct2csv(head, rs, w)
	}

	// bgwriter
	struct2csv("pgmetrics.bg_writer.", m.BGWriter, w)

	// backends
	rec2csv("pgmetrics.backends.count", strconv.Itoa(len(m.Backends)), w)
	for i, be := range m.Backends {
		head := fmt.Sprintf("pgmetrics.backends.%d.", i)
		struct2csv(head, be, w)
	}

	// vacuum progress
	rec2csv("pgmetrics.vacuum.count", strconv.Itoa(len(m.VacuumProgress)), w)
	for i, vp := range m.VacuumProgress {
		head := fmt.Sprintf("pgmetrics.vacuum.%d.", i)
		struct2csv(head, vp, w)
	}

	// databases
	rec2csv("pgmetrics.databases.count", strconv.Itoa(len(m.Databases)), w)
	for _, db := range m.Databases {
		head := fmt.Sprintf("pgmetrics.databases.%s.", db.Name)
		struct2csv(head, db, w)
	}

	// tablespaces
	rec2csv("pgmetrics.tablespaces.count", strconv.Itoa(len(m.Tablespaces)), w)
	for _, ts := range m.Tablespaces {
		head := fmt.Sprintf("pgmetrics.tablespaces.%s.", ts.Name)
		struct2csv(head, ts, w)
	}

	// tables
	rec2csv("pgmetrics.tables.count", strconv.Itoa(len(m.Tables)), w)
	for _, t := range m.Tables {
		head := fmt.Sprintf("pgmetrics.tables.%s.%s.%s.", t.DBName, t.SchemaName, t.Name)
		struct2csv(head, t, w)
	}

	// indexes
	rec2csv("pgmetrics.indexes.count", strconv.Itoa(len(m.Indexes)), w)
	for _, idx := range m.Indexes {
		head := fmt.Sprintf("pgmetrics.indexes.%s.%s.%s.", idx.DBName, idx.SchemaName, idx.Name)
		struct2csv(head, idx, w)
	}

	// locks
	rec2csv("pgmetrics.locks.count", strconv.Itoa(len(m.Locks)), w)
	for i, l := range m.Locks {
		head := fmt.Sprintf("pgmetrics.locks.%d.", i)
		struct2csv(head, l, w)
	}

	// system metrics
	if m.System != nil {
		struct2csv("pgmetrics.system.", *(m.System), w)
	}

	// note: sequences, user functions, extensions, disabled triggers, statements,
	// roles, blocking pids, publications, subscriptions and settings are not
	// written to the csv as of now inorder to keep csv size small.
	return
}

func struct2csv(head string, s interface{}, w *csv.Writer) {
	t := reflect.TypeOf(s)
	if t.Kind() != reflect.Struct {
		panic(errors.New("struct2csv: arg is not a struct"))
	}
	v := reflect.ValueOf(s)
	for i := 0; i < t.NumField(); i++ {
		// make key
		f := t.Field(i)
		j := strings.Replace(f.Tag.Get("json"), ",omitempty", "", 1)
		if len(j) == 0 || j == "-" {
			continue
		}
		// make value
		var sv string
		switch fv := v.Field(i); fv.Kind() {
		case reflect.Int, reflect.Int64, reflect.Bool, reflect.Float64:
			sv = fmt.Sprintf("%v", fv)
		case reflect.String:
			sv = cleanstr(fv.String())
		}
		if len(sv) > 0 {
			// write key, value into csv writer
			rec2csv(head+j, sv, w)
		}
	}
}

func rec2csv(key, value string, w *csv.Writer) {
	if err := w.Write([]string{key, value}); err != nil {
		panic(err)
	}
}

var cleanrepl = strings.NewReplacer("\t", " ", "\r", " ", "\n", " ")

func cleanstr(s string) string {
	s = cleanrepl.Replace(s)
	if len(s) > 1024 {
		s = s[:1024]
	}
	return s
}
