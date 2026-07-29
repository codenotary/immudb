/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Command embedded shows how to use immudb fully in-process, as a library,
// with no server and no container. It opens (or creates) a database directly on
// disk through pkg/database, then exercises verifiable key-value writes and the
// embedded SQL engine. This is the recommended starting point for embedding
// immudb into a Go application.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
)

const (
	dbName   = "plugindb"
	dataRoot = "./data-embedded"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	db, err := openOrCreate()
	if err != nil {
		return err
	}
	defer db.Close()

	if err := verifiableKV(ctx, db); err != nil {
		return err
	}
	return embeddedSQL(ctx, db)
}

// openOrCreate opens the database if its directory already exists, otherwise it
// creates a fresh one. Passing a nil sql.MultiDBHandler keeps this to a single,
// self-contained database (no cross-database "USE DATABASE" support needed).
func openOrCreate() (database.DB, error) {
	// keep logs quiet for the example; use logger.LogInfo to see engine startup
	log := logger.NewSimpleLoggerWithLevel("embedded", os.Stdout, logger.LogError)
	opts := database.DefaultOptions().WithDBRootPath(dataRoot)

	if _, err := os.Stat(filepath.Join(dataRoot, dbName)); errors.Is(err, os.ErrNotExist) {
		return database.NewDB(dbName, nil, opts, log)
	}
	return database.OpenDB(dbName, nil, opts, log)
}

// verifiableKV writes a key with a cryptographic proof and reads it back
// verifiably. VerifiableGet returns the entry together with the proof material
// the immudb client would use to check tamper-evidence.
func verifiableKV(ctx context.Context, db database.DB) error {
	key := []byte("audit:1")

	_, err := db.VerifiableSet(ctx, &schema.VerifiableSetRequest{
		SetRequest: &schema.SetRequest{
			KVs: []*schema.KeyValue{{Key: key, Value: []byte("record-A")}},
		},
	})
	if err != nil {
		return err
	}

	ventry, err := db.VerifiableGet(ctx, &schema.VerifiableGetRequest{
		KeyRequest: &schema.KeyRequest{Key: key},
	})
	if err != nil {
		return err
	}

	fmt.Printf("verifiable KV: %s = %s (tx %d)\n",
		ventry.Entry.Key, ventry.Entry.Value, ventry.Entry.Tx)
	return nil
}

// embeddedSQL runs the SQL engine in the same process. The statements are
// idempotent (CREATE TABLE IF NOT EXISTS + UPSERT) so this example can be run
// repeatedly against the same data directory.
func embeddedSQL(ctx context.Context, db database.DB) error {
	stmts := []string{
		"CREATE TABLE IF NOT EXISTS items (id INTEGER, name VARCHAR, PRIMARY KEY id)",
		"UPSERT INTO items (id, name) VALUES (1, 'widget'), (2, 'gadget')",
	}
	for _, stmt := range stmts {
		if _, _, err := db.SQLExec(ctx, nil, &schema.SQLExecRequest{Sql: stmt}); err != nil {
			return err
		}
	}

	reader, err := db.SQLQuery(ctx, nil, &schema.SQLQueryRequest{
		Sql: "SELECT id, name FROM items ORDER BY id",
	})
	if err != nil {
		return err
	}
	defer reader.Close()

	for {
		row, err := reader.Read(ctx)
		if errors.Is(err, sql.ErrNoMoreRows) {
			break
		}
		if err != nil {
			return err
		}
		fmt.Printf("row: id=%v name=%v\n",
			row.ValuesByPosition[0].RawValue(),
			row.ValuesByPosition[1].RawValue(),
		)
	}
	return nil
}
