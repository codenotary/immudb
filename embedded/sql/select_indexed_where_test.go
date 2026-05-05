/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1

you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/
*/

package sql

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSelectIndexedWhereUsesSecondaryIndex is the regression guard for issue
// #2080: a plain `SELECT * FROM t WHERE col = ?` against a single-column
// secondary index must produce a ScanSpecs whose Index is that secondary
// index — i.e. the planner emits a tbtree range scan over the index, not a
// full primary-key scan.
//
// The reporter saw 1.0x speedup with secondary indexes on v1.10.0; the INLJ
// equality fallback that fixes this only landed in v1.11.0 (commit 4e9926b2).
// This test pins the planner behaviour so the bug cannot regress for either
// VARCHAR or INTEGER leading columns.
func TestSelectIndexedWhereUsesSecondaryIndex(t *testing.T) {
	engine := setupCommonTest(t)
	ctx := context.Background()

	mustExec := func(stmt string) {
		t.Helper()
		_, _, err := engine.Exec(ctx, nil, stmt, nil)
		require.NoError(t, err, stmt)
	}

	// Schema mirrors the reporter's table shape: integer PK plus several
	// VARCHAR/INTEGER columns each with their own secondary index.
	mustExec(`CREATE TABLE historytable (
		id INTEGER AUTO_INCREMENT,
		transactionHash VARCHAR[66] NOT NULL,
		fromAddr VARCHAR[42] NOT NULL,
		toAddr VARCHAR[42],
		blockNumber INTEGER NOT NULL,
		PRIMARY KEY (id)
	);`)
	mustExec(`CREATE INDEX ON historytable(transactionHash);`)
	mustExec(`CREATE INDEX ON historytable(fromAddr);`)
	mustExec(`CREATE INDEX ON historytable(toAddr);`)
	mustExec(`CREATE INDEX ON historytable(blockNumber);`)

	for i := 0; i < 50; i++ {
		mustExec(fmt.Sprintf(
			`INSERT INTO historytable (transactionHash, fromAddr, toAddr, blockNumber) VALUES ('h%d', 'f%d', 't%d', %d);`,
			i, i%5, i%7, i%10))
	}

	tableTx, err := engine.NewTx(ctx, DefaultTxOptions())
	require.NoError(t, err)
	defer tableTx.Cancel()

	table, err := tableTx.catalog.GetTableByName("historytable")
	require.NoError(t, err)

	// Index lookups use the catalog's stored column name, which immudb
	// folds to lowercase regardless of the CREATE TABLE casing.
	indexFor := func(colName string) *Index {
		t.Helper()
		for _, idx := range table.indexes {
			if !idx.IsPrimary() && len(idx.cols) == 1 && idx.cols[0].colName == colName {
				return idx
			}
		}
		t.Fatalf("secondary index on %q not found", colName)
		return nil
	}

	cases := []struct {
		name  string
		query string
		col   string // catalog name (lowercase)
	}{
		{"varchar leading column (transactionHash)",
			`SELECT * FROM historytable WHERE transactionHash = 'h7';`, "transactionhash"},
		{"varchar shorter column (fromAddr)",
			`SELECT * FROM historytable WHERE fromAddr = 'f3';`, "fromaddr"},
		{"varchar nullable (toAddr)",
			`SELECT * FROM historytable WHERE toAddr = 't4';`, "toaddr"},
		{"integer leading column (blockNumber)",
			`SELECT * FROM historytable WHERE blockNumber = 5;`, "blocknumber"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := engine.Query(ctx, nil, tc.query, nil)
			require.NoError(t, err)
			defer r.Close()

			specs := r.ScanSpecs()
			require.NotNil(t, specs)
			require.False(t, specs.Index.IsPrimary(),
				"selected index must be a secondary index (regression for #2080)")
			// The catalog used inside engine.Query lives in a different tx
			// than the one we read above, so compare by id and leading col
			// name rather than by pointer identity.
			expected := indexFor(tc.col)
			require.Equal(t, expected.id, specs.Index.id,
				"planner must pick the secondary index on %s for plain WHERE equality", tc.col)
			require.Equal(t, tc.col, specs.Index.cols[0].colName,
				"selected index leading column must be %s", tc.col)

			// And the query still returns a correct result set.
			seen := 0
			for {
				row, err := r.Read(ctx)
				if err == ErrNoMoreRows {
					break
				}
				require.NoError(t, err)
				require.NotNil(t, row)
				seen++
			}
			require.Greater(t, seen, 0, "expected at least one matching row")
		})
	}
}
