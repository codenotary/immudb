//go:build wasip1

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

package main

import (
	"errors"

	"github.com/codenotary/immudb/embedded/sql"
)

// ensureSQL lazily creates the SQL engine on first use (the store is already
// opened with multi-indexing, which the engine requires).
func (e *engine) ensureSQL() error {
	if e.sqlEngine != nil {
		return nil
	}
	eng, err := sql.NewEngine(e.st, sql.DefaultOptions().WithPrefix(sqlPrefix))
	if err != nil {
		return err
	}
	e.sqlEngine = eng
	return nil
}

// sqlExec runs one or more SQL statements (DDL/DML).
func (e *engine) sqlExec(stmt string) error {
	if err := e.ensureSQL(); err != nil {
		return err
	}
	_, _, err := e.sqlEngine.Exec(ctx, nil, stmt, nil)
	return err
}

type sqlQueryResult struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// sqlQuery runs a SELECT and returns columns plus rows of raw values.
func (e *engine) sqlQuery(stmt string) (sqlQueryResult, error) {
	var res sqlQueryResult
	if err := e.ensureSQL(); err != nil {
		return res, err
	}
	reader, err := e.sqlEngine.Query(ctx, nil, stmt, nil)
	if err != nil {
		return res, err
	}
	defer reader.Close()

	cols, err := reader.Columns(ctx)
	if err != nil {
		return res, err
	}
	res.Columns = make([]string, len(cols))
	for i, c := range cols {
		res.Columns[i] = c.Column
	}

	for {
		row, rerr := reader.Read(ctx)
		if errors.Is(rerr, sql.ErrNoMoreRows) {
			break
		}
		if rerr != nil {
			return res, rerr
		}
		vals := make([]any, len(row.ValuesByPosition))
		for i, v := range row.ValuesByPosition {
			vals[i] = v.RawValue()
		}
		res.Rows = append(res.Rows, vals)
	}
	return res, nil
}
