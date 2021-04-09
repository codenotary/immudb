/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package database

import (
	"errors"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/sql"
)

func (d *db) SQLExec(req *schema.SQLExecRequest) (*schema.SQLExecResult, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	stmts, err := sql.Parse(strings.NewReader(req.Sql))
	if err != nil {
		return nil, err
	}

	for _, stmt := range stmts {
		switch stmt.(type) {
		case *sql.UseDatabaseStmt:
			{
				return nil, errors.New("SQL statement not supported. Please use `UseDatabase` operation instead")
			}
		case *sql.CreateDatabaseStmt:
			{
				return nil, errors.New("SQL statement not supported. Please use `CreateDatabase` operation instead")
			}
		}
	}

	return d.SQLExecPrepared(stmts, req.Params, !req.NoWait)
}

func (d *db) SQLExecPrepared(stmts []sql.SQLStmt, namedParams []*schema.NamedParam, waitForIndexing bool) (*schema.SQLExecResult, error) {
	if len(stmts) == 0 {
		return nil, ErrIllegalArguments
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	params := make(map[string]interface{})

	for _, p := range namedParams {
		params[p.Name] = p.Value
	}

	ddTxs, dmTxs, err := d.sqlEngine.ExecPreparedStmts(stmts, params, waitForIndexing)
	if err != nil {
		return nil, err
	}

	res := &schema.SQLExecResult{
		Ctxs: make([]*schema.TxMetadata, len(ddTxs)),
		Dtxs: make([]*schema.TxMetadata, len(dmTxs)),
	}

	for i, md := range ddTxs {
		res.Ctxs[i] = schema.TxMetatadaTo(md)
	}

	for i, md := range dmTxs {
		res.Dtxs[i] = schema.TxMetatadaTo(md)
	}

	return res, nil
}

func (d *db) SQLQuery(req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	params := make(map[string]interface{})

	for _, p := range req.Params {
		params[p.Name] = p.Value
	}

	r, err := d.sqlEngine.QueryStmt(req.Sql, params)
	if err != nil {
		return nil, err
	}

	cols := make([]*schema.Column, len(r.Columns()))

	for i, c := range r.Columns() {
		cols[i] = &schema.Column{
			Name: c.ColName,
			Type: c.ColType,
		}
	}

	res := &schema.SQLQueryResult{Columns: cols}

	for l := 0; l < int(req.Limit); l++ {
		row, err := r.Read()
		if err == sql.ErrNoMoreRows {
			break
		}

		rrow := &schema.Row{
			Values: make([]*schema.RowValue, len(row.Values)),
		}

		for i, c := range res.Columns {
			v, isDefined := row.Values[c.Name]

			if !isDefined {
				rrow.Values[i] = nil
			} else {
				rrow.Values[i] = typedValueToRowValue(v)
			}
		}

		res.Rows = append(res.Rows, rrow)
	}

	err = r.Close()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func typedValueToRowValue(tv sql.TypedValue) *schema.RowValue {
	switch v := tv.(type) {
	case *sql.Number:
		{
			return &schema.RowValue{Operation: &schema.RowValue_N{N: v.Value().(uint64)}}
		}
	case *sql.String:
		{
			return &schema.RowValue{Operation: &schema.RowValue_S{S: v.Value().(string)}}
		}
	case *sql.Bool:
		{
			return &schema.RowValue{Operation: &schema.RowValue_V{V: v.Value().(bool)}}
		}
	case *sql.Blob:
		{
			return &schema.RowValue{Operation: &schema.RowValue_B{B: v.Value().([]byte)}}
		}
	}
	return nil
}
