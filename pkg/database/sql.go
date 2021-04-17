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

	stmts, err := sql.Parse(strings.NewReader(req.Sql))
	if err != nil {
		return nil, err
	}

	stmt, ok := stmts[0].(*sql.SelectStmt)
	if !ok {
		return nil, ErrIllegalArguments
	}

	return d.SQLQueryPrepared(stmt, req.Params, int(req.Limit))
}

func (d *db) SQLQueryPrepared(stmt *sql.SelectStmt, namedParams []*schema.NamedParam, limit int) (*schema.SQLQueryResult, error) {
	if stmt == nil {
		return nil, ErrIllegalArguments
	}

	if limit > MaxKeyScanLimit {
		return nil, ErrMaxKeyScanLimitExceeded
	}

	if limit == 0 {
		limit = MaxKeyScanLimit
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	params := make(map[string]interface{})

	for _, p := range namedParams {
		params[p.Name] = p.Value
	}

	r, err := d.sqlEngine.QueryPreparedStmt(stmt, params)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var cols []*schema.Column

	colDescriptors, err := r.Columns()
	if err != nil {
		return nil, err
	}

	for c, t := range colDescriptors {
		cols = append(cols, &schema.Column{Name: c, Type: t})
	}

	res := &schema.SQLQueryResult{Columns: cols}

	for l := 0; l < limit; l++ {
		row, err := r.Read()
		if err == sql.ErrNoMoreRows {
			break
		}
		if err != nil {
			return nil, err
		}

		rrow := &schema.Row{
			Values: make([]*schema.RowValue, len(row.Values)),
		}

		for i, c := range res.Columns {
			v := row.Values[c.Name]

			_, isNull := v.(*sql.NullValue)
			if isNull {
				rrow.Values[i] = &schema.RowValue{Operation: &schema.RowValue_Null{}}
			} else {
				rrow.Values[i] = typedValueToRowValue(v)
			}
		}

		res.Rows = append(res.Rows, rrow)
	}

	return res, nil
}

func typedValueToRowValue(tv sql.TypedValue) *schema.RowValue {
	switch tv.Type() {
	case sql.IntegerType:
		{
			return &schema.RowValue{Operation: &schema.RowValue_N{N: tv.Value().(uint64)}}
		}
	case sql.StringType:
		{
			return &schema.RowValue{Operation: &schema.RowValue_S{S: tv.Value().(string)}}
		}
	case sql.BooleanType:
		{
			return &schema.RowValue{Operation: &schema.RowValue_V{V: tv.Value().(bool)}}
		}
	case sql.BLOBType:
		{
			return &schema.RowValue{Operation: &schema.RowValue_B{B: tv.Value().([]byte)}}
		}
	}
	return nil
}
