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

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
)

func (d *db) ListTables() (*schema.SQLQueryResult, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	db, err := d.sqlEngine.Catalog().GetDatabaseByName(d.options.dbName)
	if err != nil {
		return nil, err
	}

	res := &schema.SQLQueryResult{Columns: []*schema.Column{{Name: "TABLE", Type: sql.VarcharType}}}

	for _, t := range db.GetTables() {
		res.Rows = append(res.Rows, &schema.Row{Values: []*schema.SQLValue{{Value: &schema.SQLValue_S{S: t.Name()}}}})
	}

	return res, nil
}

func (d *db) DescribeTable(tableName string) (*schema.SQLQueryResult, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	table, err := d.sqlEngine.Catalog().GetTableByName(d.options.dbName, tableName)
	if err != nil {
		return nil, err
	}

	res := &schema.SQLQueryResult{Columns: []*schema.Column{
		{Name: "COLUMN", Type: sql.VarcharType},
		{Name: "TYPE", Type: sql.VarcharType},
		{Name: "INDEX", Type: sql.VarcharType},
	}}

	for _, c := range table.GetColsByID() {
		index := "NO"

		if table.PrimaryKey().Name() == c.Name() {
			index = "PRIMARY KEY"
		}

		indexed, err := table.IsIndexed(c.Name())
		if err != nil {
			return nil, err
		}
		if indexed {
			index = "YES"
		}

		res.Rows = append(res.Rows, &schema.Row{
			Values: []*schema.SQLValue{
				{Value: &schema.SQLValue_S{S: c.Name()}},
				{Value: &schema.SQLValue_S{S: c.Type()}},
				{Value: &schema.SQLValue_S{S: index}},
			},
		})
	}

	return res, nil
}

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

	return d.SQLQueryPrepared(stmt, req.Params)
}

func (d *db) SQLQueryPrepared(stmt *sql.SelectStmt, namedParams []*schema.NamedParam) (*schema.SQLQueryResult, error) {
	if stmt == nil {
		return nil, ErrIllegalArguments
	}

	if stmt.Limit() > MaxKeyScanLimit {
		return nil, ErrMaxKeyScanLimitExceeded
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	params := make(map[string]interface{})

	for _, p := range namedParams {
		params[p.Name] = rawValue(p.Value)
	}

	r, err := d.sqlEngine.QueryPreparedStmt(stmt, params)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	colDescriptors, err := r.Columns()
	if err != nil {
		return nil, err
	}

	cols := make([]*schema.Column, len(colDescriptors))

	for i, c := range colDescriptors {
		cols[i] = &schema.Column{Name: c.Selector, Type: c.Type}
	}

	res := &schema.SQLQueryResult{Columns: cols}

	for l := 0; l < MaxKeyScanLimit; l++ {
		row, err := r.Read()
		if err == sql.ErrNoMoreRows {
			break
		}
		if err != nil {
			return nil, err
		}

		rrow := &schema.Row{
			Values: make([]*schema.SQLValue, len(res.Columns)),
		}

		for i, c := range res.Columns {
			v := row.Values[c.Name]

			_, isNull := v.(*sql.NullValue)
			if isNull {
				rrow.Values[i] = &schema.SQLValue{Value: &schema.SQLValue_Null{}}
			} else {
				rrow.Values[i] = typedValueToRowValue(v)
			}
		}

		res.Rows = append(res.Rows, rrow)
	}

	return res, nil
}

func rawValue(v *schema.SQLValue) interface{} {
	if v == nil {
		return nil
	}

	switch tv := v.Value.(type) {
	case *schema.SQLValue_Null:
		{
			return nil
		}
	case *schema.SQLValue_N:
		{
			return tv.N
		}
	case *schema.SQLValue_S:
		{
			return tv.S
		}
	case *schema.SQLValue_B:
		{
			return tv.B
		}
	case *schema.SQLValue_Bs:
		{
			return tv.Bs
		}
	}
	return nil
}

func typedValueToRowValue(tv sql.TypedValue) *schema.SQLValue {
	switch tv.Type() {
	case sql.IntegerType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: tv.Value().(uint64)}}
		}
	case sql.VarcharType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_S{S: tv.Value().(string)}}
		}
	case sql.BooleanType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_B{B: tv.Value().(bool)}}
		}
	case sql.BLOBType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_Bs{Bs: tv.Value().([]byte)}}
		}
	}
	return nil
}
