/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package server

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	pserr "github.com/codenotary/immudb/pkg/pgsql/errors"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
)

const helpPrefix = "select relname, nspname, relkind from pg_catalog.pg_class c, pg_catalog.pg_namespace n where relkind in ('r', 'v', 'm', 'f', 'p') and nspname not in ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1') and n.oid = relnamespace order by nspname, relname"
const tableHelpPrefix = "select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, c.relhasrules, c.relkind, c.oid, pg_get_expr(d.adbin, d.adrelid), case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, c.relhasoids, '', c.relhassubclass from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace and c.relname like '"

func (s *session) QueryMachine() error {
	var waitForSync = false

	_, err := s.writeMessage(bm.ReadyForQuery())
	if err != nil {
		return err
	}

	for {
		msg, extQueryMode, err := s.nextMessage()
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.log.Warningf("connection is closed")
				return nil
			}
			s.HandleError(err)
			continue
		}

		// When an error is detected while processing any extended-query message, the backend issues ErrorResponse,
		// then reads and discards messages until a Sync is reached, then issues ReadyForQuery and returns to normal
		// message processing. (But note that no skipping occurs if an error is detected while processing Sync — this
		// ensures that there is one and only one ReadyForQuery sent for each Sync.)
		if waitForSync && extQueryMode {
			if _, ok := msg.(fm.SyncMsg); !ok {
				continue
			}
		}

		switch v := msg.(type) {
		case fm.TerminateMsg:
			return s.mr.CloseConnection()
		case fm.QueryMsg:
			statements := v.GetStatements()

			if statements == helpPrefix {
				statements = "show tables"
			}

			if strings.HasPrefix(statements, tableHelpPrefix) {
				tableName := strings.Split(strings.TrimPrefix(statements, tableHelpPrefix), "'")[0]
				statements = fmt.Sprintf("select column_name as tq, column_name as tow, column_name as tn, column_name as COLUMN_NAME, type_name as DATA_TYPE, type_name as TYPE_NAME, type_name as p, type_name as l, type_name as s, type_name as r, is_nullable as NULLABLE, column_name as rk, column_name as cd, type_name as SQL_DATA_TYPE, type_name as sts, column_name as coll, type_name as orp, is_nullable as IS_NULLABLE, type_name as dz, type_name as ft, type_name as iau, type_name as pn, column_name as toi, column_name as btd, column_name as tmo, column_name as tin from table(%s)", tableName)
			}

			err := s.fetchAndWriteResults(statements, nil, nil, extQueryMode)
			if err != nil {
				waitForSync = extQueryMode
				s.HandleError(err)
			}

			if _, err = s.writeMessage(bm.ReadyForQuery()); err != nil {
				waitForSync = extQueryMode
			}
		case fm.ParseMsg:
			_, ok := s.statements[v.DestPreparedStatementName]
			// unnamed prepared statement overrides previous
			if ok && v.DestPreparedStatementName != "" {
				waitForSync = extQueryMode
				s.HandleError(fmt.Errorf("statement '%s' already present", v.DestPreparedStatementName))
				continue
			}

			var paramCols []*schema.Column
			var resCols []*schema.Column
			var stmt sql.SQLStmt

			if !s.isInBlackList(v.Statements) {
				stmts, err := sql.Parse(strings.NewReader(v.Statements))
				if err != nil {
					waitForSync = extQueryMode
					s.HandleError(err)
					continue
				}

				// Note: as stated in the pgsql spec, the query string contained in a Parse message cannot include more than one SQL statement;
				// else a syntax error is reported. This restriction does not exist in the simple-query protocol, but it does exist
				// in the extended protocol, because allowing prepared statements or portals to contain multiple commands would
				// complicate the protocol unduly.
				if len(stmts) > 1 {
					waitForSync = extQueryMode
					s.HandleError(pserr.ErrMaxStmtNumberExceeded)
					continue
				}
				if paramCols, resCols, err = s.inferParamAndResultCols(stmts[0]); err != nil {
					waitForSync = extQueryMode
					s.HandleError(err)
					continue
				}
			}

			_, err = s.writeMessage(bm.ParseComplete())
			if err != nil {
				waitForSync = extQueryMode
				continue
			}

			newStatement := &statement{
				// if no name is provided empty string marks the unnamed prepared statement
				Name:         v.DestPreparedStatementName,
				Params:       paramCols,
				SQLStatement: v.Statements,
				PreparedStmt: stmt,
				Results:      resCols,
			}

			s.statements[v.DestPreparedStatementName] = newStatement

		case fm.DescribeMsg:
			// The Describe message (statement variant) specifies the name of an existing prepared statement
			// (or an empty string for the unnamed prepared statement). The response is a ParameterDescription
			// message describing the parameters needed by the statement, followed by a RowDescription message
			// describing the rows that will be returned when the statement is eventually executed (or a NoData
			// message if the statement will not return rows). ErrorResponse is issued if there is no such prepared
			// statement. Note that since Bind has not yet been issued, the formats to be used for returned columns
			// are not yet known to the backend; the format code fields in the RowDescription message will be zeroes
			// in this case.
			if v.DescType == "S" {
				st, ok := s.statements[v.Name]
				if !ok {
					waitForSync = extQueryMode
					s.HandleError(fmt.Errorf("statement '%s' not found", v.Name))
					continue
				}

				if _, err = s.writeMessage(bm.ParameterDescription(st.Params)); err != nil {
					waitForSync = extQueryMode
					continue
				}

				if _, err := s.writeMessage(bm.RowDescription(st.Results, nil)); err != nil {
					waitForSync = extQueryMode
					continue
				}
			}
			// The Describe message (portal variant) specifies the name of an existing portal (or an empty string
			// for the unnamed portal). The response is a RowDescription message describing the rows that will be
			// returned by executing the portal; or a NoData message if the portal does not contain a query that
			// will return rows; or ErrorResponse if there is no such portal.
			if v.DescType == "P" {
				portal, ok := s.portals[v.Name]
				if !ok {
					waitForSync = extQueryMode
					s.HandleError(fmt.Errorf("portal '%s' not found", v.Name))
					continue
				}

				if _, err = s.writeMessage(bm.RowDescription(portal.Statement.Results, portal.ResultColumnFormatCodes)); err != nil {
					waitForSync = extQueryMode
					continue
				}
			}
		case fm.SyncMsg:
			waitForSync = false
			s.writeMessage(bm.ReadyForQuery())
		case fm.BindMsg:
			_, ok := s.portals[v.DestPortalName]
			// unnamed portal overrides previous
			if ok && v.DestPortalName != "" {
				waitForSync = extQueryMode
				s.HandleError(fmt.Errorf("portal '%s' already present", v.DestPortalName))
				continue
			}

			st, ok := s.statements[v.PreparedStatementName]
			if !ok {
				waitForSync = extQueryMode
				s.HandleError(fmt.Errorf("statement '%s' not found", v.PreparedStatementName))
				continue
			}

			encodedParams, err := buildNamedParams(st.Params, v.ParamVals)
			if err != nil {
				waitForSync = extQueryMode
				s.HandleError(err)
				continue
			}

			if _, err = s.writeMessage(bm.BindComplete()); err != nil {
				waitForSync = extQueryMode
				continue
			}

			newPortal := &portal{
				Name:                    v.DestPortalName,
				Statement:               st,
				Parameters:              encodedParams,
				ResultColumnFormatCodes: v.ResultColumnFormatCodes,
			}

			s.portals[v.DestPortalName] = newPortal
		case fm.Execute:
			//query execution
			portal, ok := s.portals[v.PortalName]
			if !ok {
				waitForSync = extQueryMode
				s.HandleError(fmt.Errorf("portal '%s' not found", v.PortalName))
				continue
			}

			delete(s.portals, v.PortalName)

			err := s.fetchAndWriteResults(portal.Statement.SQLStatement,
				portal.Parameters,
				portal.ResultColumnFormatCodes,
				extQueryMode,
			)
			if err != nil {
				waitForSync = extQueryMode
				s.HandleError(err)
			}
		case fm.FlushMsg:
			// there is no buffer to be flushed
		default:
			waitForSync = extQueryMode
			s.HandleError(pserr.ErrUnknowMessageType)
		}
	}
}

func (s *session) fetchAndWriteResults(statements string, parameters []*schema.NamedParam, resultColumnFormatCodes []int16, extQueryMode bool) error {
	if s.isInBlackList(statements) {
		_, err := s.writeMessage(bm.CommandComplete([]byte("ok")))
		return err
	}

	if i := s.isEmulableInternally(statements); i != nil {
		if err := s.tryToHandleInternally(i); err != nil && err != pserr.ErrMessageCannotBeHandledInternally {
			return err
		}

		_, err := s.writeMessage(bm.CommandComplete([]byte("ok")))
		return err
	}

	stmts, err := sql.Parse(strings.NewReader(statements))
	if err != nil {
		return err
	}

	for _, stmt := range stmts {
		switch st := stmt.(type) {
		case *sql.UseDatabaseStmt:
			{
				return pserr.ErrUseDBStatementNotSupported
			}
		case *sql.SelectStmt:
			if err = s.query(st, parameters, resultColumnFormatCodes, extQueryMode); err != nil {
				return err
			}
		default:
			if err = s.exec(st, parameters, resultColumnFormatCodes, extQueryMode); err != nil {
				return err
			}
		}
	}

	_, err = s.writeMessage(bm.CommandComplete([]byte("ok")))
	if err != nil {
		return err
	}

	return nil
}

func (s *session) query(st *sql.SelectStmt, parameters []*schema.NamedParam, resultColumnFormatCodes []int16, skipRowDesc bool) error {
	res, err := s.db.SQLQueryPrepared(s.ctx, s.tx, st, parameters)
	if err != nil {
		return err
	}

	if !skipRowDesc {
		if _, err = s.writeMessage(bm.RowDescription(res.Columns, nil)); err != nil {
			return err
		}
	}

	_, err = s.writeMessage(bm.DataRow(res.Rows, len(res.Columns), resultColumnFormatCodes))
	return err
}

func (s *session) exec(st sql.SQLStmt, namedParams []*schema.NamedParam, resultColumnFormatCodes []int16, skipRowDesc bool) error {
	params := make(map[string]interface{}, len(namedParams))

	for _, p := range namedParams {
		params[p.Name] = schema.RawValue(p.Value)
	}

	ntx, _, err := s.db.SQLExecPrepared(s.ctx, s.tx, []sql.SQLStmt{st}, params)
	s.tx = ntx

	return err
}

type portal struct {
	Name                    string
	Statement               *statement
	Parameters              []*schema.NamedParam
	ResultColumnFormatCodes []int16
}

type statement struct {
	Name         string
	SQLStatement string
	PreparedStmt sql.SQLStmt
	Params       []*schema.Column
	Results      []*schema.Column
}

func (s *session) inferParamAndResultCols(stmt sql.SQLStmt) ([]*schema.Column, []*schema.Column, error) {
	resCols := make([]*schema.Column, 0)

	sel, ok := stmt.(*sql.SelectStmt)
	if ok {
		rr, err := s.db.SQLQueryRowReader(s.ctx, s.tx, sel, nil)
		if err != nil {
			return nil, nil, err
		}
		cols, err := rr.Columns(s.ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, c := range cols {
			resCols = append(resCols, &schema.Column{Name: c.Selector(), Type: c.Type})
		}
	}

	r, err := s.db.InferParametersPrepared(s.ctx, s.tx, stmt)
	if err != nil {
		return nil, nil, err
	}

	if len(r) > math.MaxInt16 {
		return nil, nil, pserr.ErrMaxParamsNumberExceeded
	}

	var paramsNameList []string
	for n := range r {
		paramsNameList = append(paramsNameList, n)
	}
	sort.Strings(paramsNameList)

	paramCols := make([]*schema.Column, 0)
	for _, n := range paramsNameList {
		paramCols = append(paramCols, &schema.Column{Name: n, Type: r[n]})
	}

	return paramCols, resCols, nil
}
