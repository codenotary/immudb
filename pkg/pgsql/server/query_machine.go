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

package server

import (
	"encoding/binary"
	"errors"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"io"
	"regexp"
	"strconv"
	"strings"
)

func (s *session) QueryMachine() (err error) {
	s.Lock()
	defer s.Unlock()

	var portals = make(map[string]*portal)
	var statements = make(map[string]*statement)

	var waitForSync = false

	if _, err := s.writeMessage(bm.ReadyForQuery()); err != nil {
		return err
	}

	for {
		msg, extQueryMode, err := s.nextMessage()
		if err != nil {
			if err == io.EOF {
				s.log.Warningf("connection is closed")
				return nil
			}
			s.ErrorHandle(err)
			if extQueryMode {
				waitForSync = true
			}
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
			waitForSync = false
			extQueryMode = false
		}

		switch v := msg.(type) {
		case fm.TerminateMsg:
			return s.mr.CloseConnection()
		case fm.QueryMsg:
			var set = regexp.MustCompile(`(?i)set\s+.+`)
			if set.MatchString(v.GetStatements()) {
				if _, err := s.writeMessage(bm.CommandComplete([]byte(`ok`))); err != nil {
					s.ErrorHandle(err)
				}
				if _, err := s.writeMessage(bm.ReadyForQuery()); err != nil {
					s.ErrorHandle(err)
					continue
				}
				continue
			}
			var version = regexp.MustCompile(`(?i)select\s+version\(\s*\)`)
			if version.MatchString(v.GetStatements()) {
				if err = s.writeVersionInfo(); err != nil {
					s.ErrorHandle(err)
					continue
				}
				if _, err := s.writeMessage(bm.ReadyForQuery()); err != nil {
					s.ErrorHandle(err)
					continue
				}
				continue
			}
			// todo handle the result outside in order to avoid err suppression
			if _, err = s.queryMsg(v.GetStatements()); err != nil {
				s.ErrorHandle(err)
				continue
			}
			if _, err := s.writeMessage(bm.CommandComplete([]byte(`ok`))); err != nil {
				s.ErrorHandle(err)
				continue
			}
			if _, err := s.writeMessage(bm.ReadyForQuery()); err != nil {
				s.ErrorHandle(err)
				continue
			}
		case fm.ParseMsg:
			var set = regexp.MustCompile(`(?i)set\s+.+`)
			var cols []*schema.Column
			var parameters []string
			if !set.MatchString(v.Statements) {
				r, err := s.database.QueryStmt(v.Statements, nil, true)
				if err != nil {
					s.ErrorHandle(err)
					waitForSync = true
					continue
				}

				/**TEMP**/
				columns, err := r.Columns()
				if err != nil {
					return err
				}
				cols = make([]*schema.Column, len(columns))

				for i, c := range columns {
					cols[i] = &schema.Column{Name: c.Selector, Type: c.Type}
				}
				/**TEMP**/
				_, ok := statements[v.DestPreparedStatementName]
				// unnamed prepared statement overrides previous
				if ok && v.DestPreparedStatementName != "" {
					return errors.New("statement already present")
				}

				parameters = []string{"INTEGER", "INTEGER", "VARCHAR"}
			}

			newStatement := &statement{
				// if no name is provided empty string marks the unnamed prepared statement
				Name:       v.DestPreparedStatementName,
				Columns:    cols,
				Statements: v.Statements,
				ParamsType: parameters,
			}

			statements[v.DestPreparedStatementName] = newStatement

			if _, err = s.writeMessage(bm.ParseComplete()); err != nil {
				s.ErrorHandle(err)
				waitForSync = true
				continue
			}
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

				st, ok := statements[v.Name]
				if !ok {
					return errors.New("not match")
				}
				cols := st.Columns
				if _, err = s.writeMessage(bm.ParameterDescriptiom(len(st.ParamsType))); err != nil {
					s.ErrorHandle(err)
					waitForSync = true
					continue
				}
				if _, err := s.writeMessage(bm.RowDescription(cols)); err != nil {
					s.ErrorHandle(err)
					waitForSync = true
					continue
				}
			}
			// The Describe message (portal variant) specifies the name of an existing portal (or an empty string
			// for the unnamed portal). The response is a RowDescription message describing the rows that will be
			// returned by executing the portal; or a NoData message if the portal does not contain a query that
			// will return rows; or ErrorResponse if there is no such portal.
			if v.DescType == "P" {
				p, ok := portals[v.Name]
				if !ok {
					return errors.New("not match")
				}
				cols := p.Statement.Columns
				if _, err := s.writeMessage(bm.RowDescription(cols)); err != nil {
					s.ErrorHandle(err)
					waitForSync = true
					continue
				}
			}

		// sync
		case fm.SyncMsg:
			if _, err := s.writeMessage(bm.ReadyForQuery()); err != nil {
				s.ErrorHandle(err)
			}
		case fm.BindMsg:
			_, ok := portals[v.DestPortalName]
			// unnamed portal overrides previous
			if ok && v.DestPortalName != "" {
				return errors.New("portal already present")
			}

			/** TEMP **/
			pMap := make(map[string]interface{})

			if len(v.Parameters) > 0 {
				params := v.Parameters
				if p, ok := params[0].(string); ok {
					switch statements[v.PreparedStatementName].ParamsType[0] {
					case "INTEGER":
						int, err := strconv.Atoi(p)
						if err != nil {
							return err
						}
						pMap["total"] = int64(int)
					case "VARCHAR":
						pMap["total"] = p
					}
				}
				if p, ok := params[0].([]byte); ok {
					switch statements[v.PreparedStatementName].ParamsType[0] {
					case "INTEGER":
						pMap["total"] = int64(binary.BigEndian.Uint64(p))
					case "VARCHAR":
						pMap["total"] = string(p)
					}
				}
				if p, ok := params[1].(string); ok {
					switch statements[v.PreparedStatementName].ParamsType[1] {
					case "INTEGER":
						int, err := strconv.Atoi(p)
						if err != nil {
							return err
						}
						pMap["amount"] = int64(int)
					case "VARCHAR":
						pMap["amount"] = p
					}
				}
				if p, ok := params[1].([]byte); ok {
					switch statements[v.PreparedStatementName].ParamsType[1] {
					case "INTEGER":
						pMap["amount"] = int64(binary.BigEndian.Uint64(p))
					case "VARCHAR":
						pMap["amount"] = string(p)
					}
				}
				if p, ok := params[2].(string); ok {
					switch statements[v.PreparedStatementName].ParamsType[2] {
					case "INTEGER":
						int, err := strconv.Atoi(p)
						if err != nil {
							return err
						}
						pMap["title"] = int
					case "VARCHAR":
						pMap["title"] = p
					}
				}
				if p, ok := params[2].([]byte); ok {
					switch statements[v.PreparedStatementName].ParamsType[2] {
					case "INTEGER":
						pMap["title"] = int64(binary.BigEndian.Uint64(p))
					case "VARCHAR":
						pMap["title"] = string(p)
					}
				}

			}

			encodedParams, err := encodeParams(pMap)
			if err != nil {
				return err
			}

			st, ok := statements[v.PreparedStatementName]
			newPortal := &portal{
				Name:                    v.DestPortalName,
				Statement:               st,
				Parameters:              encodedParams,
				ResultColumnFormatCodes: v.ResultColumnFormatCodes,
			}
			portals[v.DestPortalName] = newPortal

			if _, err := s.writeMessage(bm.BindComplete()); err != nil {
				s.ErrorHandle(err)
				waitForSync = true
				continue
			}
		case fm.Execute:
			var set = regexp.MustCompile(`(?i)set\s+.+`)
			if set.MatchString(portals[v.PortalName].Statement.Statements) {
				if _, err = s.writeMessage(bm.EmptyQueryResponse()); err != nil {
					s.ErrorHandle(err)
					waitForSync = true
					continue
				}
				continue
			}
			//query execution
			stmts, err := sql.Parse(strings.NewReader(portals[v.PortalName].Statement.Statements))
			if err != nil {
				return err
			}

			for _, stmt := range stmts {
				switch st := stmt.(type) {
				case *sql.SelectStmt:
					res, err := s.database.SQLQueryPrepared(st, portals[v.PortalName].Parameters, true)
					if err != nil {
						return err
					}
					if res != nil && len(res.Rows) > 0 {
						if _, err = s.writeMessage(bm.DataRow(res.Rows, len(res.Columns), portals[v.PortalName].ResultColumnFormatCodes)); err != nil {
							return err
						}
						break
					}
					if _, err = s.writeMessage(bm.EmptyQueryResponse()); err != nil {
						return err
					}
				}
			}
			if _, err := s.writeMessage(bm.CommandComplete([]byte(`ok`))); err != nil {
				s.ErrorHandle(err)
				waitForSync = true
				continue
			}
		default:
			s.ErrorHandle(ErrUnknowMessageType)
			continue
		}
	}
}

func (s *session) queryMsg(statements string) (*schema.SQLExecResult, error) {
	var res *schema.SQLExecResult
	stmts, err := sql.Parse(strings.NewReader(statements))
	if err != nil {
		return nil, err
	}
	for _, stmt := range stmts {
		switch st := stmt.(type) {
		case *sql.UseDatabaseStmt:
			{
				return nil, ErrUseDBStatementNotSupported
			}
		case *sql.CreateDatabaseStmt:
			{
				return nil, ErrCreateDBStatementNotSupported
			}
		case *sql.SelectStmt:
			err := s.selectStatement(st)
			if err != nil {
				return nil, err
			}
		case sql.SQLStmt:
			res, err = s.database.SQLExecPrepared([]sql.SQLStmt{st}, nil, true)
			if err != nil {
				return nil, err
			}
		}
	}
	return res, nil
}

func (s *session) selectStatement(st *sql.SelectStmt) error {
	res, err := s.database.SQLQueryPrepared(st, nil, true)
	if err != nil {
		return err
	}
	if res != nil && len(res.Rows) > 0 {
		if _, err = s.writeMessage(bm.RowDescription(res.Columns)); err != nil {
			return err
		}
		if _, err = s.writeMessage(bm.DataRow(res.Rows, len(res.Columns), nil)); err != nil {
			return err
		}
		return nil
	}
	if _, err = s.writeMessage(bm.EmptyQueryResponse()); err != nil {
		return err
	}
	return nil
}

func (s *session) writeVersionInfo() error {
	cols := []*schema.Column{{Name: "version", Type: "VARCHAR"}}
	if _, err := s.writeMessage(bm.RowDescription(cols)); err != nil {
		return err
	}
	rows := []*schema.Row{{
		Columns: []string{"version"},
		Values:  []*schema.SQLValue{{Value: &schema.SQLValue_S{S: pgmeta.PgsqlProtocolVersionMessage}}},
	}}
	if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
		return err
	}
	if _, err := s.writeMessage(bm.CommandComplete([]byte(`ok`))); err != nil {
		return err
	}
	if _, err := s.writeMessage(bm.ReadyForQuery()); err != nil {
		return err
	}
	return nil
}

func encodeParams(params map[string]interface{}) ([]*schema.NamedParam, error) {
	if params == nil {
		return nil, nil
	}

	namedParams := make([]*schema.NamedParam, len(params))

	i := 0
	for n, v := range params {
		sqlVal, err := asSQLValue(v)
		if err != nil {
			return nil, err
		}

		namedParams[i] = &schema.NamedParam{Name: n, Value: sqlVal}
		i++
	}

	return namedParams, nil
}

func asSQLValue(v interface{}) (*schema.SQLValue, error) {
	if v == nil {
		return &schema.SQLValue{Value: &schema.SQLValue_Null{}}, nil
	}

	switch tv := v.(type) {
	case uint:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case int:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case int64:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case uint64:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: uint64(tv)}}, nil
		}
	case string:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_S{S: tv}}, nil
		}
	case bool:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_B{B: tv}}, nil
		}
	case []byte:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_Bs{Bs: tv}}, nil
		}
	}

	return nil, sql.ErrInvalidValue
}

type portal struct {
	Name                    string
	Statement               *statement
	Parameters              []*schema.NamedParam
	ResultColumnFormatCodes []int16
}

type statement struct {
	Name       string
	Statements string
	Columns    []*schema.Column
	ParamsType []string
}
