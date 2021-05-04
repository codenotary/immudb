package server

import (
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
	"io"
	"strings"
)

// HandleSimpleQueries errors are returned and handled in the caller
func (s *session) HandleSimpleQueries() (err error) {
	for true {
		if _, err := s.writeMessage(bm.ReadyForQuery()); err != nil {
			return err
		}
		msg, err := s.nextMessage()
		if err != nil {
			if err == io.EOF {
				s.log.Warningf("connection is closed")
				return nil
			}
			s.ErrorHandle(err)
			continue
		}

		switch v := msg.(type) {
		case fm.TerminateMsg:
			s.conn.Close()
			return nil
		case fm.QueryMsg:
			stmts, err := sql.Parse(strings.NewReader(v.GetStatements()))
			if err != nil {
				s.ErrorHandle(err)
				continue
			}
			sqlQuery := false
			for _, stmt := range stmts {
				switch stmt.(type) {
				case *sql.UseDatabaseStmt:
					{
						return ErrUseDBStatementNotSupported
					}
				case *sql.CreateDatabaseStmt:
					{
						return ErrCreateDBStatementNotSupported
					}
				case *sql.SelectStmt:
					sqlQuery = true
				}
			}

			if sqlQuery {
				r := &schema.SQLQueryRequest{
					Sql: v.GetStatements(),
				}
				res, err := s.database.SQLQuery(r)
				if err != nil {
					s.ErrorHandle(err)
					continue
				}
				if _, err := s.writeMessage(bm.RowDescription(res.Columns)); err != nil {
					s.ErrorHandle(err)
					continue
				}
				if _, err := s.writeMessage(bm.DataRow(res.Rows, len(res.Columns), false)); err != nil {
					s.ErrorHandle(err)
					continue
				}
			} else {
				r := &schema.SQLExecRequest{
					Sql: v.GetStatements(),
				}
				_, err = s.database.SQLExec(r)
				if err != nil {
					s.ErrorHandle(err)
					continue
				}
			}
			break
		default:
			s.ErrorHandle(ErrUnknowMessageType)
			continue
		}
		if _, err := s.writeMessage(bm.CommandComplete()); err != nil {
			s.ErrorHandle(err)
			continue
		}
	}

	return nil
}
