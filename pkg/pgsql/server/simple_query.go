package server

import (
	"github.com/codenotary/immudb/embedded/sql"
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
			// @todo add terminate message
			return s.conn.Close()
		case fm.QueryMsg:
			// @todo remove when this will be supported
			if strings.Contains(v.GetStatements(), "SET") {
				continue
			}
			if err = s.queryMsg(v); err != nil {
				s.ErrorHandle(err)
				continue
			}
		default:
			s.ErrorHandle(ErrUnknowMessageType)
			continue
		}
		if _, err := s.writeMessage(bm.CommandComplete([]byte(`Ok`))); err != nil {
			s.ErrorHandle(err)
			continue
		}
	}

	return nil
}

func (s *session) queryMsg(v fm.QueryMsg) error {
	stmts, err := sql.Parse(strings.NewReader(v.GetStatements()))
	if err != nil {
		return err
	}
	for _, stmt := range stmts {
		switch st := stmt.(type) {
		case *sql.UseDatabaseStmt:
			{
				return ErrUseDBStatementNotSupported
			}
		case *sql.CreateDatabaseStmt:
			{
				return ErrCreateDBStatementNotSupported
			}
		case *sql.SelectStmt:
			err := s.selectStatement(st)
			if err != nil {
				return err
			}
		case sql.SQLStmt:
			_, err = s.database.SQLExecPrepared([]sql.SQLStmt{st}, nil, true)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *session) selectStatement(st *sql.SelectStmt) error {
	res, err := s.database.SQLQueryPrepared(st, nil)
	if err != nil {
		return err
	}
	if res != nil && len(res.Rows) > 0 {
		if _, err = s.writeMessage(bm.RowDescription(res.Columns)); err != nil {
			return err
		}
		if _, err = s.writeMessage(bm.DataRow(res.Rows, len(res.Columns), false)); err != nil {
			return err
		}
		return nil
	}
	if _, err = s.writeMessage(bm.EmptyQueryResponse()); err != nil {
		return err
	}
	return nil
}
