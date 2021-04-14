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
	"github.com/codenotary/immudb/pkg/logger"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
	"net"
)

type session struct {
	conn net.Conn
	log  logger.Logger
	mr   MessageReader
}

type Session interface {
	HandleStartup() error
	HandleSimpleQueries() error
}

func NewSession(c net.Conn, log logger.Logger) *session {
	return &session{conn: c, log: log, mr: NewMessageReader(c)}
}

func (s *session) HandleStartup() error {
	connString, err := s.mr.ReadStartUpMessage()
	if err != nil {
		println(connString)
	}

	if _, err := s.mr.WriteMessage(bm.AuthenticationCleartextPassword); err != nil {
		return err
	}
	msg, err := s.NextMessage()
	if err != nil {
		return err
	}
	if pw, ok := msg.(fm.PasswordMsg); ok {
		if pw.GetSecret() == "pass" {
			if _, err := s.mr.WriteMessage(bm.AuthenticationOk); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *session) HandleSimpleQueries() error {
	for true {
		if _, err := s.mr.WriteMessage(bm.ReadyForQuery); err != nil {
			return err
		}
		msg, err := s.NextMessage()
		if err != nil {
			return err
		}
		query, ok := msg.(fm.QueryMsg)
		if !ok {
			s.log.Errorf("not a query")
		}
		println(query.GetStatements())

		if _, err := s.mr.WriteMessage(bm.RowDescription); err != nil {
			return err
		}
		if _, err := s.mr.WriteMessage(bm.DataRow); err != nil {
			return err
		}
		if _, err := s.mr.WriteMessage(bm.CommandComplete); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) NextMessage() (interface{}, error) {
	msg, err := s.mr.ReadRawMessage()
	if err != nil {
		return nil, err
	}
	return s.parseRawMessage(msg), nil
}

func (s *session) parseRawMessage(msg *rawMessage) interface{} {
	switch msg.t {
	case 'p':
		return fm.ParsePasswordMsg(msg.payload)
	case 'Q':
		return fm.ParseQueryMsg(msg.payload)
	}
	return nil
}
