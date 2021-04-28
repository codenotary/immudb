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
	"encoding/json"
	"errors"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
	"net"
	"strings"
)

type session struct {
	conn       net.Conn
	log        logger.Logger
	mr         MessageReader
	username   string
	database   database.DB
	sysDb      database.DB
	connParams map[string]string
}

type Session interface {
	InitializeSession(dbList database.DatabaseList, sysDb database.DB) error
	HandleStartup() error
	HandleSimpleQueries() error
}

func NewSession(c net.Conn, log logger.Logger) *session {
	s := &session{conn: c, log: log, mr: NewMessageReader(c)}
	return s
}

func (s *session) InitializeSession(dbList database.DatabaseList, sysDb database.DB) error {
	msg, err := s.mr.ReadStartUpMessage()
	if err != nil {
		return err
	}
	s.connParams = msg.payload

	user, ok := s.connParams["user"]
	if !ok || user == "" {
		return ErrUsernameNotprovided
	}
	s.username = user
	db, ok := s.connParams["database"]
	if !ok {
		return ErrDBNotprovided
	}
	s.database, err = dbList.GetByName(db)
	if err != nil {
		if errors.Is(err, database.ErrDatabaseNotExists) {
			return ErrDBNotExists
		}
		return err
	}
	s.sysDb = sysDb
	return nil
}

func (s *session) HandleStartup() error {
	if _, err := s.mr.WriteMessage(bm.AuthenticationCleartextPassword); err != nil {
		return err
	}
	msg, err := s.NextMessage()
	if err != nil {
		return err
	}
	if pw, ok := msg.(fm.PasswordMsg); ok {
		if !ok || pw.GetSecret() == "" {
			return ErrPwNotprovided
		}
		usr, err := s.getUser([]byte(s.username))
		if err != nil {
			if !strings.Contains(err.Error(), "key not found") {
				return ErrUsernameNotFound
			}
		}
		if err := usr.ComparePasswords([]byte(pw.GetSecret())); err != nil {
			return err
		}
		if _, err := s.mr.WriteMessage(bm.AuthenticationOk); err != nil {
			return err
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

func (s *session) getUser(username []byte) (*auth.User, error) {
	key := make([]byte, 1+len(username))
	// todo put KeyPrefixUser in a common package
	key[0] = 1
	copy(key[1:], username)

	item, err := s.sysDb.Get(&schema.KeyRequest{Key: key})
	if err != nil {
		return nil, err
	}

	var usr auth.User

	err = json.Unmarshal(item.Value, &usr)
	if err != nil {
		return nil, err
	}

	return &usr, nil
}
