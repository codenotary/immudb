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
	"crypto/tls"
	"encoding/json"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"net"
	"sync"
)

type session struct {
	tlsConfig       *tls.Config
	log             logger.Logger
	mr              MessageReader
	username        string
	database        database.DB
	sysDb           database.DB
	connParams      map[string]string
	protocolVersion string
	sync.Mutex
}

type Session interface {
	InitializeSession() error
	HandleStartup(dbList database.DatabaseList) error
	HandleSimpleQueries() error
	ErrorHandle(err error)
}

func NewSession(c net.Conn, log logger.Logger, sysDb database.DB, tlsConfig *tls.Config) *session {
	s := &session{
		tlsConfig: tlsConfig,
		log:       log,
		mr:        NewMessageReader(c),
		sysDb:     sysDb,
	}
	return s
}

func (s *session) ErrorHandle(e error) {
	if e != nil {
		er := MapPgError(e)
		_, err := s.writeMessage(er.Encode())
		if err != nil {
			s.log.Errorf("unable to write error on wire %v", err)
		}
		s.log.Debugf("%s", er.ToString())
	}
}

func (s *session) nextMessage() (interface{}, error) {
	msg, err := s.mr.ReadRawMessage()
	if err != nil {
		return nil, err
	}
	s.log.Debugf("received %s - %s message", string(msg.t), pgmeta.MTypes[msg.t])
	return s.parseRawMessage(msg), nil
}

func (s *session) parseRawMessage(msg *rawMessage) interface{} {
	switch msg.t {
	case 'p':
		return fm.ParsePasswordMsg(msg.payload)
	case 'Q':
		return fm.ParseQueryMsg(msg.payload)
	case 'X':
		return fm.ParseTerminateMsg(msg.payload)
	}
	return nil
}

func (s *session) writeMessage(msg []byte) (int, error) {
	s.debugMessage(msg)
	return s.mr.Write(msg)
}

func (s *session) debugMessage(msg []byte) {
	if s.log != nil && len(msg) > 0 {
		s.log.Debugf("write %s - %s message", string(msg[0]), pgmeta.MTypes[msg[0]])
	}
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
