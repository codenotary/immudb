/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"sync"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/pgsql/errors"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
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
	portals         map[string]*portal
	statements      map[string]*statement
	sync.Mutex
}

type Session interface {
	InitializeSession() error
	HandleStartup(dbList database.DatabaseList) error
	QueriesMachine(ctx context.Context) (err error)
	ErrorHandle(err error)
}

func NewSession(c net.Conn, log logger.Logger, sysDb database.DB, tlsConfig *tls.Config) *session {
	s := &session{
		tlsConfig:  tlsConfig,
		log:        log,
		mr:         NewMessageReader(c),
		sysDb:      sysDb,
		portals:    make(map[string]*portal),
		statements: make(map[string]*statement),
	}
	return s
}

func (s *session) ErrorHandle(e error) {
	if e != nil {
		er := errors.MapPgError(e)
		_, err := s.writeMessage(er.Encode())
		if err != nil {
			s.log.Errorf("unable to write error on wire: %v", err)
		}
		s.log.Debugf("%s", er.ToString())
		if _, err := s.writeMessage(bm.ReadyForQuery()); err != nil {
			s.log.Errorf("unable to complete error handling: %v", err)
		}
	}
}

func (s *session) nextMessage() (interface{}, bool, error) {
	msg, err := s.mr.ReadRawMessage()
	if err != nil {
		return nil, false, err
	}
	s.log.Debugf("received %s - %s message", string(msg.t), pgmeta.MTypes[msg.t])
	extQueryMode := false
	i, err := s.parseRawMessage(msg)
	if msg.t == 'P' ||
		msg.t == 'B' ||
		msg.t == 'D' ||
		msg.t == 'E' ||
		msg.t == 'H' {
		extQueryMode = true
	}
	return i, extQueryMode, err
}

func (s *session) parseRawMessage(msg *rawMessage) (interface{}, error) {
	switch msg.t {
	case 'p':
		return fm.ParsePasswordMsg(msg.payload)
	case 'Q':
		return fm.ParseQueryMsg(msg.payload)
	case 'X':
		return fm.ParseTerminateMsg(msg.payload)
	case 'P':
		return fm.ParseParseMsg(msg.payload)
	case 'B':
		return fm.ParseBindMsg(msg.payload)
	case 'D':
		return fm.ParseDescribeMsg(msg.payload)
	case 'S':
		return fm.ParseSyncMsg(msg.payload)
	case 'E':
		return fm.ParseExecuteMsg(msg.payload)
	case 'H':
		return fm.ParseFlushMsg(msg.payload)
	default:
		return nil, errors.ErrUnknowMessageType
	}
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

	item, err := s.sysDb.Get(context.Background(), &schema.KeyRequest{Key: key})
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
