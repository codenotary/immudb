/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/codenotary/immudb/pkg/database"
	pserr "github.com/codenotary/immudb/pkg/pgsql/errors"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"strings"
)

// InitializeSession
func (s *session) InitializeSession() (err error) {
	defer func() {
		if err != nil {
			s.ErrorHandle(err)
			s.mr.CloseConnection()
		}
	}()

	lb := make([]byte, 4)
	if _, err := s.mr.Read(lb); err != nil {
		return err
	}
	pvb := make([]byte, 4)
	if _, err := s.mr.Read(pvb); err != nil {
		return err
	}

	s.protocolVersion = parseProtocolVersion(pvb)

	// SSL Request packet
	if s.protocolVersion == "1234.5679" {
		if s.tlsConfig == nil || len(s.tlsConfig.Certificates) == 0 {
			if _, err = s.writeMessage([]byte(`N`)); err != nil {
				return err
			}
			return pserr.ErrSSLNotSupported
		}

		if _, err = s.writeMessage([]byte(`S`)); err != nil {
			return err
		}

		if err = s.handshake(); err != nil {
			return err
		}

		lb = make([]byte, 4)
		if _, err := s.mr.Read(lb); err != nil {
			return err
		}
		pvb = make([]byte, 4)
		if _, err := s.mr.Read(pvb); err != nil {
			return err
		}

		s.protocolVersion = parseProtocolVersion(pvb)
	}

	// startup message
	connStringLenght := int(binary.BigEndian.Uint32(lb) - 4)
	connString := make([]byte, connStringLenght)

	if _, err := s.mr.Read(connString); err != nil {
		return err
	}

	pr := bufio.NewScanner(bytes.NewBuffer(connString))

	split := func(data []byte, atEOF bool) (int, []byte, error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.IndexByte(data, 0); i >= 0 {
			return i + 1, data[0:i], nil
		}
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	}

	pr.Split(split)

	pmap := make(map[string]string)

	for pr.Scan() {
		key := pr.Text()
		for pr.Scan() {
			value := pr.Text()
			if value != "" {
				pmap[key] = value
			}
			break
		}
	}

	s.connParams = pmap

	return nil
}

// HandleStartup errors are returned and handled in the caller
func (s *session) HandleStartup(dbList database.DatabaseList) (err error) {
	defer func() {
		if err != nil {
			s.ErrorHandle(err)
			s.mr.CloseConnection()
		}
	}()

	user, ok := s.connParams["user"]
	if !ok || user == "" {
		return pserr.ErrUsernameNotprovided
	}
	s.username = user
	db, ok := s.connParams["database"]
	if !ok {
		return pserr.ErrDBNotprovided
	}
	s.database, err = dbList.GetByName(db)
	if err != nil {
		if errors.Is(err, database.ErrDatabaseNotExists) {
			return pserr.ErrDBNotExists
		}
		return err
	}
	s.log.Debugf("selected %s database", s.database.GetName())

	if _, err = s.writeMessage(bm.AuthenticationCleartextPassword()); err != nil {
		return err
	}
	msg, _, err := s.nextMessage()
	if err != nil {
		return err
	}
	if pw, ok := msg.(fm.PasswordMsg); ok {
		if !ok || pw.GetSecret() == "" {
			return pserr.ErrPwNotprovided
		}
		usr, err := s.getUser([]byte(s.username))
		if err != nil {
			if strings.Contains(err.Error(), "key not found") {
				return pserr.ErrUsernameNotFound
			}
		}
		if err := usr.ComparePasswords([]byte(pw.GetSecret())); err != nil {
			return err
		}
		s.log.Debugf("authentication successful for %s", s.username)
		if _, err := s.writeMessage(bm.AuthenticationOk()); err != nil {
			return err
		}
	}
	if _, err := s.writeMessage(bm.ParameterStatus([]byte("standard_conforming_strings"), []byte("on"))); err != nil {
		return err
	}
	if _, err := s.writeMessage(bm.ParameterStatus([]byte("client_encoding"), []byte("UTF8"))); err != nil {
		return err
	}
	// todo this is needed by jdbc driver. Here is added the minor supported version at the moment
	if _, err := s.writeMessage(bm.ParameterStatus([]byte("server_version"), []byte(pgmeta.PgsqlProtocolVersion))); err != nil {
		return err
	}

	return nil
}

func parseProtocolVersion(payload []byte) string {
	major := int(binary.BigEndian.Uint16(payload[0:2]))
	minor := int(binary.BigEndian.Uint16(payload[2:4]))
	return fmt.Sprintf("%d.%d", major, minor)
}
