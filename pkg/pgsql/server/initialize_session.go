package server

import (
	"errors"
	"github.com/codenotary/immudb/pkg/database"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
	"strings"
)

func (s *session) InitializeSession() (err error) {
	defer func() {
		if err != nil {
			s.ErrorHandle(err)
			s.conn.Close()
		}
	}()
	msg, err := s.mr.ReadStartUpMessage()
	if err != nil {
		return err
	}
	s.log.Debugf("startup message \n%s", msg.ToString())
	s.connParams = msg.payload
	return nil
}

// HandleStartup errors are returned and handled in the caller
func (s *session) HandleStartup(dbList database.DatabaseList) (err error) {
	defer func() {
		if err != nil {
			s.ErrorHandle(err)
			s.conn.Close()
		}
	}()

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
	s.log.Debugf("selected %s database", s.database.GetName())

	if _, err = s.writeMessage(bm.AuthenticationCleartextPassword()); err != nil {
		return err
	}
	msg, err := s.nextMessage()
	if err != nil {
		return err
	}
	if pw, ok := msg.(fm.PasswordMsg); ok {
		if !ok || pw.GetSecret() == "" {
			return ErrPwNotprovided
		}
		usr, err := s.getUser([]byte(s.username))
		if err != nil {
			if strings.Contains(err.Error(), "key not found") {
				return ErrUsernameNotFound
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

	return nil
}
