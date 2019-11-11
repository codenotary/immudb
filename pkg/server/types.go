package server

import (
	"github.com/codenotary/immudb/pkg/db"
	"github.com/codenotary/immudb/pkg/logger"
)

type ImmuServer struct {
	Topic  *db.Topic
	Logger logger.Logger
}

func (s *ImmuServer) Errorf(f string, v ...interface{}) {
	if s.Logger == nil {
		return
	}
	s.Logger.Errorf(f, v...)
}

func (s *ImmuServer) Warningf(f string, v ...interface{}) {
	s.Logger.Warningf(f, v...)
}

func (s *ImmuServer) Infof(f string, v ...interface{}) {
	s.Logger.Infof(f, v...)
}

func (s *ImmuServer) Debugf(f string, v ...interface{}) {
	s.Logger.Debugf(f, v...)
}
