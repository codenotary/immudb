package server

func (s *ImmuServer) Errorf(f string, v ...interface{}) {
	if s.Logger != nil {
		s.Logger.Errorf(f, v...)
	}
}

func (s *ImmuServer) Warningf(f string, v ...interface{}) {
	if s.Logger != nil {
		s.Logger.Warningf(f, v...)
	}
}

func (s *ImmuServer) Infof(f string, v ...interface{}) {
	if s.Logger == nil {
		s.Logger.Infof(f, v...)
	}
}

func (s *ImmuServer) Debugf(f string, v ...interface{}) {
	if s.Logger == nil {
		s.Logger.Debugf(f, v...)
	}
}
