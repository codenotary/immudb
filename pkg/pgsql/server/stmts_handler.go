package server

import (
	pserr "github.com/codenotary/immudb/pkg/pgsql/errors"
	"regexp"
)

var set = regexp.MustCompile(`(?i)set\s+.+`)
var selectVersion = regexp.MustCompile(`(?i)select\s+version\(\s*\)`)

func (s *session) isInBlackList(statement string) bool {
	if set.MatchString(statement) {
		return true
	}
	if statement == ";" {
		return true
	}
	return false
}

func (s *session) isEmulableInternally(statement string) interface{} {
	if selectVersion.MatchString(statement) {
		return &version{}
	}
	return nil
}
func (s *session) tryToHandleInternally(command interface{}) error {
	switch command.(type) {
	case *version:
		if err := s.writeVersionInfo(); err != nil {
			return err
		}
	default:
		return pserr.ErrMessageCannotBeHandledInternally
	}
	return nil
}

type version struct{}
