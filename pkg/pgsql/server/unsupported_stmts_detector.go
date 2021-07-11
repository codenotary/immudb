package server

import (
	"regexp"
)

func (s *session) isSupportedByCore(statement string) bool {
	var set = regexp.MustCompile(`(?i)set\s+.+`)
	if set.MatchString(statement) {
		return false
	}
	if statement == ";" {
		return false
	}
	return true
}

func (s *session) isEmulableInternally(statement string) interface{} {
	if regexp.MustCompile(`(?i)select\s+version\(\s*\)`).MatchString(statement) {
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
		return ErrMessageCannotBeHandledInternally
	}
	return nil
}

type version struct{}
