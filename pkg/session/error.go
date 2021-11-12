package session

import "github.com/codenotary/immudb/pkg/errors"

var ErrSessionAlreadyPresent = errors.New("session already present")
var ErrNoSessionIDPresent = errors.New("no sessionID provided")
