package auth

import "github.com/codenotary/immudb/pkg/errors"

var ErrNoAuthData = errors.New("no authentication data provided").WithCode(errors.CodProtocolViolation)
var ErrNotLoggedIn = errors.New("not logged in").WithCode(errors.CodInvalidAuthorizationSpecification)
