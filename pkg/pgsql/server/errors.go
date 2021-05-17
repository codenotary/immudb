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
	"errors"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"strings"
)

var ErrUnknowMessageType = errors.New("found an unknown message type on the wire")
var ErrDBNotprovided = errors.New("database name not provided")
var ErrUsernameNotprovided = errors.New("user name not provided")
var ErrPwNotprovided = errors.New("password not provided")
var ErrDBNotExists = errors.New("selected db doesn't exists")
var ErrUsernameNotFound = errors.New("user not found")
var ErrExpectedQueryMessage = errors.New("expected query message")
var ErrUseDBStatementNotSupported = errors.New("SQL statement not supported. Please use `UseDatabase` operation instead")
var ErrCreateDBStatementNotSupported = errors.New("SQL statement not supported. Please use `CreateDatabase` operation instead")
var ErrSSLNotSupported = errors.New("SSL not supported")

func MapPgError(err error) (er bm.ErrorResp) {
	switch {
	case errors.Is(err, ErrDBNotprovided):
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Code(pgmeta.PgServerErrRejectedEstablishmentOfSqlconnection),
			bm.Message(ErrDBNotprovided.Error()),
			bm.Hint("please provide a valid database name or use immuclient to create a new one"),
		)
	case errors.Is(err, ErrDBNotExists):
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Code(pgmeta.PgServerErrRejectedEstablishmentOfSqlconnection),
			bm.Message(ErrDBNotExists.Error()),
			bm.Hint("please provide a valid database name or use immuclient to create a new one"),
		)
	case strings.Contains(err.Error(), "syntax error"):
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Code(pgmeta.PgServerErrSyntaxError),
			bm.Message(err.Error()),
		)
	case strings.Contains(err.Error(), ErrUnknowMessageType.Error()):
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Code(pgmeta.PgServerErrProtocolViolation),
			bm.Message(err.Error()),
			bm.Hint("submitted message is not yet implemented"),
		)
	case errors.Is(err, ErrSSLNotSupported):
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Code(pgmeta.PgServerErrConnectionFailure),
			bm.Message(err.Error()),
			bm.Hint("launch immudb with a certificate and a private key"),
		)
	default:
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Message(err.Error()),
		)
	}
	return er
}
