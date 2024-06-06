/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package errors

import (
	"errors"
	"strings"

	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
)

var ErrUnknowMessageType = errors.New("found an unknown message type on the wire")
var ErrDBNotprovided = errors.New("database name not provided")
var ErrUsernameNotprovided = errors.New("user name not provided")
var ErrPwNotprovided = errors.New("password not provided")
var ErrDBNotExists = errors.New("selected db doesn't exists")
var ErrInvalidUsernameOrPassword = errors.New("invalid user name or password")
var ErrExpectedQueryMessage = errors.New("expected query message")
var ErrUseDBStatementNotSupported = errors.New("SQL statement not supported")
var ErrSSLNotSupported = errors.New("SSL not supported")
var ErrMaxStmtNumberExceeded = errors.New("maximum number of statements in a single query exceeded")
var ErrMessageCannotBeHandledInternally = errors.New("message cannot be handled internally")
var ErrMaxParamsNumberExceeded = errors.New("number of parameters exceeded the maximum limit")
var ErrParametersValueSizeTooLarge = errors.New("provided parameters exceeded the maximum allowed size limit")
var ErrNegativeParameterValueLen = errors.New("negative parameter length detected")
var ErrMalformedMessage = errors.New("malformed message detected")
var ErrMessageTooLarge = errors.New("payload message hit allowed memory boundaries")

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
	case errors.Is(err, ErrMaxStmtNumberExceeded):
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Code(pgmeta.PgServerErrSyntaxError),
			bm.Message(err.Error()),
			bm.Hint("at the moment is possible to receive only 1 statement. Please split query or use a single statement"),
		)
	case errors.Is(err, ErrParametersValueSizeTooLarge):
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Code(pgmeta.DataException),
			bm.Message(err.Error()),
		)
	case errors.Is(err, ErrNegativeParameterValueLen):
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Code(pgmeta.DataException),
			bm.Message(err.Error()),
		)
	case errors.Is(err, ErrMalformedMessage):
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Code(pgmeta.PgServerErrProtocolViolation),
			bm.Message(err.Error()),
		)
	default:
		er = bm.ErrorResponse(bm.Severity(pgmeta.PgSeverityError),
			bm.Message(err.Error()),
		)
	}
	return er
}
