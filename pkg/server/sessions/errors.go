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

package sessions

import (
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/codenotary/immudb/pkg/server/sessions/internal/transactions"
)

var ErrSessionAlreadyPresent = errors.New("session already present").WithCode(errors.CodInternalError)
var ErrNoSessionIDPresent = errors.New("no sessionID provided").WithCode(errors.CodInvalidAuthorizationSpecification)
var ErrNoSessionAuthDataProvided = errors.New("no session auth data provided").WithCode(errors.CodInvalidAuthorizationSpecification)
var ErrSessionNotFound = errors.New("no session found").WithCode(errors.CodInvalidParameterValue)
var ErrOngoingReadWriteTx = errors.New("only 1 read write transaction supported at once").WithCode(errors.CodSqlserverRejectedEstablishmentOfSqlSession)
var ErrNoTransactionIDPresent = errors.New("no transactionID provided").WithCode(errors.CodInvalidAuthorizationSpecification)
var ErrNoTransactionAuthDataProvided = errors.New("no transaction auth data provided").WithCode(errors.CodInvalidAuthorizationSpecification)
var ErrInvalidOptionsProvided = errors.New("invalid options provided")
var ErrTransactionNotFound = transactions.ErrTransactionNotFound
var ErrGuardAlreadyRunning = errors.New("session guard already launched")
var ErrGuardNotRunning = errors.New("session guard not running")
var ErrMaxSessionsReached = errors.New("max sessions reached")
var ErrWriteOnlyTXNotAllowed = errors.New("write only transaction not allowed")
var ErrReadOnlyTXNotAllowed = errors.New("read only transaction not allowed")
