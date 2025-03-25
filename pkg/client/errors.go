/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package client

import (
	"github.com/codenotary/immudb/pkg/client/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Errors related to Client connection and health check
var (

	// ErrIllegalArguments indicates illegal arguments provided to a method
	ErrIllegalArguments = errors.New("illegal arguments")

	// ErrAlreadyConnected is used when trying to establish a new connection with a client that is already connected
	ErrAlreadyConnected = errors.New("already connected")

	// ErrNotConnected is used when the operation can not be done because the client connection is closed
	ErrNotConnected = errors.New("not connected")

	// ErrHealthCheckFailed is used to indicate that health check has failed
	ErrHealthCheckFailed = errors.New("health check failed")

	// ErrServerStateIsOlder is used to inform that the client has newer state than the server.
	// This could happen if the client connects to an asynchronous replica that did not yet
	// replicate all transactions from the primary database.
	ErrServerStateIsOlder = errors.New("server state is older than the client one")

	// ErrSessionAlreadyOpen is used when trying to create a new session but there's a valid session already set up.
	ErrSessionAlreadyOpen = errors.New("session already opened")
)

// Server errors mapping
var (
	ErrSrvIllegalArguments   = status.Error(codes.InvalidArgument, "illegal arguments")
	ErrSrvIllegalState       = status.Error(codes.InvalidArgument, "illegal state")
	ErrSrvEmptyAdminPassword = status.Error(codes.InvalidArgument, "Admin password cannot be empty")
	ErrWriteOnlyTXNotAllowed = status.Error(codes.InvalidArgument, "write only transaction not allowed")
)
