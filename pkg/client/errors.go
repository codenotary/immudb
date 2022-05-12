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

package client

import (
	"github.com/codenotary/immudb/pkg/client/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Errors related to Client connection and health check
var (
	ErrIllegalArguments = errors.New("illegal arguments")

	ErrAlreadyConnected   = errors.New("already connected")
	ErrNotConnected       = errors.New("not connected")
	ErrHealthCheckFailed  = errors.New("health check failed")
	ErrServerStateIsOlder = errors.New("server state is older than the client one")
	ErrSessionAlreadyOpen = errors.New("session already opened")
)

// Server errors mapping
var (
	ErrSrvIllegalArguments   = status.Error(codes.InvalidArgument, "illegal arguments")
	ErrSrvIllegalState       = status.Error(codes.InvalidArgument, "illegal state")
	ErrSrvEmptyAdminPassword = status.Error(codes.InvalidArgument, "Admin password cannot be empty")
	ErrWriteOnlyTXNotAllowed = status.Error(codes.InvalidArgument, "write only transaction not allowed")
)
