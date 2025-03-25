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

package database

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrIndexKeyMismatch      = status.New(codes.InvalidArgument, "mismatch between provided index and key").Err()
	ErrNoReferenceProvided   = status.New(codes.InvalidArgument, "provided argument is not a reference").Err()
	ErrReferenceKeyMissing   = status.New(codes.InvalidArgument, "reference key not provided").Err()
	ErrZAddIndexMissing      = status.New(codes.InvalidArgument, "zAdd index not provided").Err()
	ErrReferenceIndexMissing = status.New(codes.InvalidArgument, "reference index not provided").Err()

	ErrDatabaseAlreadyExists      = errors.New("database already exists")
	ErrDatabaseNotExists          = errors.New("database does not exist")
	ErrCannotDeleteAnOpenDatabase = errors.New("cannot delete an open database")
	ErrTxReadPoolExhausted        = errors.New("read tx pool exhausted")
)
