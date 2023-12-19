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

package schema

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrEmptySet                         = status.New(codes.InvalidArgument, "empty set").Err()
	ErrDuplicatedKeysNotSupported       = status.New(codes.InvalidArgument, "duplicated keys are not supported in single batch transaction").Err()
	ErrDuplicatedZAddNotSupported       = status.New(codes.InvalidArgument, "duplicated index inside zAdd insertions are not supported in single batch transaction").Err()
	ErrDuplicatedReferencesNotSupported = status.New(codes.InvalidArgument, "duplicated references insertions are not supported in single batch transaction").Err()
)
