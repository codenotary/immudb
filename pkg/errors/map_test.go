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

package errors

import (
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"testing"
)

func Test_Map(t *testing.T) {
	st := mapGRPcErrorCode(CodSuccessCompletion)
	require.Equal(t, codes.OK, st)
	st = mapGRPcErrorCode(CodSqlclientUnableToEstablishSqlConnection)
	require.Equal(t, codes.PermissionDenied, st)
	st = mapGRPcErrorCode(CodDataException)
	require.Equal(t, codes.FailedPrecondition, st)
	st = mapGRPcErrorCode(CodInvalidParameterValue)
	require.Equal(t, codes.InvalidArgument, st)
	st = mapGRPcErrorCode(CodInternalError)
	require.Equal(t, codes.Internal, st)
	st = mapGRPcErrorCode(CodUndefinedFunction)
	require.Equal(t, codes.Unimplemented, st)
	st = mapGRPcErrorCode(Code("Unknown"))
	require.Equal(t, codes.Unknown, st)
}
