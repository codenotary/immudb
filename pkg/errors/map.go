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

package errors

import (
	"google.golang.org/grpc/codes"
)

func mapGRPcErrorCode(code Code) codes.Code {
	switch code {
	case CodSuccessCompletion:
		return codes.OK
	case CodSqlclientUnableToEstablishSqlConnection:
		return codes.PermissionDenied
	case CodDataException:
		return codes.FailedPrecondition
	case CodInvalidParameterValue:
		return codes.InvalidArgument
	case CodInternalError:
		return codes.Internal
	case CodUndefinedFunction:
		return codes.Unimplemented
	case CodInvalidDatabaseName:
		return codes.NotFound
	case CodIntegrityConstraintViolation:
		return codes.FailedPrecondition
	default:
		return codes.Unknown
	}
}
