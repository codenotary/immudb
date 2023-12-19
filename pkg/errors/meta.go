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

type Code string

const (
	CodSuccessCompletion                             Code = "00000"
	CodInternalError                                 Code = "XX000"
	CodSqlclientUnableToEstablishSqlConnection       Code = "08001"
	CodSqlserverRejectedEstablishmentOfSqlconnection Code = "08004"
	CodProtocolViolation                             Code = "08P01"
	CodDataException                                 Code = "22000"
	CodInvalidParameterValue                         Code = "22023"
	CodUndefinedFunction                             Code = "42883"
	CodInvalidDatabaseName                           Code = "3F000"
	CodInvalidAuthorizationSpecification             Code = "28000"
	CodSqlserverRejectedEstablishmentOfSqlSession    Code = "08001"
	CodInvalidTransactionInitiation                  Code = "0B000"
	CodInFailedSqlTransaction                        Code = "25P02"
	CodIntegrityConstraintViolation                  Code = "23000"
)

var (
	CodeMap = make(map[string]Code)
)
