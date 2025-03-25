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

package stream

import (
	"fmt"

	"github.com/codenotary/immudb/pkg/errors"
)

var ErrMaxValueLenExceeded = "internal store max value length exceeded"
var ErrMaxTxValuesLenExceeded = "max transaction values length exceeded"
var ErrChunkTooSmall = fmt.Sprintf("minimum chunk size is %d", MinChunkSize)
var ErrRefOptNotImplemented = "reference operation is not implemented"
var ErrUnableToReassembleExecAllMessage = "unable to reassemble ZAdd message on a streamExecAll"

func init() {
	errors.CodeMap[ErrMaxValueLenExceeded] = errors.CodDataException
	errors.CodeMap[ErrMaxTxValuesLenExceeded] = errors.CodDataException
	errors.CodeMap[ErrRefOptNotImplemented] = errors.CodUndefinedFunction
	errors.CodeMap[ErrUnableToReassembleExecAllMessage] = errors.CodInternalError
}
