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

package stream

import (
	"fmt"
	"github.com/codenotary/immudb/pkg/errors"
)

var ErrMaxValueLenExceeded = "internal store max value length exceeded"
var ErrMaxTxValuesLenExceeded = "max transaction values length exceeded"
var ErrNotEnoughDataOnStream = "not enough data to build the expected message. check value length declaration"
var ErrMessageLengthIsZero = "message trailer length is declared equal to zero"
var ErrReaderIsEmpty = "reader contains no data"
var ErrChunkTooSmall = fmt.Sprintf("minimum chunk size is %d", MinChunkSize)
var ErrMissingExpectedData = "expected data on stream is missing"
var ErrRefOptNotImplemented = "reference operation is not implemented"
var ErrUnableToReassembleExecAllMessage = "unable to reassemble ZAdd message on a streamExecAll"

func init() {
	errors.CodeMap[ErrMaxValueLenExceeded] = errors.CodDataException
	errors.CodeMap[ErrMaxTxValuesLenExceeded] = errors.CodDataException
	errors.CodeMap[ErrNotEnoughDataOnStream] = errors.CodInvalidParameterValue
	errors.CodeMap[ErrMessageLengthIsZero] = errors.CodInvalidParameterValue
	errors.CodeMap[ErrReaderIsEmpty] = errors.CodInvalidParameterValue
	errors.CodeMap[ErrMissingExpectedData] = errors.CodInternalError
	errors.CodeMap[ErrRefOptNotImplemented] = errors.CodUndefinedFunction
	errors.CodeMap[ErrUnableToReassembleExecAllMessage] = errors.CodInternalError
}
