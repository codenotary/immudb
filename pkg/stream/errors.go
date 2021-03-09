/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrMaxValueLenExceeded = status.Error(codes.FailedPrecondition, "internal store max value length exceeded")
var ErrMaxTxValuesLenExceeded = status.Error(codes.FailedPrecondition, "max transaction values length exceeded")
var ErrNotEnoughDataOnStream = status.Error(codes.InvalidArgument, "not enough data to build the expected message. check value length declaration")
var ErrMessageLengthIsZero = status.Error(codes.InvalidArgument, "message trailer length is declared equal to zero")
var ErrReaderIsEmpty = status.Error(codes.InvalidArgument, "reader contains no data")
var ErrChunkTooSmall = status.Error(codes.InvalidArgument, fmt.Sprintf("minimum chunk size is %d", MinChunkSize))
var ErrMissingExpectedData = status.Error(codes.Internal, fmt.Sprintf("expected data on stream is missing"))
var ErrRefOptNotImplemented = status.Error(codes.Unimplemented, fmt.Sprintf("reference operation is not implemented"))
var ErrUnableToReassembleExecAllMessage = status.Error(codes.Internal, fmt.Sprintf("unable to reassemble ZAdd message on a streamExecAll"))
