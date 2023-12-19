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

import (
	"os"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/status"
)

// GRPCStatus return the gRPC status from a wrapped error.
func (f *wrappedError) GRPCStatus() *status.Status {
	err, ok := f.cause.(*immuError)
	if !ok {
		return nil
	}
	return setupStatus(err, f.msg, err.retryDelay)
}

func (f *immuError) GRPCStatus() *status.Status {
	return setupStatus(f, f.msg, f.retryDelay)
}

func setupStatus(cause *immuError, message string, retryDelay int32) *status.Status {
	st := status.New(mapGRPcErrorCode(cause.code), message)

	errorInfo := &schema.ErrorInfo{
		Cause: cause.msg,
		Code:  string(cause.code),
	}

	retryInfo := &schema.RetryInfo{RetryDelay: retryDelay}

	details := make([]proto.Message, 0)
	details = append(details, errorInfo, retryInfo)

	if di := debugInfo(cause.stack); di != nil {
		details = append(details, di)
	}
	st, err := st.WithDetails(details...)
	if err != nil {
		return nil
	}
	return st
}

func debugInfo(stack string) (dbg *schema.DebugInfo) {
	if strings.ToLower(os.Getenv("LOG_LEVEL")) == "debug" {
		dbg = &schema.DebugInfo{
			Stack: stack,
		}
	}
	return dbg
}
