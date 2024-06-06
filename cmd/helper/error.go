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

package helper

import (
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var osexit = os.Exit

// QuitToStdErr prints an error on stderr and closes
func QuitToStdErr(msg interface{}) {
	_, _ = fmt.Fprintln(os.Stderr, msg)
	osexit(1)
}

// QuitWithUserError ...
func QuitWithUserError(err error) {
	s, ok := status.FromError(err)
	if !ok {
		QuitToStdErr(err)
	}
	if s.Code() == codes.Unauthenticated {
		QuitToStdErr(errors.New("unauthenticated, please login"))
	}
	QuitToStdErr(err)
}

func OverrideQuitter(quitter func(int)) {
	osexit = quitter
}

func UnwrapMessage(msg interface{}) interface{} {
	if err, ok := msg.(error); ok {
		if statusErr, isStatusErr := status.FromError(err); isStatusErr {
			return statusErr.Message()
		}
	}
	return msg
}
