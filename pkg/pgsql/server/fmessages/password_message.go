/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

package fmessages

import pgserrors "github.com/codenotary/immudb/pkg/pgsql/errors"

type PasswordMsg struct {
	secret string
}

func ParsePasswordMsg(payload []byte) (PasswordMsg, error) {
	if len(payload) < 1 {
		return PasswordMsg{}, pgserrors.ErrMalformedMessage
	}
	password := payload[:len(payload)-1] //-1 A null-terminated string
	return PasswordMsg{secret: string(password)}, nil
}

func (pw *PasswordMsg) GetSecret() string {
	return pw.secret
}
