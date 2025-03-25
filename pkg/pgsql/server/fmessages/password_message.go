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

package fmessages

type PasswordMsg struct {
	secret string
}

func ParsePasswordMsg(payload []byte) (PasswordMsg, error) {
	password := payload[:len(payload)-1] //-1 A null-terminated string
	return PasswordMsg{secret: string(password)}, nil
}

func (pw *PasswordMsg) GetSecret() string {
	return pw.secret
}
