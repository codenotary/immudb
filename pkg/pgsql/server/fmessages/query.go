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

type QueryMsg struct {
	statements string
}

func ParseQueryMsg(payload []byte) (QueryMsg, error) {
	msg := payload[:len(payload)-1] //-1 A null-terminated string
	return QueryMsg{statements: string(msg)}, nil
}

func (q *QueryMsg) GetStatements() string {
	return q.statements
}
