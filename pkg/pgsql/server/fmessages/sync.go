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

// At completion of each series of extended-query messages, the frontend should issue a Sync message. This parameterless
// message causes the backend to close the current transaction if it's not inside a BEGIN/COMMIT transaction block
// (“close” meaning to commit if no error, or roll back if error). Then a ReadyForQuery response is issued. The purpose
// of Sync is to provide a resynchronization point for error recovery. When an error is detected while processing any
// extended-query message, the backend issues ErrorResponse, then reads and discards messages until a Sync is reached,
// then issues ReadyForQuery and returns to normal message processing. (But note that no skipping occurs if an error is
// detected while processing Sync — this ensures that there is one and only one ReadyForQuery sent for each Sync.)
type SyncMsg struct{}

func ParseSyncMsg(msg []byte) (SyncMsg, error) {
	return SyncMsg{}, nil
}
