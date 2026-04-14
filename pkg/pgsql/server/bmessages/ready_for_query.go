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

package bmessages

import (
	"bytes"
	"encoding/binary"
)

// TransactionStatus byte values per the PG wire protocol's ReadyForQuery
// message. Clients use this to track their session-side transaction
// state — pq's `unexpected transaction status idle` error fires when
// the server returns 'I' after a successful BEGIN.
const (
	TxStatusIdle    byte = 'I'
	TxStatusInTx    byte = 'T'
	TxStatusFailed  byte = 'E'
)

// ReadyForQuery emits the standard PG `Z` message with the supplied
// transaction-status byte. Pass TxStatusIdle outside an explicit
// transaction; TxStatusInTx between BEGIN and COMMIT/ROLLBACK; TxStatusFailed
// once an in-transaction statement has errored and the client must roll
// back before continuing.
func ReadyForQuery(status byte) []byte {
	messageType := []byte(`Z`)
	message := make([]byte, 4)
	binary.BigEndian.PutUint32(message, uint32(5))
	return bytes.Join([][]byte{messageType, message, {status}}, nil)
}
