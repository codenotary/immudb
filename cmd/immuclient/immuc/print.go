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

package immuc

import (
	"fmt"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
)

// PrintKV ...
func PrintKV(
	entry *schema.Entry,
	verified, valueOnly bool,
) string {
	if valueOnly {
		return fmt.Sprintf("%s\n", entry.Value)
	}

	str := &strings.Builder{}
	fmt.Fprintf(str, "tx:       %d\n", entry.Tx)

	if entry.Revision != 0 {
		fmt.Fprintf(str, "rev:      %d\n", entry.Revision)
	}

	fmt.Fprintf(str, "key:      %s\n", entry.Key)

	if entry.Metadata != nil {
		fmt.Fprintf(str, "metadata: {%s}\n", entry.Metadata)
	}

	fmt.Fprintf(str, "value:    %s\n", entry.Value)

	if verified {
		fmt.Fprintf(str, "verified: %t\n", verified)
	}

	return str.String()
}

// PrintSetItem ...
func PrintSetItem(set []byte, referencedkey []byte, score float64, txhdr *schema.TxHeader, verified bool) string {
	return fmt.Sprintf("tx:		%d\nset:		%s\nreferenced key:		%s\nscore:		%f\nhash:		%x\nverified:	%t\n",
		txhdr.Id,
		set,
		referencedkey,
		score,
		txhdr.EH,
		verified)
}

func PrintServerInfo(resp *schema.ServerInfoResponse) string {
	return fmt.Sprintf("version:	%s", resp.GetVersion())
}

func PrintHealth(res *schema.DatabaseHealthResponse) string {
	return fmt.Sprintf("pendingRequests:		%d\nlastRequestCompletedAt:		%s\n", res.PendingRequests, time.Unix(0, res.LastRequestCompletedAt*int64(time.Millisecond)))
}

// PrintState ...
func PrintState(root *schema.ImmutableState) string {
	if root.TxId == 0 {
		return fmt.Sprintf("database '%s' is empty\n", root.Db)
	}

	str := strings.Builder{}

	if root.PrecommittedTxId == 0 {
		str.WriteString(fmt.Sprintf("database:  %s\n", root.Db))
		str.WriteString(fmt.Sprintf("txID:      %d\n", root.TxId))
		str.WriteString(fmt.Sprintf("hash:      %x\n", root.TxHash))
	} else {
		str.WriteString(fmt.Sprintf("database:         %s\n", root.Db))
		str.WriteString(fmt.Sprintf("txID:             %d\n", root.TxId))
		str.WriteString(fmt.Sprintf("hash:             %x\n", root.TxHash))
		str.WriteString(fmt.Sprintf("precommittedTxID: %d\n", root.PrecommittedTxId))
		str.WriteString(fmt.Sprintf("precommittedHash: %x\n", root.PrecommittedTxHash))
	}

	return str.String()
}

// PrintTx ...
func PrintTx(tx *schema.Tx, verified bool) string {
	str := strings.Builder{}
	str.WriteString(fmt.Sprintf("tx:		%d\n", tx.Header.Id))
	str.WriteString(fmt.Sprintf("time:		%s\n", time.Unix(int64(tx.Header.Ts), 0)))
	str.WriteString(fmt.Sprintf("entries:	%d\n", tx.Header.Nentries))
	str.WriteString(fmt.Sprintf("hash:		%x\n", schema.TxHeaderFromProto(tx.Header).Alh()))
	if verified {
		str.WriteString(fmt.Sprintf("verified:	%t \n", verified))
	}

	return str.String()
}

// PadRight ...
func PadRight(str, pad string, length int) string {
	for {
		str += pad
		if len(str) > length {
			return str[0:length]
		}
	}
}
