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

package immuc

import (
	"fmt"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
)

// PrintKV ...
func PrintKV(key []byte, md *schema.KVMetadata, value []byte, tx uint64, verified, valueOnly bool) string {
	if valueOnly {
		return fmt.Sprintf("%s\n", value)
	}

	str := strings.Builder{}
	if !valueOnly {
		str.WriteString(fmt.Sprintf("tx:		%d \n", tx))
		str.WriteString(fmt.Sprintf("key:		%s \n", key))

		if md != nil {
			str.WriteString(fmt.Sprintf("metadata:	{%s} \n", md))
		}

		str.WriteString(fmt.Sprintf("value:		%s \n", value))

		if verified {
			str.WriteString(fmt.Sprintf("verified:	%t \n", verified))
		}
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

func PrintHealth(res *schema.DatabaseHealthResponse) string {
	return fmt.Sprintf("pendingRequests:		%d\nlastRequestCompletedAt:		%s\n", res.PendingRequests, time.Unix(0, res.LastRequestCompletedAt*int64(time.Millisecond)))
}

// PrintState ...
func PrintState(root *schema.ImmutableState) string {
	if root.TxId == 0 {
		return fmt.Sprintf("database %s is empty\n", root.Db)
	}
	return fmt.Sprintf("txID:		%d\nhash:		%x\n", root.TxId, root.TxHash)
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
