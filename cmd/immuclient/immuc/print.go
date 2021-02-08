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

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

// PrintKV ...
func PrintKV(key []byte, value []byte, tx uint64, verified, valueOnly bool) string {
	hash := (&store.KV{Key: key, Value: value}).Digest()

	if valueOnly {
		return fmt.Sprintf("%s\n", value)
	}

	str := strings.Builder{}
	if !valueOnly {
		str.WriteString(fmt.Sprintf("tx:		%d \n", tx))
		str.WriteString(fmt.Sprintf("key:		%s \n", key))
		str.WriteString(fmt.Sprintf("value:		%s \n", value))
		str.WriteString(fmt.Sprintf("hash:		%x \n", hash))
		if verified {
			str.WriteString(fmt.Sprintf("verified:	%t \n", verified))
		}
	}

	return str.String()
}

// PrintSetItem ...
func PrintSetItem(set []byte, referencedkey []byte, score float64, txMetadata *schema.TxMetadata, verified bool) string {
	return fmt.Sprintf("tx:		%d\nset:		%s\nreferenced key:		%s\nscore:		%f\nhash:		%x\nverified:	%t\n",
		txMetadata.Id,
		set,
		referencedkey,
		score,
		txMetadata.EH,
		verified)
}

// PrintState ...
func PrintState(root *schema.ImmutableState) string {
	if root.TxId == 0 {
		return "immudb is empty\n"
	}
	return fmt.Sprintf("txID:		%d\nhash:		%x\n", root.TxId, root.TxHash)
}

// PrintTx ...
func PrintTx(tx *schema.Tx, verified bool) string {
	str := strings.Builder{}
	str.WriteString(fmt.Sprintf("tx:		%d\n", tx.Metadata.Id))
	str.WriteString(fmt.Sprintf("time:		%s\n", time.Unix(int64(tx.Metadata.Ts), 0)))
	str.WriteString(fmt.Sprintf("entries:	%d\n", tx.Metadata.Nentries))
	str.WriteString(fmt.Sprintf("hash:		%x\n", schema.TxMetadataFrom(tx.Metadata).Alh()))
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
