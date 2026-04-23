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

package database

import (
	"context"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

// TestScanSinceTxZeroSeesLatestCommitted guards the contract documented on
// ScanRequest.sinceTx: when sinceTx == 0, Scan must resolve to the latest
// committed transaction at call-reception time and wait for it to be indexed
// before taking the snapshot. Issue #2082.
func TestScanSinceTxZeroSeesLatestCommitted(t *testing.T) {
	db := makeDb(t)

	ctx := context.Background()
	prefix := []byte("scan-latest:")

	const n = 50
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("scan-latest:%03d", i))
		val := []byte(fmt.Sprintf("v-%03d", i))

		_, err := db.Set(ctx, &schema.SetRequest{
			KVs: []*schema.KeyValue{{Key: key, Value: val}},
		})
		require.NoError(t, err)

		// SinceTx: 0 must expose the just-committed key without the caller
		// needing to thread through the tx id from Set's response.
		list, err := db.Scan(ctx, &schema.ScanRequest{
			Prefix:  prefix,
			SinceTx: 0,
		})
		require.NoError(t, err)
		require.Len(t, list.Entries, i+1,
			"Scan(sinceTx=0) did not observe the just-committed write at iteration %d", i)
		require.Equal(t, key, list.Entries[i].Key)
		require.Equal(t, val, list.Entries[i].Value)
	}
}
