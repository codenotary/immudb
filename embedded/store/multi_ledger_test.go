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

package store

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetSameKeyOnMultipleLedgers(t *testing.T) {
	st, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)
	defer st.Close()

	numLedgers := 10
	ledgers := make([]*Ledger, numLedgers)

	for n := 0; n < numLedgers; n++ {
		ledger, err := st.OpenLedger(fmt.Sprintf("ledger-%d", n))
		require.NoError(t, err)

		ledgers[n] = ledger
	}

	var wg sync.WaitGroup
	wg.Add(numLedgers)

	for n := 0; n < numLedgers; n++ {
		go func(n int) {
			defer wg.Done()

			l := ledgers[n]
			otx, err := l.NewTx(context.Background(), DefaultTxOptions())
			require.NoError(t, err)

			err = otx.Set([]byte("key"), nil, []byte(fmt.Sprintf("value-%d", n)))
			require.NoError(t, err)

			_, err = otx.Commit(context.Background())
			require.NoError(t, err)
		}(n)
	}
	wg.Wait()

	for n, ledger := range ledgers {
		otx, err := ledger.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		valRef, err := otx.Get(context.Background(), []byte("key"))
		require.NoError(t, err)

		val, err := ledger.Resolve(valRef)
		require.NoError(t, err)
		require.Equal(t, []byte(fmt.Sprintf("value-%d", n)), val)

		err = otx.Cancel()
		require.NoError(t, err)
	}

	rand.Shuffle(numLedgers, func(i, j int) {
		ledgers[i], ledgers[j] = ledgers[j], ledgers[i]
	})

	// remove key from first half of ledgers
	for n := 0; n < numLedgers/2; n++ {
		ledger := ledgers[n]

		otx, err := ledger.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = otx.Delete(context.Background(), []byte("key"))
		require.NoError(t, err)

		_, err = otx.Commit(context.Background())
		require.NoError(t, err)
	}

	for n := 0; n < numLedgers; n++ {
		ledger := ledgers[n]

		_, err := ledger.Get(context.Background(), []byte("key"))
		if n < numLedgers/2 {
			require.ErrorIs(t, err, ErrKeyNotFound)
		} else {
			require.NoError(t, err)
		}
	}

	for n := 0; n < numLedgers/2; n++ {
		ledger := ledgers[n]

		err := ledger.CloseIndexing(nil)
		require.NoError(t, err)

		_, err = ledger.Get(context.Background(), []byte("key"))
		require.ErrorIs(t, err, ErrKeyNotFound)
	}

	for n := numLedgers / 2; n < numLedgers; n++ {
		ledger := ledgers[n]

		_, err := ledger.Get(context.Background(), []byte("key"))
		require.NoError(t, err)
	}
}
