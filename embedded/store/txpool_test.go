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

package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTxPool(t *testing.T) {
	for _, preallocated := range []bool{true, false} {

		t.Run(fmt.Sprintf("preallocated: %v", preallocated), func(t *testing.T) {
			p, err := newTxPool(txPoolOptions{
				maxTxEntries: 100,
				maxKeyLen:    101,
				poolSize:     102,
				preallocated: preallocated,
			})
			require.NoError(t, err)

			t.Run("test initial pool stats", func(t *testing.T) {
				used, free, max := p.Stats()
				require.Equal(t, 0, used)
				require.Equal(t, 102, max)
				if preallocated {
					require.Equal(t, 102, free)
				} else {
					require.Equal(t, 0, free)
				}
			})

			t.Run("test allocation of single tx", func(t *testing.T) {
				tx, err := p.Alloc()
				require.NoError(t, err)
				require.NotNil(t, tx)
				require.Len(t, tx.entries, 100)
				require.NotNil(t, tx.htree)
				require.NotNil(t, tx.header)

				for _, e := range tx.entries {
					require.Len(t, e.k, 101)
				}

				used, free, max := p.Stats()
				require.Equal(t, 1, used)
				require.Equal(t, 102, max)

				if preallocated {
					require.Equal(t, 101, free)
				} else {
					require.Equal(t, 0, free)
				}

				p.Release(tx)

				used, free, max = p.Stats()
				require.Equal(t, 0, used)
				require.Equal(t, 102, max)
				if preallocated {
					require.Equal(t, 102, free)
				} else {
					require.Equal(t, 1, free)
				}
			})

			txsMap := map[*Tx]struct{}{}
			txs := []*Tx{}

			t.Run("test allocation of all pool entries", func(t *testing.T) {
				for i := 0; i < 102; i++ {
					tx, err := p.Alloc()
					require.NoError(t, err)

					_, found := txsMap[tx]
					require.False(t, found)
					txsMap[tx] = struct{}{}

					txs = append(txs, tx)

					used, free, max := p.Stats()
					require.Equal(t, len(txsMap), used)
					require.Equal(t, 102, max)
					if preallocated {
						require.Equal(t, 102-len(txsMap), free)
					} else {
						require.Equal(t, 0, free)
					}

					require.Len(t, tx.entries, 100)
				}
			})

			t.Run("ensure txs objects don't share buffers", func(t *testing.T) {
				for i := range txs {
					for j, e := range txs[i].entries {
						require.Len(t, e.k, 101)
						for k := range e.k {
							e.k[k] = byte(i + j + k)
						}
					}
				}

				for i := range txs {
					for j, e := range txs[i].entries {
						require.Len(t, e.k, 101)
						for k := range e.k {
							require.Equal(t, byte(i+j+k), e.k[k])
						}
					}
				}
			})

			t.Run("test error once the pool is exhausted", func(t *testing.T) {
				tx, err := p.Alloc()
				require.ErrorIs(t, err, ErrTxPoolExhausted)
				require.Nil(t, tx)
			})

			t.Run("test freeing all entries in the pool", func(t *testing.T) {
				for tx := range txsMap {
					p.Release(tx)
				}

				used, free, max := p.Stats()
				require.Equal(t, 0, used)
				require.Equal(t, 102, max)
				require.Equal(t, 102, free)
			})

			t.Run("test if reallocated object wre in the pool before", func(t *testing.T) {
				for i := 0; i < 102; i++ {
					tx, err := p.Alloc()
					require.NoError(t, err)

					_, found := txsMap[tx]
					require.True(t, found)

					delete(txsMap, tx)
				}
			})
		})
	}
}
