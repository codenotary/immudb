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

package tbtree

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/codenotary/immudb/v2/embedded/metrics"
	"github.com/stretchr/testify/require"
)

func TestPageCache(t *testing.T) {
	maxPages := 10

	pgContent := make([]byte, PageSize)
	rand.Read(pgContent)

	cache := NewPageCache(maxPages, metrics.NewNopPageCacheMetrics())

	pageLoader := func(dst []byte, id PageID) error {
		if id != 0 {
			return fmt.Errorf("invalid page")
		}
		copy(dst, pgContent)
		return nil
	}

	_, err := cache.Get(0, 1, pageLoader)
	require.ErrorContains(t, err, "invalid page")

	t.Run("redundant loads are avoided", func(t *testing.T) {
		var wg sync.WaitGroup
		n := 1000
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()

				page, err := cache.Get(0, 0, pageLoader)
				require.NoError(t, err)
				require.NotNil(t, page)
				require.Equal(t, page.Bytes(), pgContent)
			}()
		}
		wg.Wait()
	})
}

func BenchmarkPageBuffer(b *testing.B) {
	for nPages := 5; nPages < 100000; nPages *= 2 {
		b.Run(fmt.Sprintf("nPages=%d", nPages), func(b *testing.B) {
			pgBuf := NewPageCache(nPages*PageSize, metrics.NewNopPageCacheMetrics())

			nThreads := 1000

			b.ResetTimer()

			var wg sync.WaitGroup
			wg.Add(nThreads)
			for n := 0; n < nThreads; n++ {
				go func(n int) {
					defer wg.Done()

					pgID := PageID(rand.Intn(100000))
					_, err := pgBuf.Get(TreeID(n), pgID, func(dst []byte, id PageID) error {
						rand.Read(dst)
						return nil
					})
					if err != nil {
						panic(err)
					}
					pgBuf.Release(TreeID(n), pgID)
				}(n)
			}
			wg.Wait()
		})
	}
}
