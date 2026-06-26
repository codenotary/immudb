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

package multiapp

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMultiAppConcurrentReadEvictionRace exercises the read path under a
// cache too small to hold the working set while background prefetch is
// active. A foreground cache-miss opens and caches a chunk, then has to
// re-acquire it from the cache; a concurrent prefetch insert can evict
// that just-inserted entry in between, which previously surfaced as a
// spurious "key not found" error from ReadAt for a perfectly valid
// offset. The read must always succeed and return the correct bytes.
func TestMultiAppConcurrentReadEvictionRace(t *testing.T) {
	const (
		fileSize  = 8
		numChunks = 32
		total     = fileSize * numChunks
	)

	a, err := Open(t.TempDir(),
		DefaultOptions().
			WithFileSize(fileSize).
			WithMaxOpenedFiles(2).
			WithPrefetchAheadDepth(4),
	)
	require.NoError(t, err)

	data := make([]byte, total)
	for i := range data {
		data[i] = byte(i)
	}

	_, _, err = a.Append(data)
	require.NoError(t, err)
	require.NoError(t, a.Flush())

	const readers = 16
	var wg sync.WaitGroup
	errCh := make(chan error, readers)

	for g := 0; g < readers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1)
			for rep := 0; rep < 8; rep++ {
				// Sequential scan triggers prefetch-ahead.
				for off := int64(0); off < total; off++ {
					n, rerr := a.ReadAt(buf, off)
					if rerr != nil {
						errCh <- rerr
						return
					}
					if n != 1 || buf[0] != byte(off) {
						errCh <- fmt.Errorf("offset %d: read %d bytes = %v, want byte %d", off, n, buf[:n], byte(off))
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for rerr := range errCh {
		require.NoError(t, rerr)
	}

	require.NoError(t, a.Close())
}
