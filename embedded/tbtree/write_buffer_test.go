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
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/codenotary/immudb/embedded/metrics"
	"github.com/stretchr/testify/require"
)

func TestSharedWriteBuffer(t *testing.T) {
	chunkSize := 1024 * 1024

	numChunks := 128
	wb := NewSharedWriteBuffer(numChunks*chunkSize, chunkSize)

	numThreads := numChunks * 10

	t.Run("alloc slot", func(t *testing.T) {
		allocatedSlots := make([]int, numThreads)

		var wg sync.WaitGroup
		wg.Add(numThreads)
		for i := 0; i < numThreads; i++ {
			go func(i int) {
				defer wg.Done()

				slot := wb.AllocPageChunk()
				allocatedSlots[i] = slot
			}(i)
		}
		wg.Wait()

		for slot := range wb.state {
			require.True(t, wb.state[slot].Load())
		}
		require.Equal(t, wb.AllocatedChunks(), wb.NumChunks())

		sort.Slice(allocatedSlots, func(i, j int) bool {
			return allocatedSlots[i] < allocatedSlots[j]
		})

		n := numThreads - numChunks
		for _, slot := range allocatedSlots[:n] {
			require.Equal(t, -1, slot)
		}

		for i, slot := range allocatedSlots[n:] {
			require.Equal(t, i, slot)
		}
	})

	t.Run("release", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(numChunks)
		for i := 0; i < numChunks; i++ {
			go func(i int) {
				defer wg.Done()

				wb.Release(i)
			}(i)
		}
		wg.Wait()

		require.Zero(t, wb.AllocatedChunks())

		for slot := range wb.state {
			require.False(t, wb.state[slot].Load())
		}

		require.Panics(t, func() {
			wb.Release(rand.IntN(wb.NumChunks()))
		})
	})
}

func TestLocalWriteBuffer(t *testing.T) {
	chunkSize := 1024 * 1024
	numChunks := 128

	sb := NewSharedWriteBuffer(
		numChunks*chunkSize,
		chunkSize,
	)

	wb, err := NewWriteBuffer(sb, 0, numChunks*chunkSize, metrics.NewNopWriteBufferMetrics())
	require.NoError(t, err)

	require.Zero(t, wb.FreePages())
	require.Zero(t, wb.UsedPages())

	require.True(t, wb.Grow(1))
	require.Equal(t, sb.PagesPerChunk(), wb.FreePages())

	require.True(t, wb.Grow(256))
	require.Equal(t, sb.PagesPerChunk(), wb.FreePages())

	require.True(t, wb.Grow(257))
	require.Equal(t, 2*sb.PagesPerChunk(), wb.FreePages())

	require.Zero(t, wb.UsedPages())
	require.Equal(t, wb.FreePages(), 2*sb.PagesPerChunk())
}

func TestMultipleLocalWriteBuffer(t *testing.T) {
	chunkSize := 1024 * 1024
	numChunks := 128

	sb := NewSharedWriteBuffer(
		numChunks*chunkSize,
		chunkSize,
	)

	numBuffers := 100
	maxBufferSize := 10 * chunkSize

	buffers := make([]*WriteBuffer, numBuffers)
	for i := 0; i < numBuffers; i++ {
		wb, err := NewWriteBuffer(
			sb,
			chunkSize,
			maxBufferSize,
			metrics.NewNopWriteBufferMetrics(),
		)
		require.NoError(t, err)

		buffers[i] = wb
	}

	var totalPagesAllocated atomic.Uint32

	var wg sync.WaitGroup
	wg.Add(numBuffers)
	for i := 0; i < numBuffers; i++ {
		go func(i int) {
			defer wg.Done()

			for {
				_, _, err := buffers[i].AllocInnerPage()
				if err != nil {
					break
				}
				totalPagesAllocated.Add(1)
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, sb.NumPages(), int(totalPagesAllocated.Load()))
	require.Equal(t, sb.AllocatedChunks(), numChunks)
}
