package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/embedded/metrics"
	"github.com/codenotary/immudb/embedded/tbtree"
)

func main() {
	size := 1024 * 1024 * 128

	fmt.Println("| NumChunks | Alloc time (ms) |")
	fmt.Println("------------------------------------------------")

	const n = 10000

	const numThreads = 64

	// Test with increasing number of goroutines
	for chunkSizePages := 1; chunkSizePages <= 256; chunkSizePages *= 2 {
		var wg sync.WaitGroup
		var allocCount atomic.Uint32

		swb := tbtree.NewSharedWriteBuffer(size, chunkSizePages*tbtree.PageSize)

		buffers := make([]*tbtree.WriteBuffer, numThreads)
		for t := 0; t < 64; t++ {
			wb, err := tbtree.NewWriteBuffer(swb, chunkSizePages*tbtree.PageSize, size, metrics.NewNopWriteBufferMetrics())
			if err != nil {
				panic(err)
			}
			buffers[t] = wb
		}

		var times [numThreads]time.Duration

		wg.Add(numThreads)
		for t := 0; t < numThreads; t++ {
			go func(wb *tbtree.WriteBuffer, nTh int) {
				defer wg.Done()
				for i := 0; i < n; i++ {
					start := time.Now()
					_, _, err := wb.AllocLeafPage()
					times[nTh] += time.Since(start)

					if errors.Is(err, tbtree.ErrWriteBufferFull) || errors.Is(err, tbtree.ErrNoChunkAvailable) {
						wb.Reset()
					}

					if err == nil {
						allocCount.Add(1)
					}
				}
			}(buffers[t], t)
		}
		wg.Wait()

		swb.ReleaseAll()

		avg, std := computeStats(times[:])
		fmt.Printf("| %d | %f | %f\n", chunkSizePages, avg, std)
	}
}

func computeStats(times []time.Duration) (float64, float64) {
	avgTime := avg(times)

	std := float64(0)
	for _, v := range times {
		diff := (float64(v.Milliseconds()) - avgTime)
		std += diff * diff
	}
	return avgTime, std
}

func avg(times []time.Duration) float64 {
	x := time.Duration(0)
	for _, v := range times {
		x += v
	}
	return float64(x.Milliseconds()) / float64(len(times))
}
