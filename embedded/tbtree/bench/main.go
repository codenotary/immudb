package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/embedded/tbtree"
)

func main() {
	size := 1024 * 1024 * 128

	fmt.Println("Pages Per Chunk | Alloc/sec | Time (ms)")
	fmt.Println("------------------------------------------------")

	n := 10000

	numThreads := 64

	// Test with increasing number of goroutines
	for pages := 1; pages <= 256; pages *= 2 {
		var wg sync.WaitGroup
		var allocCount atomic.Uint32

		swb := tbtree.NewSharedWriteBuffer(size, pages*tbtree.PageSize)

		buffers := make([]*tbtree.WriteBuffer, numThreads)
		for t := 0; t < 64; t++ {
			wb, err := tbtree.NewWriteBuffer(swb, pages*tbtree.PageSize, size)
			if err != nil {
				panic(err)
			}
			buffers[t] = wb
		}

		start := time.Now()

		wg.Add(numThreads)
		for t := 0; t < numThreads; t++ {
			go func(wb *tbtree.WriteBuffer) {
				defer wg.Done()
				for i := 0; i < n; i++ {
					_, _, err := wb.AllocLeafPage()
					if errors.Is(err, tbtree.ErrWriteBufferFull) || errors.Is(err, tbtree.ErrNoChunkAvailable) {
						wb.Reset()
					}

					if err == nil {
						allocCount.Add(1)
					}
				}
			}(buffers[t])
		}
		wg.Wait()

		swb.ReleaseAll()

		// Calculate and display results
		elapsed := time.Since(start).Milliseconds()
		fmt.Printf("%7d | %11d | %9d\n", pages, allocCount.Load(), elapsed)
	}
}
