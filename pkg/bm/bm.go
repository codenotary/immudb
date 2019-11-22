package bm

import (
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/db"
)

type Bm struct {
	CreateTopic bool
	Topic       *db.Topic
	Name        string
	Concurrency int
	Iterations  int
	Before      func(bm *Bm)
	After       func(bm *Bm)
	Work        func(bm *Bm, start int, end int)
}

func (b *Bm) Execute() *BmResult {
	var wg sync.WaitGroup
	chunkSize := b.Iterations / b.Concurrency
	if b.Topic == nil && b.CreateTopic {
		topic, closer := makeTopic()
		b.Topic = topic
		defer closer()
	}
	if b.Before != nil {
		b.Before(b)
	}
	startTime := time.Now()
	for k := 0; k < b.Concurrency; k++ {
		wg.Add(1)
		go func(kk int) {
			defer wg.Done()
			start := kk * chunkSize
			end := (kk + 1) * chunkSize
			b.Work(b, start, end)
		}(k)
	}
	wg.Wait()
	endTime := time.Now()
	elapsed := float64(endTime.UnixNano()-startTime.UnixNano()) / (1000 * 1000 * 1000)
	txnSec := float64(b.Iterations) / elapsed
	if b.After != nil {
		b.After(b)
	}
	return &BmResult{
		Bm:           b,
		Time:         elapsed,
		Transactions: txnSec,
	}
}
