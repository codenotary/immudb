/*
Copyright 2019 vChain, Inc.

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
