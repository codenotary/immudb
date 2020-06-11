/*
Copyright 2019-2020 vChain, Inc.

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

package suite

import (
	"runtime"
	"strconv"

	"github.com/codenotary/immudb/pkg/api/schema"

	"github.com/codenotary/immudb/pkg/bm"
	"github.com/codenotary/immudb/pkg/store"
)

var maxProcs int

// Concurrency number of go parallel executions
var Concurrency = runtime.NumCPU()

// V ...
var V = []byte{0, 1, 3, 4, 5, 6, 7}

// FunctionBenchmarks ...
var FunctionBenchmarks = []bm.Bm{
	// {
	// 	CreateStore: true,
	// 	Name:        "sequential write (fine tuned / experimental)",
	// 	Concurrency: 1_000_000, // Concurrency,
	// 	Iterations:  1_000_000,
	// 	Before: func(bm *bm.Bm) {
	// 		maxProcs = runtime.GOMAXPROCS(Concurrency)
	// 	},
	// 	After: func(bm *bm.Bm) {
	// 		runtime.GOMAXPROCS(maxProcs)
	// 	},
	// 	Work: func(bm *bm.Bm, start int, end int) error {
	// 		for i := start; i < end; i++ {
	// 			kv := schema.KeyValue{Key:[]byte(strconv.FormatUint(uint64(i), 10)), Value: V}
	// 			if _, err := bm.Store.Set(kv); err != nil {
	// 				return err
	// 			}
	// 		}
	// 		return nil
	// 	},
	// },
	{
		CreateStore: true,
		Name:        "sequential write (baseline)",
		Concurrency: Concurrency,
		Iterations:  1_000_000,
		Work: func(bm *bm.Bm, start int, end int) error {
			for i := start; i < end; i++ {
				kv := schema.KeyValue{Key: []byte(strconv.FormatUint(uint64(i), 10)), Value: V}
				if _, err := bm.Store.Set(kv); err != nil {
					return err
				}
			}
			return nil
		},
	},
	{
		CreateStore: true,
		Name:        "sequential write (async commit)",
		Concurrency: Concurrency,
		Iterations:  1_000_000,
		Work: func(bm *bm.Bm, start int, end int) error {
			opt := store.WithAsyncCommit(true)
			for i := start; i < end; i++ {
				kv := schema.KeyValue{Key: []byte(strconv.FormatUint(uint64(i), 10)), Value: V}
				if _, err := bm.Store.Set(kv, opt); err != nil {
					return err
				}
			}
			return nil
		},
	},
	{
		CreateStore: true,
		Name:        "sequential write (concurrency++)",
		Concurrency: Concurrency * 8,
		Iterations:  1_000_000,
		Work: func(bm *bm.Bm, start int, end int) error {
			for i := start; i < end; i++ {
				kv := schema.KeyValue{Key: []byte(strconv.FormatUint(uint64(i), 10)), Value: V}
				if _, err := bm.Store.Set(kv); err != nil {
					return err
				}
			}
			return nil
		},
	},
	{
		CreateStore: true,
		Name:        "sequential write (async commit / concurrency++)",
		Concurrency: Concurrency * 8,
		Iterations:  1_000_000,
		Work: func(bm *bm.Bm, start int, end int) error {
			opt := store.WithAsyncCommit(true)
			for i := start; i < end; i++ {
				kv := schema.KeyValue{Key: []byte(strconv.FormatUint(uint64(i), 10)), Value: V}
				if _, err := bm.Store.Set(kv, opt); err != nil {
					return err
				}
			}
			return nil
		},
	},
	{
		CreateStore: true,
		Name:        "sequential write (GOMAXPROCS=128 / concurrency++)",
		Concurrency: Concurrency * 8,
		Iterations:  1_000_000,
		Before: func(bm *bm.Bm) {
			maxProcs = runtime.GOMAXPROCS(128)
		},
		After: func(bm *bm.Bm) {
			runtime.GOMAXPROCS(maxProcs)
		},
		Work: func(bm *bm.Bm, start int, end int) error {
			for i := start; i < end; i++ {
				kv := schema.KeyValue{Key: []byte(strconv.FormatUint(uint64(i), 10)), Value: V}
				if _, err := bm.Store.Set(kv); err != nil {
					return err
				}
			}
			return nil
		},
	},
	{
		CreateStore: true,
		Name:        "batch write",

		// batch size cannot execeed 100k items otherwise txn fail with:
		// "Txn is too big to fit into one request"
		Concurrency: func() int {
			if Concurrency < 10 {
				return 10
			}
			return Concurrency
		}(),
		Iterations: 1_000_000,

		Work: func(bm *bm.Bm, start int, end int) error {
			list := schema.KVList{}
			for i := start; i < end; i++ {
				list.KVs = append(list.KVs, &schema.KeyValue{
					Key:   []byte(strconv.FormatUint(uint64(i), 10)),
					Value: V,
				})
			}
			if _, err := bm.Store.SetBatch(list); err != nil {
				return err
			}
			return nil
		},
	},
	{
		CreateStore: true,
		Name:        "batch write (async commit)",

		// batch size cannot execeed 100k items otherwise txn fail with:
		// "Txn is too big to fit into one request"
		Concurrency: func() int {
			if Concurrency < 10 {
				return 10
			}
			return Concurrency
		}(),
		Iterations: 1_000_000,

		Work: func(bm *bm.Bm, start int, end int) error {
			opt := store.WithAsyncCommit(true)
			list := schema.KVList{}
			for i := start; i < end; i++ {
				list.KVs = append(list.KVs, &schema.KeyValue{
					Key:   []byte(strconv.FormatUint(uint64(i), 10)),
					Value: V,
				})
			}
			if _, err := bm.Store.SetBatch(list, opt); err != nil {
				return err
			}
			return nil
		},
	},
}
