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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/codenotary/immudb/pkg/bm"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
)

// Iterations number of iterations to perform
const Iterations = 500_000

// BatchSize batch size
const BatchSize = 100

var tmpDir, _ = ioutil.TempDir("", "immu")
var immuServer = server.DefaultServer().
	WithOptions(
		server.DefaultOptions().WithDir(tmpDir),
	)

var immuClient, _ = client.NewImmuClient(client.DefaultOptions())

// RPCBenchmarks ...
var RPCBenchmarks = []bm.Bm{
	makeRPCBenchmark("sequential write", Concurrency, Iterations, sequentialSet),
	makeRPCBenchmark("batch write", Concurrency, Iterations, batchSet),
	makeRPCBenchmark("batch write no concurrency", 1, Iterations, batchSet),
}

func sequentialSet(bm *bm.Bm, start int, end int) error {
	for i := start; i < end; i++ {
		key := []byte(strconv.FormatUint(uint64(i), 10))
		if _, err := immuClient.Set(context.Background(), key, V); err != nil {
			return err
		}
	}
	return nil
}

func batchSet(bm *bm.Bm, start int, end int) error {
	var keyReaders []io.Reader
	var valueReaders []io.Reader
	for i := start; i < end; i++ {
		key := []byte(strconv.FormatUint(uint64(i), 10))
		keyReaders = append(keyReaders, bytes.NewReader(key))
		valueReaders = append(valueReaders, bytes.NewReader(V))
		if i%BatchSize == 0 || i == end-1 {
			if _, err := immuClient.SetBatch(context.Background(), &client.BatchRequest{
				Keys:   keyReaders,
				Values: valueReaders,
			}); err != nil {
				return err
			}
			keyReaders = nil
			valueReaders = nil
		}
	}
	return nil
}

func makeRPCBenchmark(name string, concurrency int, iterations int,
	work func(bm *bm.Bm, start int, end int) error) bm.Bm {
	return bm.Bm{
		CreateStore: false,
		Name:        name,
		Concurrency: concurrency,
		Iterations:  iterations,
		Before: func(bm *bm.Bm) {
			go func() {
				if err := immuServer.Start(); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
			}()
		},
		After: func(bm *bm.Bm) {
			if err := immuClient.Disconnect(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := immuServer.Stop(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := os.RemoveAll(tmpDir); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		},
		Work: work,
	}
}
