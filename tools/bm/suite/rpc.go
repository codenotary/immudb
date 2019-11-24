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

package suite

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/codenotary/immudb/pkg/bm"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
)

var tmpDir, _ = ioutil.TempDir("", "immudb")
var immuServer = server.DefaultServer().
	WithOptions(
		server.DefaultOptions().WithDir(tmpDir),
	)
var immuClient = client.DefaultClient()

var RpcBenchmarks = []bm.Bm{
	{
		CreateTopic: false,
		Name:        "sequential write (baseline)",
		Concurrency: Concurrency,
		Iterations:  100_000,
		Before: func(bm *bm.Bm) {
			go func() {
				if err := immuServer.Run(); err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}()
			for i := 0; i < 5; i++ {
				result, _ := immuClient.HealthCheck()
				if result {
					return
				}
				time.Sleep(time.Second * 2)
			}
		},
		After: func(bm *bm.Bm) {
			if err := immuServer.Stop(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			if err := os.RemoveAll(tmpDir); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
		Work: func(bm *bm.Bm, start int, end int) {
			for i := start; i < end; i++ {
				key := []byte(strconv.FormatUint(uint64(i), 10))
				_, err := immuClient.Set(key, bytes.NewReader(V))
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
			}
		},
	},
}
