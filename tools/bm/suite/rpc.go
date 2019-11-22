package suite

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/codenotary/immudb/pkg/bm"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
)

var tmpDir, _ = ioutil.TempDir("", "immudb")
var immuServer = server.DefaultServer().
	WithOptions(server.DefaultOptions().WithDir(tmpDir))
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
				_, _ = immuClient.Set(key, bytes.NewReader(V))
			}
		},
	},
}
