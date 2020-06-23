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

package immuadmin

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	immuclient "github.com/codenotary/immudb/pkg/client"
	immudb "github.com/codenotary/immudb/pkg/server"
)

func generateRandomTCPPort() int {
	rand.Seed(time.Now().UnixNano())
	min := 1024
	max := 64000
	return rand.Intn(max-min+1) + min
}

func TestDumpToFile(t *testing.T) {
	tcpPort := generateRandomTCPPort()
	clientDir := "ClDir" + strconv.FormatInt(int64(tcpPort), 10)
	//MetricsServer must not be started as during tests because prometheus lib panics with: duplicate metrics collector registration attempted
	op := immudb.DefaultOptions().
		WithPort(tcpPort).WithDir("db_" + strconv.FormatInt(int64(tcpPort), 10)).
		WithCorruptionCheck(false).WithAuth(false)
	s := immudb.DefaultServer().WithOptions(op)
	go s.Start()
	time.Sleep(2 * time.Second)
	defer func() {
		s.Stop()
		time.Sleep(2 * time.Second) //without the delay the db dir is deleted before all the data has been flushed to disk and results in crash.
		os.RemoveAll(op.Dir)
		os.RemoveAll(clientDir)
	}()
	clOp := immuclient.DefaultOptions().WithPort(tcpPort).WithDir(clientDir)
	ic, err := immuclient.NewImmuClient(clOp)
	if err != nil {
		t.Errorf("%s", err.Error())
	}
	type f func()
	tt := []struct {
		test      string
		wantCount int64
		exec      f
	}{
		{
			"Backup zero records",
			2,
			func() {},
		},
		{
			"Backup two records",
			8,
			func() {
				ic.SafeSet(context.Background(), []byte("Jan"), []byte("ulrich"))
				ic.SafeSet(context.Background(), []byte("Jan"), []byte("ulrich"))
			},
		},
	}
	for _, tc := range tt {
		t.Run(tc.test, func(t *testing.T) {
			filename := fmt.Sprintf("immudb_%d.bkp", time.Now().UnixNano())
			file, err := os.Create(filename)
			if err != nil {
				t.Errorf("%s", err.Error())
			}
			defer func() {
				file.Close()
				os.Remove(filename)
			}()
			tc.exec()
			recDumped, err := ic.Dump(context.Background(), file)
			if err != nil {
				t.Errorf("%s", err.Error())
			}
			if recDumped != tc.wantCount {
				t.Errorf("Excpected %d dumped records got %d", tc.wantCount, recDumped)
			}
		})
	}
}
