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

package server

import (
	"os"
	"testing"
	"time"

	"google.golang.org/grpc/test/bufconn"
)

func TestService(t *testing.T) {
	bufSize := 1024 * 1024
	l := bufconn.Listen(bufSize)
	datadir := "rome"
	defer func() {
		os.RemoveAll(datadir)
	}()
	options := DefaultOptions().WithAuth(true).WithCorruptionCheck(true).WithDir(datadir).WithListener(l).WithPort(22222).WithMetricsServer(false)

	server := DefaultServer().WithOptions(options).(*ImmuServer)

	srvc := &Service{
		ImmuServerIf: server,
	}

	srvc.Start()
	time.Sleep(1 * time.Second)
	srvc.Stop()
}
