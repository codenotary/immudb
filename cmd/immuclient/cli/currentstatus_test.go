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

package cli

import (
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestCurrentRoot(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)

	cli := new(cli)
	cli.immucl = imc

	_, _ = cli.set([]string{"key", "val"})
	msg, err := cli.currentRoot([]string{""})

	if err != nil {
		t.Fatal("CurrentRoot fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("CurrentRoot failed: %s", msg)
	}
}
