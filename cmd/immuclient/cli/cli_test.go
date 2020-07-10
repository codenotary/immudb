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
	"testing"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestInit(t *testing.T) {
	cli := Init(nil)
	if len(cli.HelpMessage()) == 0 {
		t.Fatal("cli help failed")
	}
}
func TestRunCommand(t *testing.T) {
	cli := new(cli)
	cli.commands = make(map[string]*command, 0)
	cli.commandsList = make([]*command, 0)
	cli.initCommands()
	cli.helpInit()

	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)
	cli.immucl = imc
	cli.runCommand([]string{"set", "key", "value"})
}
