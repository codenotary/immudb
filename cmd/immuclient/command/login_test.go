/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package immuclient

import (
	"bytes"
	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/helper"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestLogin(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.WithTokenFileService(ts)
	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.login(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"login", "immudb"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd := cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil
	// since we issue two commands we need to remove PersistentPostRun ( disconnect )
	innercmd.PersistentPostRun = nil

	err := cmd.Execute()

	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "Successfully logged in") {
		t.Fatal(err)
	}

	cmd, _ = cmdl.NewCmd()
	cmdl.logout(cmd)
	cmd.SetOut(b)
	cmd.SetArgs([]string{"logout"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	_, err = ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
}
