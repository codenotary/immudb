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

package immuclient

import (
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestGetByIndex(t *testing.T) {
	defer os.Remove(".root-")
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.getByIndex(cmd)
	cmdl.safeset(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd := cmd.Commands()[1]
	innercmd.PersistentPreRunE = nil
	// since we issue two commands we need to remove PersistentPostRun ( disconnect )
	cmd.Commands()[1].PersistentPostRun = nil

	err := cmd.Execute()

	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"getByIndex", "0"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "hash") {
		t.Fatal(err)
	}
}

func TestGetRawBySafeIndex(t *testing.T) {
	defer os.Remove(".root-")
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.getRawBySafeIndex(cmd)
	cmdl.safeset(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	cmd.Commands()[0].PersistentPreRunE = nil
	cmd.Commands()[1].PersistentPreRunE = nil
	// since we issue two commands we need to remove PersistentPostRun ( disconnect )
	cmd.Commands()[1].PersistentPostRun = nil

	err := cmd.Execute()

	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"getRawBySafeIndex", "0"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "hash") {
		t.Fatal(err)
	}
}

func TestGetKey(t *testing.T) {
	defer os.Remove(".root-")
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.getKey(cmd)
	cmdl.safeset(cmd)

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	cmd.Commands()[0].PersistentPreRunE = nil
	cmd.Commands()[1].PersistentPreRunE = nil
	// since we issue two commands we need to remove PersistentPostRun ( disconnect )
	cmd.Commands()[1].PersistentPostRun = nil

	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})

	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"get", "key"})

	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "hash") {
		t.Fatal(err)
	}
}

func TestSafeGetKey(t *testing.T) {
	defer os.Remove(".root-")
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.safeGetKey(cmd)
	cmdl.safeset(cmd)

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	cmd.Commands()[0].PersistentPreRunE = nil
	cmd.Commands()[1].PersistentPreRunE = nil
	// since we issue two commands we need to remove PersistentPostRun ( disconnect )
	cmd.Commands()[1].PersistentPostRun = nil

	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd := cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	err := cmd.Execute()

	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"safeget", "key"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "hash") {
		t.Fatal(err)
	}
}

func TestRawSafeGetKey(t *testing.T) {
	defer os.Remove(".root-")
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.rawSafeGetKey(cmd)
	cmdl.safeset(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	cmd.Commands()[0].PersistentPreRunE = nil
	cmd.Commands()[1].PersistentPreRunE = nil
	// since we issue two commands we need to remove PersistentPostRun ( disconnect )
	cmd.Commands()[1].PersistentPostRun = nil

	err := cmd.Execute()

	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"rawsafeget", "key"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "hash") {
		t.Fatal(err)
	}
}
