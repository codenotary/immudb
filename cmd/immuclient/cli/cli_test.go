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
	"github.com/codenotary/immudb/pkg/client"
	"strings"
	"testing"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/peterh/liner"
	"github.com/stretchr/testify/require"
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

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cli.immucl = ic.Imc

	msg := test.CaptureStdout(func() {
		cli.runCommand([]string{"set", "key", "value"})
	})
	if !strings.Contains(msg, "hash") {
		t.Fatal(msg)
	}
}

func TestRunCommandExtraArgs(t *testing.T) {
	cli := new(cli)
	cli.commands = make(map[string]*command, 0)
	cli.commandsList = make([]*command, 0)
	cli.initCommands()
	cli.helpInit()

	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cli.immucl = ic.Imc

	msg := test.CaptureStdout(func() {
		cli.runCommand([]string{"set", "key", "value", "value"})
	})
	if !strings.Contains(msg, "Redunant argument") {
		t.Fatal(msg)
	}
}
func TestRunMissingArgs(t *testing.T) {
	cli := new(cli)
	cli.commands = make(map[string]*command, 0)
	cli.commandsList = make([]*command, 0)
	cli.initCommands()
	cli.helpInit()

	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cli.immucl = ic.Imc

	msg := test.CaptureStdout(func() {
		cli.runCommand([]string{"set", "key"})
	})
	if !strings.Contains(msg, "Not enough arguments") {
		t.Fatal(msg)
	}
}

func TestRunWrongCommand(t *testing.T) {
	cli := new(cli)
	cli.commands = make(map[string]*command, 0)
	cli.commandsList = make([]*command, 0)
	cli.initCommands()
	cli.helpInit()

	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cli.immucl = ic.Imc
	msg := test.CaptureStdout(func() {
		cli.runCommand([]string{"fet", "key"})
	})
	if !strings.Contains(msg, "ERROR: Unknown command") {
		t.Fatal(msg)
	}
}

func TestCheckCommand(t *testing.T) {
	cli := new(cli)
	cli.commands = make(map[string]*command, 0)
	cli.commandsList = make([]*command, 0)
	cli.initCommands()
	cli.helpInit()

	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cli.immucl = ic.Imc
	l := liner.NewLiner()
	msg := test.CaptureStdout(func() {
		cli.checkCommand([]string{"--help"}, l)
	})
	if len(msg) == 0 {
		t.Fatal("Help is empty")
	}
	msg = test.CaptureStdout(func() {
		cli.checkCommand([]string{"set", "-h"}, l)
	})
	if len(msg) == 0 {
		t.Fatal("Help is empty")
	}

	msg = test.CaptureStdout(func() {
		cli.checkCommand([]string{"met", "-h"}, l)
	})
	if !strings.Contains(msg, "Did you mean this") {
		t.Fatal("Help is empty")
	}
}

func TestCheckCommandErrors(t *testing.T) {
	cli := new(cli)
	require.False(t, cli.checkCommand([]string{"--help"}, nil))
	require.False(t, cli.checkCommand([]string{"help"}, nil))
	require.False(t, cli.checkCommand([]string{"-h"}, nil))
	require.False(t, cli.checkCommand([]string{"clear"}, nil))
	require.True(t, cli.checkCommand([]string{"unknown"}, nil))
}
