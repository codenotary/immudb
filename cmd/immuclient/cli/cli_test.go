/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cli

import (
	"os"
	"path"
	"testing"

	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/codenotary/immudb/pkg/fs"

	"github.com/codenotary/immudb/pkg/client"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/peterh/liner"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	cli := Init(nil)
	require.NotEmpty(t, cli.HelpMessage())
}

func setupTest(t *testing.T) *cli {
	cli := new(cli)
	cli.commands = make(map[string]*command, 0)
	cli.commandsList = make([]*command, 0)
	cli.initCommands()
	cli.helpInit()

	options := server.DefaultOptions().WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	t.Cleanup(func() { bs.Stop() })

	ts := tokenservice.NewInmemoryTokenService()
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts, client.DefaultOptions().WithDir(t.TempDir()))
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cli.immucl = ic.Imc

	return cli
}

func TestRunCommand(t *testing.T) {
	cli := setupTest(t)

	msg := test.CaptureStdout(func() {
		cli.runCommand([]string{"set", "key", "value"})
	})
	require.Contains(t, msg, "value")
}

func TestRunCommandExtraArgs(t *testing.T) {
	cli := setupTest(t)

	msg := test.CaptureStdout(func() {
		cli.runCommand([]string{"set", "key", "value", "value"})
	})
	require.Contains(t, msg, "Redunant argument")
}

func TestRunMissingArgs(t *testing.T) {
	cli := setupTest(t)

	msg := test.CaptureStdout(func() {
		cli.runCommand([]string{"set", "key"})
	})
	require.Contains(t, msg, "Not enough arguments")
}

func TestRunWrongCommand(t *testing.T) {
	cli := setupTest(t)

	msg := test.CaptureStdout(func() {
		cli.runCommand([]string{"fet", "key"})
	})
	require.Contains(t, msg, "ERROR: Unknown command")
}

func TestCheckCommand(t *testing.T) {
	cli := setupTest(t)

	l := liner.NewLiner()
	msg := test.CaptureStdout(func() {
		cli.checkCommand([]string{"--help"}, l)
	})
	require.NotEmpty(t, msg, "Help must not be empty")

	msg = test.CaptureStdout(func() {
		cli.checkCommand([]string{"set", "-h"}, l)
	})
	require.NotEmpty(t, msg, "Help must not be empty")

	msg = test.CaptureStdout(func() {
		cli.checkCommand([]string{"met", "-h"}, l)
	})
	require.Contains(t, msg, "Did you mean this", "Help is empty")
}

func TestCheckCommandErrors(t *testing.T) {
	cli := new(cli)
	require.False(t, cli.checkCommand([]string{"--help"}, nil))
	require.False(t, cli.checkCommand([]string{"help"}, nil))
	require.False(t, cli.checkCommand([]string{"-h"}, nil))
	require.False(t, cli.checkCommand([]string{"clear"}, nil))
	require.True(t, cli.checkCommand([]string{"unknown"}, nil))
}

func TestImmuClient_BackupAndRestoreUX(t *testing.T) {
	stateFileDir := path.Join(t.TempDir(), "testStates")
	dir := path.Join(t.TempDir(), "data")
	dirAtTx3 := path.Join(t.TempDir(), "dataTx3")

	options := server.DefaultOptions().WithDir(dir)
	bs := servertest.NewBufconnServer(options)
	uuid := bs.GetUUID()

	err := bs.Start()
	require.NoError(t, err)

	cliOpts := client.
		DefaultOptions().
		WithDir(stateFileDir)
	cliOpts.CurrentDatabase = client.DefaultDB
	ts := tokenservice.NewInmemoryTokenService()

	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts, cliOpts)

	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cl := new(cli)
	cl.immucl = ic.Imc

	_, err = cl.safeset([]string{"key1", "val"})
	require.NoError(t, err)

	_, err = cl.safeset([]string{"key2", "val"})
	require.NoError(t, err)

	_, err = cl.safeset([]string{"key3", "val"})
	require.NoError(t, err)

	err = bs.Stop()
	require.NoError(t, err)

	copier := fs.NewStandardCopier()
	err = copier.CopyDir(dir, dirAtTx3)
	require.NoError(t, err)

	bs = servertest.NewBufconnServer(options)
	require.NoError(t, err)
	bs.SetUUID(uuid)
	err = bs.Start()
	require.NoError(t, err)

	cliOpts = client.
		DefaultOptions().
		WithDir(stateFileDir)
	cliOpts.CurrentDatabase = client.DefaultDB
	ic = test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts, cliOpts)

	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cl = new(cli)
	cl.immucl = ic.Imc

	_, err = cl.safeset([]string{"key1", "val"})
	require.NoError(t, err)

	_, err = cl.safeset([]string{"key2", "val"})
	require.NoError(t, err)

	_, err = cl.safeset([]string{"key3", "val"})
	require.NoError(t, err)

	err = bs.Stop()
	require.NoError(t, err)

	os.RemoveAll(dir)
	err = copier.CopyDir(dirAtTx3, dir)
	require.NoError(t, err)

	bs = servertest.NewBufconnServer(options)
	require.NoError(t, err)
	bs.SetUUID(uuid)
	err = bs.Start()
	require.NoError(t, err)

	cliOpts = client.
		DefaultOptions().
		WithDir(stateFileDir)
	cliOpts.CurrentDatabase = client.DefaultDB
	ic = test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts, cliOpts)

	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cl = new(cli)
	cl.immucl = ic.Imc

	_, err = cl.safeGetKey([]string{"key3"})

	require.Equal(t, client.ErrServerStateIsOlder, err)
}
