/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package immuclient

/*
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

func TestRawSafeSet(t *testing.T) {
	defer os.Remove(".root-")
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
defer bs.Stop()

	ts := tokenservice.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
ic.
Connect(bs.Dialer)
	ic.Login("immudb")

	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.rawSafeSet(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"rawsafeset", "key", "value"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd := cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	err := cmd.Execute()

	require.NoError(t, err)
	msg, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(msg), "hash")
}

func TestSet(t *testing.T) {
	defer os.Remove(".root-")
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
defer bs.Stop()

	ts := tokenservice.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
ic.
Connect(bs.Dialer)
	ic.Login("immudb")

	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.set(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"set", "key", "value"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd := cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	err := cmd.Execute()

	require.NoError(t, err)
	msg, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(msg), "hash")
}

func TestSafeset(t *testing.T) {
	defer os.Remove(".root-")
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
defer bs.Stop()

	ts := tokenservice.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
ic.
Connect(bs.Dialer)
	ic.Login("immudb")

	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.safeset(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"safeset", "key", "value"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd := cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	err := cmd.Execute()

	require.NoError(t, err)
	msg, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(msg), "hash")
}

func TestZAdd(t *testing.T) {
	defer os.Remove(".root-")
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
defer bs.Stop()

	ts := tokenservice.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
ic.
Connect(bs.Dialer)
	ic.Login("immudb")

	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.zAdd(cmd)
	cmdl.safeset(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd := cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil
	innercmd.PersistentPostRun = nil

	err := cmd.Execute()

	require.NoError(t, err)

	cmd.SetArgs([]string{"zadd", "name", "1", "key"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[2]
	innercmd.PersistentPreRunE = nil

	err = cmd.Execute()
	require.NoError(t, err)
	msg, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(msg), "score")
}

func TestSafeZAdd(t *testing.T) {
	defer os.Remove(".root-")
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
defer bs.Stop()

	ts := tokenservice.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
ic.
Connect(bs.Dialer)
	ic.Login("immudb")

	cmdl := commandline{
		config: helper.Config{Name: "immuclient"},
		immucl: ic.Imc,
	}
	cmd, _ := cmdl.NewCmd()
	cmdl.safeZAdd(cmd)
	cmdl.safeset(cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd := cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil
	// since we issue two commands we need to remove PersistentPostRun ( disconnect )
	innercmd.PersistentPostRun = nil

	err := cmd.Execute()

	require.NoError(t, err)

	cmd.SetArgs([]string{"safezadd", "name", "1", "key"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[2]
	innercmd.PersistentPreRunE = nil

	err = cmd.Execute()
	require.NoError(t, err)
	msg, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(msg), "score")
}
*/
