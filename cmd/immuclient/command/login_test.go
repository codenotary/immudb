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

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
)

func TestLogin(t *testing.T) {
	options := server.DefaultOptions().WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	t.Cleanup(func() { bs.Stop() })

	ts := tokenservice.NewInmemoryTokenService()
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts, client.DefaultOptions().WithDir(t.TempDir()))

	ic.Connect(bs.Dialer)

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
	require.NoError(t, err)

	msg, err := ioutil.ReadAll(b)
	require.NoError(t, err)
	require.Contains(t, string(msg), "Successfully logged in")

	cmd, err = cmdl.NewCmd()
	require.NoError(t, err)

	cmdl.logout(cmd)
	cmd.SetOut(b)
	cmd.SetArgs([]string{"logout"})

	// remove ConfigChain method to avoid options override
	cmd.PersistentPreRunE = nil
	innercmd = cmd.Commands()[0]
	innercmd.PersistentPreRunE = nil

	err = cmd.Execute()
	require.NoError(t, err)

	_, err = ioutil.ReadAll(b)
	require.NoError(t, err)
}
