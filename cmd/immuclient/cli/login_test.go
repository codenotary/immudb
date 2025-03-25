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

package cli

import (
	"testing"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/stretchr/testify/require"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestLogin(t *testing.T) {
	options := server.DefaultOptions().WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	ts := tokenservice.NewInmemoryTokenService()
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts, client.DefaultOptions().WithDir(t.TempDir()))
	ic.Connect(bs.Dialer)

	cli := new(cli)
	cli.immucl = ic.Imc
	cli.immucl.WithFileTokenService(ts)

	msg, err := cli.login([]string{"immudb"})
	require.NoError(t, err)
	require.Contains(t, msg, "immudb user has the default password", "Login failed")

	msg, err = cli.logout([]string{"immudb"})
	require.NoError(t, err)
	require.Contains(t, msg, "Successfully logged out", "Login failed")
}
