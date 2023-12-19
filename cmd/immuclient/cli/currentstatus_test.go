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

/*
import (
	"github.com/codenotary/immudb/pkg/client"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestCurrentRoot(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
defer bs.Stop()
	ts := tokenservice.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts, client.DefaultOptions().WithDir(t.TempDir()))
ic.
Connect(bs.Dialer)
	ic.Login("immudb")

	cli := new(cli)
	cli.immucl = ic.Imc

	_, err := cli.safeset([]string{"key", "val"})
	assert.NoError(t, err)
	msg, err := cli.currentRoot([]string{""})

	require.NoError(t, err, "CurrentRoot fail")
	require.Contains(t, msg, "hash", "CurrentRoot failed")
}
*/
