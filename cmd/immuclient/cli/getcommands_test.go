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

/*
import (
	"github.com/codenotary/immudb/pkg/client"
	"strings"
	"testing"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestGetByIndex(t *testing.T) {
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

	_, _ = cli.safeset([]string{"key", "val"})
	msg, err := cli.getByIndex([]string{"0"})
	require.NoError(t, err, "GetByIndex fail")
	require.Contains(t, msg, "hash", "GetByIndex failed")
}

func TestGetKey(t *testing.T) {
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

	_, _ = cli.set([]string{"key", "val"})
	msg, err := cli.getKey([]string{"key"})
	require.NoError(t, err, "GetKey fail")
	require.Contains(t, msg, "hash", "GetKey failed")
}
func TestRawSafeGetKey(t *testing.T) {
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

	_, _ = cli.set([]string{"key", "val"})
	msg, err := cli.rawSafeGetKey([]string{"key"})
	require.NoError(t, err, "RawSafeGetKey fail")
	require.Contains(t, msg, "hash", "RawSafeGetKey failed")
}
func TestSafeGetKey(t *testing.T) {
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

	_, _ = cli.set([]string{"key", "val"})
	msg, err := cli.safeGetKey([]string{"key"})
	require.NoError(t, err, "SafeGetKey fail")
	require.Contains(t, msg, "hash", "SafeGetKey failed")
}

func TestGetRawBySafeIndex(t *testing.T) {
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

	_, _ = cli.set([]string{"key", "val"})
	msg, err := cli.getRawBySafeIndex([]string{"0"})
	require.NoError(t, err, "GetRawBySafeIndex fail")
	require.Contains(t, msg, "hash", "GetRawBySafeIndex failed")
}
*/
