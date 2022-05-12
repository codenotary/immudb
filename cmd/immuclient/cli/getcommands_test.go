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
	}, ts)
ic.
Connect(bs.Dialer)
	ic.Login("immudb")

	cli := new(cli)
	cli.immucl = ic.Imc

	_, _ = cli.safeset([]string{"key", "val"})
	msg, err := cli.getByIndex([]string{"0"})
	if err != nil {
		t.Fatal("GetByIndex fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("GetByIndex failed: %s", msg)
	}
}

func TestGetKey(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
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

	cli := new(cli)
	cli.immucl = ic.Imc

	_, _ = cli.set([]string{"key", "val"})
	msg, err := cli.getKey([]string{"key"})
	if err != nil {
		t.Fatal("GetKey fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("GetKey failed: %s", msg)
	}
}
func TestRawSafeGetKey(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
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

	cli := new(cli)
	cli.immucl = ic.Imc

	_, _ = cli.set([]string{"key", "val"})
	msg, err := cli.rawSafeGetKey([]string{"key"})
	if err != nil {
		t.Fatal("RawSafeGetKey fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("RawSafeGetKey failed: %s", msg)
	}
}
func TestSafeGetKey(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
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

	cli := new(cli)
	cli.immucl = ic.Imc

	_, _ = cli.set([]string{"key", "val"})
	msg, err := cli.safeGetKey([]string{"key"})
	if err != nil {
		t.Fatal("SafeGetKey fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("SafeGetKey failed: %s", msg)
	}
}

func TestGetRawBySafeIndex(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
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

	cli := new(cli)
	cli.immucl = ic.Imc

	_, _ = cli.set([]string{"key", "val"})
	msg, err := cli.getRawBySafeIndex([]string{"0"})
	if err != nil {
		t.Fatal("GetRawBySafeIndex fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("GetRawBySafeIndex failed: %s", msg)
	}
}
*/
