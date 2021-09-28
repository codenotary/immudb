/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"os"
	"strings"
	"testing"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestSet(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := tokenservice.NewFileTokenService().WithTokenFileName("testTokenFile").
		WithHds(&test.HomedirServiceMock{Token: tokenservice.BuildToken("database", "fakeToken")})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts).WithOptions(client.DefaultOptions())
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cli := new(cli)
	cli.immucl = ic.Imc
	msg, err := cli.set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("Set failed: %s", msg)
	}
}

func TestSafeSet(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := tokenservice.NewFileTokenService().WithTokenFileName("testTokenFile").
		WithHds(&test.HomedirServiceMock{Token: tokenservice.BuildToken("database", "fakeToken")})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts).WithOptions(client.DefaultOptions())
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cli := new(cli)
	cli.immucl = ic.Imc
	msg, err := cli.safeset([]string{"key", "val"})
	if err != nil {
		t.Fatal("SafeSet fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("SafeSet failed: %s", msg)
	}
}

func TestZAdd(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := tokenservice.NewFileTokenService().WithTokenFileName("testTokenFile").
		WithHds(&test.HomedirServiceMock{Token: tokenservice.BuildToken("database", "fakeToken")})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts).WithOptions(client.DefaultOptions())
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cli := new(cli)
	cli.immucl = ic.Imc
	_, _ = cli.safeset([]string{"key", "val"})

	msg, err := cli.zAdd([]string{"val", "1", "key"})

	if err != nil {
		t.Fatal("ZAdd fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("ZAdd failed: %s", msg)
	}
}

func TestSafeZAdd(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := tokenservice.NewFileTokenService().WithTokenFileName("testTokenFile").
		WithHds(&test.HomedirServiceMock{Token: tokenservice.BuildToken("database", "fakeToken")})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts).WithOptions(client.DefaultOptions())
	ic.
		Connect(bs.Dialer)
	ic.Login("immudb")

	cli := new(cli)
	cli.immucl = ic.Imc
	_, _ = cli.safeset([]string{"key", "val"})

	msg, err := cli.safeZAdd([]string{"val", "1", "key"})

	if err != nil {
		t.Fatal("SafeZAdd fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("SafeZAdd failed: %s", msg)
	}
}
