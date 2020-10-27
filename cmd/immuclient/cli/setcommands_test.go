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
	"os"
	"strings"
	"testing"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestRawSafeSet(t *testing.T) {
	defer os.Remove(".root-")
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	cli := new(cli)
	cli.immucl = ic.Imc
	msg, err := cli.rawSafeSet([]string{"key", "val"})
	if err != nil {
		t.Fatal("RawSafeSet fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("RawSafeSet failed: %s", msg)
	}
}

func TestSet(t *testing.T) {
	defer os.Remove(".root-")
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
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
	defer os.Remove(".root-")
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
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
	defer os.Remove(".root-")
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
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
	defer os.Remove(".root-")
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
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
