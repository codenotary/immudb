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

package immuc_test

import (
	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"os"
	"strings"
	"testing"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestZScan(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	_, err := ic.Imc.Set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}

	_, err = ic.Imc.ZAdd([]string{"set", "10.5", "key"})
	if err != nil {
		t.Fatal("ZAdd fail", err)
	}

	msg, err := ic.Imc.ZScan([]string{"set"})
	if err != nil {
		t.Fatal("ZScan fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("ZScan failed: %s", msg)
	}
}

func TestIScan(t *testing.T) {
	defer os.Remove(".state-")
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.
		Connect(bs.Dialer)
	ic.Login("immudb")
	_, err := ic.Imc.VerifiedSet([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}
}

func TestScan(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.
		Connect(bs.Dialer)
	ic.Login("immudb")
	_, err := ic.Imc.Set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}

	msg, err := ic.Imc.Scan([]string{"k"})
	if err != nil {
		t.Fatal("Scan fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("Scan failed: %s", msg)
	}
}

func _TestCount(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.
		Connect(bs.Dialer)
	ic.Login("immudb")

	_, err := ic.Imc.Set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}

	msg, err := ic.Imc.Count([]string{"key"})
	if err != nil {
		t.Fatal("Count fail", err)
	}
	if !strings.Contains(msg, "1") {
		t.Fatalf("Count failed: %s", msg)
	}
}
