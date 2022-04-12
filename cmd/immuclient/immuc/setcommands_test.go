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

package immuc_test

import (
	"os"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/codenotary/immudb/pkg/client/tokenservice"

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

	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf)
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.
		Connect(bs.Dialer)
	ic.Login("immudb")

	msg, err := ic.Imc.Set([]string{"key", "val"})

	if err != nil {
		t.Fatal("Set fail", err)
	}
	if !strings.Contains(msg, "value") {
		t.Fatalf("Set failed: %s", msg)
	}
}
func TestVerifiedSet(t *testing.T) {
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

	msg, err := ic.Imc.VerifiedSet([]string{"key", "val"})

	if err != nil {
		t.Fatal("VerifiedSet fail", err)
	}
	if !strings.Contains(msg, "value") {
		t.Fatalf("VerifiedSet failed: %s", msg)
	}
}
func TestZAdd(t *testing.T) {
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

	_, _ = ic.Imc.VerifiedSet([]string{"key", "val"})

	msg, err := ic.Imc.ZAdd([]string{"val", "1", "key"})

	if err != nil {
		t.Fatal("ZAdd fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("ZAdd failed: %s", msg)
	}
}
func _TestVerifiedZAdd(t *testing.T) {
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

	_, _ = ic.Imc.VerifiedSet([]string{"key", "val"})

	msg, err := ic.Imc.VerifiedZAdd([]string{"val", "1", "key"})

	if err != nil {
		t.Fatal("VerifiedZAdd fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("VerifiedZAdd failed: %s", msg)
	}
}
func TestCreateDatabase(t *testing.T) {
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

	msg, err := ic.Imc.CreateDatabase([]string{"newdb"})
	if err != nil {
		t.Fatal("CreateDatabase fail", err)
	}
	if !strings.Contains(msg, "database successfully created") {
		t.Fatalf("CreateDatabase failed: %s", msg)
	}

	_, err = ic.Imc.DatabaseList([]string{})
	if err != nil {
		t.Fatal("DatabaseList fail", err)
	}

	msg, err = ic.Imc.UseDatabase([]string{"newdb"})
	if err != nil {
		t.Fatal("UseDatabase fail", err)
	}
	if !strings.Contains(msg, "newdb") {
		t.Fatalf("UseDatabase failed: %s", msg)
	}
}
