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

import (
	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"os"
	"strings"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestLogin(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithAdminPassword(auth.SysAdminPassword)
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

	cli := new(cli)
	cli.immucl = ic.Imc
	cli.immucl.WithFileTokenService(ts)

	msg, err := cli.login([]string{"immudb"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, "immudb user has the default password") {
		t.Fatalf("Login failed: %s", err)
	}

	msg, err = cli.logout([]string{"immudb"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg, "Successfully logged out") {
		t.Fatalf("Login failed: %s", err)
	}
}
