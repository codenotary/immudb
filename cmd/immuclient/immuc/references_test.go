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

func TestReference(t *testing.T) {
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

	_, _ = ic.Imc.Set([]string{"key", "val"})

	msg, err := ic.Imc.SetReference([]string{"val", "key"})
	if err != nil {
		t.Fatal("Reference fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("Reference failed: %s", msg)
	}
}

func _TestVerifiedSetReference(t *testing.T) {
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

	_, _ = ic.Imc.Set([]string{"key", "val"})

	msg, err := ic.Imc.VerifiedSetReference([]string{"val", "key"})
	if err != nil {
		t.Fatal("SafeReference fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("SafeReference failed: %s", msg)
	}
}
