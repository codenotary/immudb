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
	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
)

func TestGetTxByID(t *testing.T) {

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

	_, _ = ic.Imc.VerifiedSet([]string{"key", "val"})

	msg, err := ic.Imc.GetTxByID([]string{"1"})
	if err != nil {
		t.Fatal("GetByIndex fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("GetByIndex failed: %s", msg)
	}
}
func TestGet(t *testing.T) {
	defer os.Remove(".state")
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
	msg, err := ic.Imc.Get([]string{"key"})
	if err != nil {
		t.Fatal("GetKey fail", err)
	}
	if !strings.Contains(msg, "value") {
		t.Fatalf("GetKey failed: %s", msg)
	}
}

func TestVerifiedGet(t *testing.T) {
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
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	_, _ = ic.Imc.Set([]string{"key", "val"})
	msg, err := ic.Imc.VerifiedGet([]string{"key"})
	if err != nil {
		t.Fatal("VerifiedGet fail", err)
	}
	if !strings.Contains(msg, "value") {
		t.Fatalf("VerifiedGet failed: %s", msg)
	}
}

func TestGetByRevision(t *testing.T) {

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

	_, err := ic.Imc.Set([]string{"key", "value1"})
	require.NoError(t, err)

	_, err = ic.Imc.Set([]string{"key", "value2"})
	require.NoError(t, err)

	_, err = ic.Imc.Set([]string{"key", "value3"})
	require.NoError(t, err)

	msg, err := ic.Imc.Get([]string{"key@1"})
	require.NoError(t, err)
	require.Contains(t, msg, "value1")

	msg, err = ic.Imc.Get([]string{"key@2"})
	require.NoError(t, err)
	require.Contains(t, msg, "value2")

	msg, err = ic.Imc.Get([]string{"key@3"})
	require.NoError(t, err)
	require.Contains(t, msg, "value3")

	msg, err = ic.Imc.Get([]string{"key@0"})
	require.NoError(t, err)
	require.Contains(t, msg, "value3")

	msg, err = ic.Imc.Get([]string{"key@-0"})
	require.NoError(t, err)
	require.Contains(t, msg, "value3")

	msg, err = ic.Imc.Get([]string{"key@-1"})
	require.NoError(t, err)
	require.Contains(t, msg, "value2")

	msg, err = ic.Imc.Get([]string{"key@-2"})
	require.NoError(t, err)
	require.Contains(t, msg, "value1")

	msg, err = ic.Imc.Get([]string{"key@notarevision"})
	require.Error(t, err)
}
