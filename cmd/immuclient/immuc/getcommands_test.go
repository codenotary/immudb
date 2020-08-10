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

package immuc_test

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

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	_, _ = ic.Imc.SafeSet([]string{"key", "val"})
	msg, err := ic.Imc.GetByIndex([]string{"0"})
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

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	_, _ = ic.Imc.Set([]string{"key", "val"})
	msg, err := ic.Imc.GetKey([]string{"key"})
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

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	_, _ = ic.Imc.Set([]string{"key", "val"})
	msg, err := ic.Imc.RawSafeGetKey([]string{"key"})
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

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	_, _ = ic.Imc.Set([]string{"key", "val"})
	msg, err := ic.Imc.SafeGetKey([]string{"key"})
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

	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(&test.HomedirServiceMock{})
	ic := test.NewClientTest(&test.PasswordReader{
		Pass: []string{"immudb"},
	}, ts)
	ic.Connect(bs.Dialer)
	ic.Login("immudb")

	_, _ = ic.Imc.Set([]string{"key", "val"})
	msg, err := ic.Imc.GetRawBySafeIndex([]string{"0"})
	if err != nil {
		t.Fatal("GetRawBySafeIndex fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("GetRawBySafeIndex failed: %s", msg)
	}
}
