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
	"strings"
	"testing"

	test "github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestConsistency(t *testing.T) {
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
	hash := strings.Split(msg, "hash:		")[1]
	hash = hash[:64]
	msg, err = cli.consistency([]string{"0", hash})
	if err != nil {
		t.Fatal("Consistency fail", err)
	}
	if !strings.Contains(msg, "secondRoot") {
		t.Fatalf("Set failed: %s", msg)
	}

	msg, err = ic.Imc.Consistency([]string{"0"})
	if err == nil {
		t.Fatal("Consistency fail expected error")
	}
}
func TestInclusion(t *testing.T) {
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
	hash := strings.Split(msg, "hash:		")[1]
	hash = hash[:64]
	msg, err = cli.inclusion([]string{"0", hash})
	if err != nil {
		t.Fatal("Consistency fail", err)
	}
	if !strings.Contains(msg, "root") {
		t.Fatalf("Set failed: %s", msg)
	}

	msg, err = ic.Imc.Inclusion([]string{"0"})
	if err != nil {
		t.Fatal("Consistency fail", err)
	}
	if !strings.Contains(msg, "root") {
		t.Fatalf("Set failed: %s", msg)
	}
}
