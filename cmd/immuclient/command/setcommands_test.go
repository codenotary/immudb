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

package immuclient

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/cobra"
)

func TestRawSafeSet(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	cmd := cobra.Command{}
	cmdl.rawSafeSet(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"rawsafeset", "key", "value"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "hash") {
		t.Fatal(err)
	}
}

func TestSet(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	cmd := cobra.Command{}
	cmdl.set(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"set", "key", "value"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "hash") {
		t.Fatal(err)
	}
}

func TestSafeset(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	cmd := cobra.Command{}
	cmdl.safeset(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"safeset", "key", "value"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "hash") {
		t.Fatal(err)
	}
}

func TestZAdd(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	cmd := cobra.Command{}
	cmdl.zAdd(&cmd)
	cmdl.safeset(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"zadd", "name", "1", "key"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "score") {
		t.Fatal(err)
	}
}

func TestSafeZAdd(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	cmd := cobra.Command{}
	cmdl.safeZAdd(&cmd)
	cmdl.safeset(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"safezadd", "name", "1", "key"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "score") {
		t.Fatal(err)
	}
}
