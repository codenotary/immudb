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

	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/cobra"
)

func TestGetByIndex(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	cmdl := commandline{
		immucl: login(bs.Dialer),
	}
	cmd := cobra.Command{}
	cmdl.getByIndex(&cmd)
	cmdl.safeset(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"getByIndex", "0"})
	err = cmd.Execute()
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

func TestGetRawBySafeIndex(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	cmdl := commandline{
		immucl: login(bs.Dialer),
	}
	cmd := cobra.Command{}
	cmdl.getRawBySafeIndex(&cmd)
	cmdl.safeset(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"getRawBySafeIndex", "0"})
	err = cmd.Execute()
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

func TestGetKey(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	cmdl := commandline{
		immucl: login(bs.Dialer),
	}
	cmd := cobra.Command{}
	cmdl.getKey(&cmd)
	cmdl.safeset(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"get", "key"})
	err = cmd.Execute()
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

func TestSafeGetKey(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	cmdl := commandline{
		immucl: login(bs.Dialer),
	}
	cmd := cobra.Command{}
	cmdl.safeGetKey(&cmd)
	cmdl.safeset(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"safeget", "key"})
	err = cmd.Execute()
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

func TestRawSafeGetKey(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	cmdl := commandline{
		immucl: login(bs.Dialer),
	}
	cmd := cobra.Command{}
	cmdl.rawSafeGetKey(&cmd)
	cmdl.safeset(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"safeset", "key", "value"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}

	cmd.SetArgs([]string{"rawsafeget", "key"})
	err = cmd.Execute()
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
