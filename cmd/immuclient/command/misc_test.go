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

func TestHistory(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	cmd := cobra.Command{}
	cmdl.history(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmdl.immucl.SafeSet([]string{"key", "value"})

	cmd.SetArgs([]string{"history", "key"})
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
func TestStatus(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	cmd := cobra.Command{}
	cmdl.status(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmdl.immucl.SafeSet([]string{"key", "value"})

	cmd.SetArgs([]string{"status"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "Health check OK") {
		t.Fatal(err)
	}
}

func TestUserList(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	cmd := cobra.Command{}
	cmdl.user(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "list"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "immudb") {
		t.Fatal(err)
	}
}
func TestUserChangePassword(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, hds := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	cmdl.immucl = immuclienttest.NewClient(&immuclienttest.PasswordReader{
		Pass: []string{"immudb", "MyUser@9", "MyUser@9"},
	}, bs.Dialer, hds)
	cmd := cobra.Command{}
	cmdl.user(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "changepassword", "immudb"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "Password of immudb was changed successfuly") {
		t.Fatal(err)
	}
}

func TestUserCreate(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, hds := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	cmdl.immucl = immuclienttest.NewClient(&immuclienttest.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}, bs.Dialer, hds)
	cmd := cobra.Command{}
	cmdl.user(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "create", "newuser", "readwrite", "defaultdb"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "Password of immudb was changed successfuly") {
		t.Fatal(err)
	}
}

func TestUserActivate(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, hds := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}

	imc = immuclienttest.NewClient(&immuclienttest.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}, bs.Dialer, hds)
	_, err := imc.CreateDatabase([]string{"mydb"})
	_, err = imc.UserCreate([]string{"myuser", "readwrite", "mydb"})
	if err != nil {
		t.Fatal("TestUserCreate fail", err)
	}

	cmd := cobra.Command{}
	cmdl.user(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "activate", "myuser"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "User status changed successfully") {
		t.Fatal(err)
	}
}

func TestUserDeactivate(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, hds := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}

	imc = immuclienttest.NewClient(&immuclienttest.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}, bs.Dialer, hds)
	_, err := imc.CreateDatabase([]string{"mydb"})
	_, err = imc.UserCreate([]string{"myuser", "readwrite", "mydb"})
	if err != nil {
		t.Fatal("TestUserCreate fail", err)
	}

	cmd := cobra.Command{}
	cmdl.user(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "deactivate", "myuser"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "User status changed successfully") {
		t.Fatal(err)
	}
}
func TestUserPermission(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, hds := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}

	imc = immuclienttest.NewClient(&immuclienttest.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}, bs.Dialer, hds)
	_, err := imc.CreateDatabase([]string{"mydb"})
	_, err = imc.UserCreate([]string{"myuser", "readwrite", "mydb"})
	if err != nil {
		t.Fatal("TestUserCreate fail", err)
	}

	cmd := cobra.Command{}
	cmdl.user(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "permission", "grant", "readwrite", "myuser", "mydb"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "User status changed successfully") {
		t.Fatal(err)
	}
}

func TestDatabaseList(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}

	cmd := cobra.Command{}
	cmdl.database(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"database", "list"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "defaultdb") {
		t.Fatal(err)
	}
}
func TestDatabaseCreate(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}

	cmd := cobra.Command{}
	cmdl.database(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"database", "create", "mynewdb"})
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "mynewdb") {
		t.Fatal(string(msg))
	}
}

func TestUseDatabase(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc, _ := immuclienttest.Login("immudb", "immudb", bs.Dialer)
	cmdl := commandline{
		immucl: imc,
	}
	_, err := imc.CreateDatabase([]string{"mynewdb"})
	cmd := cobra.Command{}
	cmdl.use(&cmd)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"use", "mynewdb"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "mynewdb") {
		t.Fatal(string(msg))
	}
}
