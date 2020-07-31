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

package immuadmin

import (
	"bytes"
	"context"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func TestUserList(t *testing.T) {
	bs := servertest.NewBufconnServer(server.DefaultOptions().WithAuth(true).WithInMemoryStore(true))
	bs.Start()

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb"},
	}
	hds := &immuclienttest.HomedirServiceMock{}
	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).
		WithHomedirService(hds)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}
	if err = hds.WriteFileToUserHomeDir(token.Token, ""); err != nil {
		t.Fatal(err)
	}

	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
		hds:            hds,
	}

	cmd := cobra.Command{}
	cmdl.user(&cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "list"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "immudb") {
		t.Fatal(msg)
	}
}

func TestUserChangePassword(t *testing.T) {
	bs := servertest.NewBufconnServer(server.DefaultOptions().WithAuth(true).WithInMemoryStore(true))
	bs.Start()

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb", "MyUser@9", "MyUser@9"},
	}
	hds := &immuclienttest.HomedirServiceMock{}
	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).
		WithHomedirService(hds)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}
	if err = hds.WriteFileToUserHomeDir(token.Token, ""); err != nil {
		t.Fatal(err)
	}

	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
		hds:            hds,
	}

	cmd := cobra.Command{}
	cmdl.user(&cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "changepassword", "immudb"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "Password of immudb was changed successfuly") {
		t.Fatal(msg)
	}
}

func TestUserCreate(t *testing.T) {
	bs := servertest.NewBufconnServer(server.DefaultOptions().WithAuth(true).WithInMemoryStore(true))
	bs.Start()

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb"},
	}
	hds := &immuclienttest.HomedirServiceMock{}
	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).
		WithHomedirService(hds)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}
	if err = hds.WriteFileToUserHomeDir(token.Token, ""); err != nil {
		t.Fatal(err)
	}

	pr = &immuclienttest.PasswordReader{
		Pass: []string{"MyUser@9", "MyUser@9"},
	}
	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
		hds:            hds,
	}

	cmd := cobra.Command{}
	cmdl.user(&cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "create", "newuser", "readwrite", "defaultdb"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "Created user newuser") {
		t.Fatal(msg)
	}
}

func TestUserActivate(t *testing.T) {
	bs := servertest.NewBufconnServer(server.DefaultOptions().WithAuth(true).WithInMemoryStore(true))
	bs.Start()

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb", "MyUser@9", "MyUser@9"},
	}
	hds := &immuclienttest.HomedirServiceMock{}
	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).
		WithHomedirService(hds)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}
	if err = hds.WriteFileToUserHomeDir(token.Token, ""); err != nil {
		t.Fatal(err)
	}
	clientb, _ = client.NewImmuClient(cliopt)
	err = clientb.CreateDatabase(ctx, &schema.Database{
		Databasename: "mydb",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = clientb.CreateUser(ctx, []byte("myuser"), []byte("MyUser@9"), auth.PermissionAdmin, "defaultdb")
	if err != nil {
		t.Fatal(err)
	}
	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
		hds:            hds,
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
		t.Fatal(string(msg))
	}
}

func TestUserDeactivate(t *testing.T) {
	bs := servertest.NewBufconnServer(server.DefaultOptions().WithAuth(true).WithInMemoryStore(true))
	bs.Start()

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb", "MyUser@9", "MyUser@9"},
	}
	hds := &immuclienttest.HomedirServiceMock{}
	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).
		WithHomedirService(hds)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}
	if err = hds.WriteFileToUserHomeDir(token.Token, ""); err != nil {
		t.Fatal(err)
	}
	clientb, _ = client.NewImmuClient(cliopt)
	err = clientb.CreateDatabase(ctx, &schema.Database{
		Databasename: "mydb",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = clientb.CreateUser(ctx, []byte("myuser"), []byte("MyUser@9"), auth.PermissionAdmin, "defaultdb")
	if err != nil {
		t.Fatal(err)
	}
	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
		hds:            hds,
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
		t.Fatal(string(msg))
	}
}

func TestUserPermission(t *testing.T) {
	bs := servertest.NewBufconnServer(server.DefaultOptions().WithAuth(true).WithInMemoryStore(true))
	bs.Start()

	pr := &immuclienttest.PasswordReader{
		Pass: []string{"immudb", "MyUser@9", "MyUser@9"},
	}
	hds := &immuclienttest.HomedirServiceMock{}
	ctx := context.Background()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(&dialOptions).WithPasswordReader(pr).
		WithHomedirService(hds)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions
	clientb, _ := client.NewImmuClient(cliopt)
	token, err := clientb.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}
	if err = hds.WriteFileToUserHomeDir(token.Token, ""); err != nil {
		t.Fatal(err)
	}
	clientb, _ = client.NewImmuClient(cliopt)
	err = clientb.CreateDatabase(ctx, &schema.Database{
		Databasename: "mydb",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = clientb.CreateUser(ctx, []byte("myuser"), []byte("MyUser@9"), auth.PermissionAdmin, "defaultdb")
	if err != nil {
		t.Fatal(err)
	}
	cmdl := commandline{
		options:        cliopt,
		immuClient:     clientb,
		passwordReader: pr,
		context:        ctx,
		hds:            hds,
	}

	cmd := cobra.Command{}
	cmdl.user(&cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cmd.SetArgs([]string{"user", "permission", "grant", "myuser", "readwrite", "mydb"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), "Permission changed successfully") {
		t.Fatal(string(msg))
	}
}
