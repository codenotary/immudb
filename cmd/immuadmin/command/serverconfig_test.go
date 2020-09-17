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
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/stretchr/testify/require"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestCommandLine_ServerconfigAuth(t *testing.T) {
	input, _ := ioutil.ReadFile("../../../test/immudb.toml")
	err := ioutil.WriteFile("/tmp/immudb.toml", input, 0644)
	if err != nil {
		panic(err)
	}

	options := server.Options{}.WithAuth(false).WithInMemoryStore(true).WithConfig("/tmp/immudb.toml").WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options()
	cliopt.DialOptions = &dialOptions

	hds := clienttest.DefaultHomedirServiceMock()
	hds.FileExistsInUserHomeDirF = func(string) (bool, error) {
		return true, nil
	}

	cl := &commandline{
		options:        cliopt,
		immuClient:     &scIClientMock{*new(client.ImmuClient)},
		passwordReader: pwReaderMock,
		context:        context.Background(),
		ts:             client.NewTokenService().WithHds(hds).WithTokenFileName("tokenFileName"),
	}

	cmdso, err := cl.NewCmd()
	require.Nil(t, err)
	cl.serverConfig(cmdso)

	b := bytes.NewBufferString("")
	cmdso.SetOut(b)
	cmdso.SetArgs([]string{"set", "auth", "password"})

	// remove ConfigChain method to avoid override options
	cmdso.PersistentPreRunE = nil
	sccmd := cmdso.Commands()[0]
	sccmd.PersistentPreRunE = nil

	cmdso.Execute()
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Server auth config updated")
	os.RemoveAll("/tmp/immudb.toml")
}

func TestCommandLine_ServerconfigMtls(t *testing.T) {
	input, _ := ioutil.ReadFile("../../../test/immudb.toml")
	err := ioutil.WriteFile("/tmp/immudb.toml", input, 0644)
	if err != nil {
		panic(err)
	}
	options := server.Options{}.WithAuth(false).WithInMemoryStore(true).WithConfig("/tmp/immudb.toml").WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options()
	cliopt.DialOptions = &dialOptions

	cl := commandline{
		options:        cliopt,
		immuClient:     &scIClientMock{*new(client.ImmuClient)},
		passwordReader: pwReaderMock,
		context:        context.Background(),
		ts:             client.NewTokenService().WithHds(newHomedirServiceMock()).WithTokenFileName("tokenFileName"),
	}

	cmdso, _ := cl.NewCmd()
	cl.serverConfig(cmdso)

	b := bytes.NewBufferString("")
	cmdso.SetOut(b)
	cmdso.SetArgs([]string{"set", "mtls", "false"})

	// remove ConfigChain method to avoid override options
	cmdso.PersistentPreRunE = nil
	sccmd := cmdso.Commands()[0]
	sccmd.PersistentPreRunE = nil

	cmdso.Execute()
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "Server MTLS config updated")

	os.RemoveAll("/tmp/immudb.toml")
}

type scIClientMock struct {
	client.ImmuClient
}

func (c scIClientMock) UpdateAuthConfig(ctx context.Context, kind auth.Kind) error {
	return nil
}
func (c scIClientMock) UpdateMTLSConfig(ctx context.Context, enabled bool) error {
	return nil
}
func (c scIClientMock) Disconnect() error {
	return nil
}
func (c scIClientMock) Logout(ctx context.Context) error {
	return nil
}

func (c scIClientMock) GetOptions() *client.Options {
	dialOptions := []grpc.DialOption{}
	return &client.Options{
		DialOptions: &dialOptions,
	}
}

func (c scIClientMock) Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error) {
	return &schema.LoginResponse{}, nil
}
