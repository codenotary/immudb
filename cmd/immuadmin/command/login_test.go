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
	"testing"

	"github.com/codenotary/immudb/cmd/helper"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/clienttest"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/prometheus/common/log"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

var pwReaderCounter = 0
var pwReaderMock = &clienttest.PasswordReaderMock{
	ReadF: func(msg string) ([]byte, error) {
		var pw []byte
		if pwReaderCounter == 0 {
			pw = []byte(`immudb`)
		} else {
			pw = []byte(`Passw0rd!-`)
		}
		pwReaderCounter++
		return pw, nil
	},
}

func TestCommandLine_Connect(t *testing.T) {
	log.Info("TestCommandLine_Connect")
	bs := servertest.NewBufconnServer(server.Options{}.WithAuth(false).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword))
	bs.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	opts := Options()
	opts.DialOptions = &dialOptions
	cmdl := commandline{
		context: context.Background(),
		options: opts,
	}
	err := cmdl.connect(&cobra.Command{}, []string{})
	assert.Nil(t, err)
}

func TestCommandLine_Disconnect(t *testing.T) {
	log.Info("TestCommandLine_Disconnect")
	bs := servertest.NewBufconnServer(server.Options{}.WithAuth(false).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword))
	bs.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options()
	cliopt.DialOptions = &dialOptions
	cmdl := commandline{
		options:        cliopt,
		immuClient:     &scIClientMock{*new(client.ImmuClient)},
		passwordReader: pwReaderMock,
		context:        context.Background(),
		ts:             client.NewTokenService().WithHds(newHomedirServiceMock()).WithTokenFileName("token_admin"),
	}
	_ = cmdl.connect(&cobra.Command{}, []string{})

	cmdl.disconnect(&cobra.Command{}, []string{})

	err := cmdl.immuClient.Disconnect()
	assert.Errorf(t, err, "not connected")
}

type scIClientInnerMock struct {
	cliop *client.Options
	client.ImmuClient
}

func (c scIClientInnerMock) UpdateAuthConfig(ctx context.Context, kind auth.Kind) error {
	return nil
}
func (c scIClientInnerMock) UpdateMTLSConfig(ctx context.Context, enabled bool) error {
	return nil
}
func (c scIClientInnerMock) Disconnect() error {
	return nil
}

func (c scIClientInnerMock) GetOptions() *client.Options {
	return c.cliop
}

func (c scIClientInnerMock) Login(ctx context.Context, user []byte, pass []byte) (*schema.LoginResponse, error) {
	return &schema.LoginResponse{}, nil
}

func TestCommandLine_LoginLogout(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	cl := commandline{}
	cmd, _ := cl.NewCmd()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	cliopt := Options().WithDialOptions(&dialOptions)

	cliopt.Tkns = client.NewTokenService().WithHds(client.NewHomedirService()).WithTokenFileName("token_admin")
	cmdl := commandline{
		config:         helper.Config{Name: "immuadmin"},
		options:        cliopt,
		immuClient:     &scIClientInnerMock{cliopt, *new(client.ImmuClient)},
		passwordReader: pwReaderMock,
		context:        context.Background(),
		ts:             client.NewTokenService().WithHds(client.NewHomedirService()).WithTokenFileName("token_admin"),
		newImmuClient:  client.NewImmuClient,
	}
	cmdl.login(cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"login", "immudb"})

	// remove ConfigChain method to avoid override options
	cmd.PersistentPreRunE = nil
	logincmd := cmd.Commands()[0]
	logincmd.PersistentPreRunE = nil

	cmd.Execute()
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "logged in")

	cmdlo := commandline{
		config:         helper.Config{Name: "immuadmin"},
		options:        cliopt,
		immuClient:     &scIClientMock{*new(client.ImmuClient)},
		passwordReader: pwReaderMock,
		context:        context.Background(),
		ts:             client.NewTokenService().WithHds(client.NewHomedirService()).WithTokenFileName("token_admin"),
	}
	b1 := bytes.NewBufferString("")
	cl = commandline{}
	logoutcmd, _ := cl.NewCmd()
	logoutcmd.SetOut(b1)
	logoutcmd.SetArgs([]string{"logout"})

	cmdlo.logout(logoutcmd)

	// remove ConfigChain method to avoid override options
	logoutcmd.PersistentPreRunE = nil
	logoutcmdin := logoutcmd.Commands()[0]
	logoutcmdin.PersistentPreRunE = nil

	logoutcmd.Execute()
	out1, err1 := ioutil.ReadAll(b1)
	if err1 != nil {
		t.Fatal(err1)
	}
	assert.Contains(t, string(out1), "logged out")
}

func TestCommandLine_CheckLoggedIn(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	cl := commandline{}
	cmd, _ := cl.NewCmd()
	cl.context = context.Background()
	cl.passwordReader = pwReaderMock
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}

	cmd.SetArgs([]string{"login", "immudb"})
	cmd.Execute()

	cl.options = Options()
	cl.options.DialOptions = &dialOptions
	cl.login(cmd)

	cmd1 := cobra.Command{}
	cl1 := new(commandline)
	cl1.context = context.Background()
	cl1.passwordReader = pwReaderMock
	cl1.ts = client.NewTokenService().WithHds(newHomedirServiceMock()).WithTokenFileName("token_admin")
	dialOptions1 := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}

	cl1.options = Options()
	cl1.options.DialOptions = &dialOptions1
	err := cl1.checkLoggedIn(&cmd1, nil)
	assert.Nil(t, err)
}

func newHomedirServiceMock() *clienttest.HomedirServiceMock {
	h := clienttest.DefaultHomedirServiceMock()
	h.FileExistsInUserHomeDirF = func(pathToFile string) (bool, error) {
		return true, nil
	}
	return h
}
