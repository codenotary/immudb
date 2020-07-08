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
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/prometheus/common/log"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"io/ioutil"
	"testing"
)

func TestCommandLine_Connect(t *testing.T) {
	log.Info("TestCommandLine_Connect")
	bs := servertest.NewBufconnServer(server.Options{}.WithAuth(false).WithInMemoryStore(true))
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
	bs := servertest.NewBufconnServer(server.Options{}.WithAuth(false).WithInMemoryStore(true))
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
	_ = cmdl.connect(&cobra.Command{}, []string{})

	cmdl.disconnect(&cobra.Command{}, []string{})

	err := cmdl.immuClient.Disconnect()
	assert.Errorf(t, err, "not connected")
}

func TestCommandLine_LoginLogout(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	cmd := cobra.Command{}
	cl := new(commandline)
	cl.context = context.Background()
	cl.passwordReader = &pwrMock{}
	cl.hds = client.NewHomedirService()
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}

	cl.options = Options()
	cl.options.DialOptions = &dialOptions
	cl.login(&cmd)

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetArgs([]string{"login", "immudb"})
	cmd.Execute()
	out, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(out), "logged in")

	b1 := bytes.NewBufferString("")
	logoutcmd := cobra.Command{}
	logoutcmd.SetOut(b1)
	logoutcmd.SetArgs([]string{"logout"})

	cl.logout(&logoutcmd)

	logoutcmd.Execute()
	out1, err1 := ioutil.ReadAll(b1)
	if err1 != nil {
		t.Fatal(err1)
	}
	assert.Contains(t, string(out1), "logged out")
}

func TestCommandLine_CheckLoggedIn(t *testing.T) {
	options := server.Options{}.WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	cmd := cobra.Command{}
	cl := new(commandline)
	cl.context = context.Background()
	cl.passwordReader = &pwrMock{}
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}

	cmd.SetArgs([]string{"login", "immudb"})
	cmd.Execute()

	cl.options = Options()
	cl.options.DialOptions = &dialOptions
	cl.login(&cmd)

	cmd1 := cobra.Command{}
	cl1 := new(commandline)
	cl1.context = context.Background()
	cl1.passwordReader = &pwrMock{}
	cl1.hds = &homedirServiceMock{}
	dialOptions1 := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}

	cl1.options = Options()
	cl1.options.DialOptions = &dialOptions1
	err := cl1.checkLoggedIn(&cmd1, nil)
	assert.Nil(t, err)
}

type homedirServiceMock struct {
	client.HomedirService
}

func (h *homedirServiceMock) FileExistsInUserHomeDir(pathToFile string) (bool, error) {
	return true, nil
}

type pwrMock struct{}

var count = 0

func (pr *pwrMock) Read(msg string) ([]byte, error) {
	var pw []byte
	if count == 0 {
		pw = []byte(`immudb`)
	} else {
		pw = []byte(`Passw0rd!-`)
	}
	count++
	return pw, nil
}
