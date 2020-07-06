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
	"context"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
)

func TestCommandLine_Connect(t *testing.T) {
	immuServer = newServer(true)
	immuServer.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
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

func TestCommandLine_CheckLoggedInAndConnect(t *testing.T) {
	immuServer = newServer(true)
	immuServer.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}
	opts := Options()
	opts.DialOptions = &dialOptions
	cmdl := commandline{
		context: context.Background(),
		options: opts,
	}
	err := cmdl.checkLoggedInAndConnect(&cobra.Command{}, []string{})
	assert.Nil(t, err)
}

func TestCommandLine_Disconnect(t *testing.T) {
	immuServer = newServer(true)
	immuServer.Start()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
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
