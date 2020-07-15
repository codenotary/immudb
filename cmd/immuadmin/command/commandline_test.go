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
	"errors"
	"testing"

	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/stretchr/testify/require"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
)

func TestCommandline(t *testing.T) {
	options := client.DefaultOptions()

	immuClient := &clienttest.ImmuClientMock{}
	pwr := &clienttest.PasswordReaderMock{}
	hds := clienttest.DefaultHomedirServiceMock()

	cl := &commandline{
		options:    options,
		immuClient: immuClient,
		newImmuClient: func(*client.Options) (client.ImmuClient, error) {
			return immuClient, nil
		},
		passwordReader: pwr,
		context:        context.Background(),
		hds:            hds,
	}
	cmd := &cobra.Command{}

	errDisconnect := errors.New("disconnect error")
	immuClient.DisconnectF = func() error {
		return errDisconnect
	}
	cl.onError = func(err error) {
		require.Equal(t, errDisconnect, err)
	}
	cl.disconnect(cmd, nil)

	cl.onError = func(err error) {
		require.NoError(t, err)
	}
	require.NoError(t, cl.connect(cmd, nil))
	errNewImmuClient := errors.New("error new immuclient")
	okNewImuClientF := cl.newImmuClient
	cl.newImmuClient = func(*client.Options) (client.ImmuClient, error) {
		return nil, errNewImmuClient
	}
	cl.onError = func(err error) {
		require.Equal(t, errNewImmuClient, err)
	}
	cl.connect(cmd, nil)
	cl.newImmuClient = okNewImuClientF

	errPleaseLogin := errors.New("please login first")
	cl.onError = func(err error) {
		require.Equal(t, errPleaseLogin, err)
	}
	require.Equal(t, errPleaseLogin, cl.checkLoggedIn(cmd, nil))
	errFileExists := errors.New("error file exists in user home dir")
	prevFileExists := hds.FileExistsInUserHomeDirF
	hds.FileExistsInUserHomeDirF = func(pathToFile string) (bool, error) {
		return false, errFileExists
	}
	cl.hds = hds
	require.NoError(t, cl.checkLoggedIn(cmd, nil))
	hds.FileExistsInUserHomeDirF = prevFileExists
	cl.hds = hds

	require.Equal(t, errPleaseLogin, cl.checkLoggedInAndConnect(cmd, nil))
	hds.FileExistsInUserHomeDirF = func(pathToFile string) (bool, error) {
		return true, nil
	}
	cl.hds = hds
	cl.newImmuClient = func(*client.Options) (client.ImmuClient, error) {
		return nil, errNewImmuClient
	}
	cl.onError = func(err error) {
		require.Equal(t, errNewImmuClient, err)
	}
	require.Equal(t, errNewImmuClient, cl.checkLoggedInAndConnect(cmd, nil))
}
