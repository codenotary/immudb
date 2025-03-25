/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package immuadmin

/*
import (
	"context"
	"errors"
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	"github.com/stretchr/testify/assert"

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
		ts:             tokenservice.NewTokenService().WithHds(hds).WithTokenFileName("testTokenFile"),
	}
	cmd := &cobra.Command{}

	errDisconnect := errors.New("disconnect error")
	immuClient.DisconnectF = func() error {
		return errDisconnect
	}
	cl.onError = func(msg interface{}) {
		require.Equal(t, errDisconnect, msg)
	}
	cl.disconnect(cmd, nil)

	cl.onError = func(msg interface{}) {
		require.Empty(t, msg)
	}
	require.NoError(t, cl.connect(cmd, nil))
	errNewImmuClient := errors.New("error new immuclient")
	okNewImuClientF := cl.newImmuClient
	cl.newImmuClient = func(*client.Options) (client.ImmuClient, error) {
		return nil, errNewImmuClient
	}
	cl.onError = func(msg interface{}) {
		require.Equal(t, errNewImmuClient, msg)
	}
	cl.connect(cmd, nil)
	cl.newImmuClient = okNewImuClientF

	errPleaseLogin := errors.New("please login first")
	cl.onError = func(msg interface{}) {
		require.Equal(t, errPleaseLogin, msg)
	}
	require.Equal(t, errPleaseLogin, cl.checkLoggedIn(cmd, nil))
	errFileExists := errors.New("error file exists in user home dir")
	prevFileExists := hds.FileExistsInUserHomeDirF
	hds.FileExistsInUserHomeDirF = func(pathToFile string) (bool, error) {
		return false, errFileExists
	}
	cl.ts = tokenservice.NewTokenService().WithHds(hds).WithTokenFileName("testTokenFile")
	require.NoError(t, cl.checkLoggedIn(cmd, nil))
	hds.FileExistsInUserHomeDirF = prevFileExists
	cl.ts = tokenservice.NewTokenService().WithHds(hds).WithTokenFileName("testTokenFile")

	require.Equal(t, errPleaseLogin, cl.checkLoggedInAndConnect(cmd, nil))
	hds.FileExistsInUserHomeDirF = func(pathToFile string) (bool, error) {
		return true, nil
	}
	cl.ts = tokenservice.NewTokenService().WithHds(hds).WithTokenFileName("testTokenFile")
	cl.newImmuClient = func(*client.Options) (client.ImmuClient, error) {
		return nil, errNewImmuClient
	}
	cl.onError = func(msg interface{}) {
		require.Equal(t, errNewImmuClient, msg)
	}
	require.Equal(t, errNewImmuClient, cl.checkLoggedInAndConnect(cmd, nil))
}

func TestCommandline_Register(t *testing.T) {
	c := commandline{}
	cmd := c.Register(&cobra.Command{})
	assert.IsType(t, &cobra.Command{}, cmd)
}

func TestNewCommandLine(t *testing.T) {
	cml := NewCommandLine()
	assert.IsType(t, &commandline{}, cml)
}

func TestCommandline_ConfigChain(t *testing.T) {
	cmd := &cobra.Command{}
	c := commandline{
		config: helper.Config{Name: "test"},
	}
	f := func(cmd *cobra.Command, args []string) error {
		return nil
	}
	cmd.Flags().StringVar(&c.config.CfgFn, "config", "", "config file")
	cc := c.ConfigChain(f)
	err := cc(cmd, []string{})
	assert.NoError(t, err)
}

func TestCommandline_ConfigChainErr(t *testing.T) {
	cmd := &cobra.Command{}

	c := commandline{}
	f := func(cmd *cobra.Command, args []string) error {
		return nil
	}

	cc := c.ConfigChain(f)

	err := cc(cmd, []string{})
	assert.Error(t, err)
}
*/
