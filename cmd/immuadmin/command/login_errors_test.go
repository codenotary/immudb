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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/helper"
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestLoginLogoutErrors(t *testing.T) {
	immuClientMock := &clienttest.ImmuClientMock{
		GetOptionsF: func() *client.Options {
			return client.DefaultOptions()
		},
		ConnectF: func(context.Context) (*grpc.ClientConn, error) {
			return &grpc.ClientConn{}, nil
		},
		DisconnectF: func() error {
			return nil
		},
	}
	pwReaderMock := &clienttest.PasswordReaderMock{}
	hdsMock := clienttest.DefaultHomedirServiceMock()
	cl := &commandline{
		immuClient:     immuClientMock,
		passwordReader: pwReaderMock,
		ts:             tokenservice.NewTokenService().WithHds(hdsMock).WithTokenFileName("tokenFileName"),
	}

	rootCmd := &cobra.Command{}

	// login
	cl.login(rootCmd)
	cmd := rootCmd.Commands()[0]
	cmd.PersistentPreRunE = nil
	cmd.PersistentPostRunE = nil
	cmdOutput := bytes.NewBufferString("")
	cmd.SetOutput(cmdOutput)

	username := "user1"
	args := []string{"login", username}
	rootCmd.SetArgs(args)

	errPermissionDenied :=
		fmt.Errorf("Permission denied: user %s has no admin rights", username)
	cl.onError = func(msg interface{}) {
		require.Equal(t, errPermissionDenied, msg)
	}
	require.Equal(t, errPermissionDenied, rootCmd.Execute())

	args[1] = auth.SysAdminUsername
	rootCmd.SetArgs(args)
	errPwRead := errors.New("password read error")
	pwReaderMock.ReadF = func(msg string) ([]byte, error) {
		return nil, errPwRead
	}
	cl.onError = func(msg interface{}) {
		require.Equal(t, errPwRead, msg)
	}
	require.Equal(t, errPwRead, rootCmd.Execute())

	pass := "$somePass1!"
	passBytes := []byte(pass)
	pwReaderMock.ReadF = func(msg string) ([]byte, error) {
		return passBytes, nil
	}
	errLogin := errors.New("login error")
	immuClientMock.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return nil, errLogin
	}
	cl.onError = func(msg interface{}) {
		require.Equal(t, errLogin, msg)
	}
	require.Equal(t, errLogin, rootCmd.Execute())

	immuClientMock.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{}, nil
	}
	errWriteTokenFile := errors.New("write token file error")
	hdsMock.WriteFileToUserHomeDirF = func(content []byte, pathToFile string) error {
		return errWriteTokenFile
	}
	cl.onError = func(msg interface{}) {
		require.Equal(t, errWriteTokenFile, msg)
	}
	require.Equal(t, errWriteTokenFile, rootCmd.Execute())
	hdsMock.WriteFileToUserHomeDirF = func(content []byte, pathToFile string) error {
		return nil
	}

	errNewImmuClient := errors.New("new immuclient error")
	cl.newImmuClient = func(*client.Options) (client.ImmuClient, error) {
		return nil, errNewImmuClient
	}
	cl.onError = func(msg interface{}) {
		require.Equal(t, errNewImmuClient, msg)
	}
	require.Equal(t, errNewImmuClient, rootCmd.Execute())
	cl.immuClient = immuClientMock
	cl.newImmuClient = func(*client.Options) (client.ImmuClient, error) {
		return immuClientMock, nil
	}

	require.NoError(t, rootCmd.Execute())
	outputBytes, err := ioutil.ReadAll(cmdOutput)
	require.NoError(t, err)
	require.Equal(t, "logged in\n", string(outputBytes))
	cmdOutput.Reset()

	immuClientMock.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{Warning: []byte(auth.WarnDefaultAdminPassword)}, nil
	}
	counterPwRead := 0
	pwReaderMock.ReadF = func(msg string) ([]byte, error) {
		counterPwRead++
		if counterPwRead == 1 {
			return passBytes, nil
		}
		return nil, errPwRead
	}
	errPwRead2 := errors.New("Error Reading Password")
	cl.onError = func(msg interface{}) {
		require.Equal(t, errPwRead2, msg)
	}
	require.Equal(t, errPwRead2, rootCmd.Execute())
	outputBytes, err = ioutil.ReadAll(cmdOutput)
	require.NoError(t, err)
	require.Equal(
		t,
		fmt.Sprintf("logged in\n%sSECURITY WARNING: %s\n%s", c.Yellow, auth.WarnDefaultAdminPassword, helper.Reset),
		string(outputBytes))
	cmdOutput.Reset()

	pwReaderMock.ReadF = func(msg string) ([]byte, error) {
		return passBytes, nil
	}
	immuClientMock.ChangePasswordF = func(context.Context, []byte, []byte, []byte) error {
		return nil
	}
	counterLogin := 0
	immuClientMock.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		counterLogin++
		if counterLogin == 1 {
			return &schema.LoginResponse{Warning: []byte(auth.WarnDefaultAdminPassword)}, nil
		}
		return nil, errLogin
	}
	cl.onError = func(msg interface{}) {
		require.Equal(t, errLogin, msg)
	}
	require.Equal(t, errLogin, rootCmd.Execute())
	cmdOutput.Reset()

	counterLogin = 0
	immuClientMock.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		counterLogin++
		if counterLogin == 1 {
			return &schema.LoginResponse{Warning: []byte(auth.WarnDefaultAdminPassword)}, nil
		}
		return &schema.LoginResponse{}, nil
	}
	require.NoError(t, cmd.Execute())
	outputBytes, err = ioutil.ReadAll(cmdOutput)
	require.NoError(t, err)
	require.Equal(
		t,
		fmt.Sprintf(
			"logged in\n%sSECURITY WARNING: %s\n%s%s's password has been changed",
			c.Yellow, auth.WarnDefaultAdminPassword, helper.Reset, args[1]),
		string(outputBytes))
	cmdOutput.Reset()

	// logout
	cl.logout(rootCmd)
	var cmdLogout *cobra.Command
	for _, ccmd := range rootCmd.Commands() {
		if strings.Contains(ccmd.Use, "logout") {
			cmdLogout = ccmd
			break
		}
	}
	require.NotNil(t, cmdLogout)
	cmdLogout.PersistentPreRunE = nil
	cmdLogout.PersistentPostRunE = nil
	cmdLogout.SetOutput(cmdOutput)

	rootCmd.SetArgs([]string{"logout"})

	errLogout := errors.New("logout error")
	immuClientMock.LogoutF = func(context.Context) error {
		return errLogout
	}
	cl.onError = func(msg interface{}) {
		require.Equal(t, errLogout, msg)
	}
	require.Equal(t, errLogout, rootCmd.Execute())

	immuClientMock.LogoutF = func(context.Context) error {
		return nil
	}
	require.NoError(t, rootCmd.Execute())
	outputBytes, err = ioutil.ReadAll(cmdOutput)
	require.NoError(t, err)
	require.Equal(t, "logged out\n", string(outputBytes))
	cmdOutput.Reset()
}
*/
