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

package immutest

/*
import (
	"context"
	"errors"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestImmutest(t *testing.T) {
	defer viper.Reset()

	viper.Set("database", "defaultdb")
	viper.Set("user", "immudb")
	data := map[string]string{}
	var index uint64
	loginFOK := func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{Token: "token"}, nil
	}
	disconnectFOK := func() error { return nil }
	useDatabaseFOK := func(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error) {
		return &schema.UseDatabaseReply{Token: "token"}, nil
	}
	setFOK := func(ctx context.Context, key []byte, value []byte) (*schema.Index, error) {
		data[string(key)] = string(value)
		r := schema.Index{Index: index}
		index++
		return &r, nil
	}
	icm := &clienttest.ImmuClientMock{
		GetOptionsF: func() *client.Options {
			return client.DefaultOptions()
		},
		LoginF:       loginFOK,
		DisconnectF:  disconnectFOK,
		UseDatabaseF: useDatabaseFOK,
		SetF:         setFOK,
	}

	pwReaderMockOK := &clienttest.PasswordReaderMock{}

	termReaderMockOK := &clienttest.TerminalReaderMock{
		ReadFromTerminalYNF: func(def string) (selected string, err error) {
			return "Y", nil
		},
	}

	newClient := func(opts *client.Options) (client.ImmuClient, error) {
		return icm, nil
	}

	ts := clienttest.DefaultTokenServiceMock()
	errFunc := func(err error) {
		require.NoError(t, err)
	}

	cmd1 := NewCmd(newClient, pwReaderMockOK, termReaderMockOK, ts, errFunc)
	cmd1.SetArgs([]string{"3"})
	cmd1.Execute()
	require.Equal(t, 3, len(data))

	ts2 := clienttest.DefaultTokenServiceMock()
	hdsWriteErr := errors.New("hds write error")
	ts2.SetTokenF = func(db string, content string) error {
		return hdsWriteErr
	}

	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, hdsWriteErr, err)
	}
	cmd2 := NewCmd(newClient, pwReaderMockOK, termReaderMockOK, ts2, errFunc)
	cmd2.SetArgs([]string{"3"})
	cmd2.Execute()

	viper.Set("user", "someuser")
	cmd3 := NewCmd(newClient, pwReaderMockOK, termReaderMockOK, ts, errFunc)
	cmd3.SetArgs([]string{"3"})
	cmd3.Execute()

	icErr := errors.New("some immuclient error")
	newClientErrFunc := func(opts *client.Options) (client.ImmuClient, error) {
		return nil, icErr
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, icErr, err)
	}
	cmd4 := NewCmd(newClientErrFunc, pwReaderMockOK, termReaderMockOK, ts, errFunc)
	cmd4.SetArgs([]string{"3"})
	cmd4.Execute()

	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, `strconv.Atoi: parsing "a": invalid syntax`, err.Error())
	}
	cmd5 := NewCmd(newClient, pwReaderMockOK, termReaderMockOK, ts, errFunc)
	cmd5.SetArgs([]string{"a"})
	cmd5.Execute()

	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(
			t,
			`Please specify a number of entries greater than 0 or call the command without any argument so that the default number of 100 entries will be used`,
			err.Error())
	}
	cmd6 := NewCmd(newClient, pwReaderMockOK, termReaderMockOK, ts, errFunc)
	cmd6.SetArgs([]string{"0"})
	cmd6.Execute()

	pwrErr := errors.New("pwr read error")
	pwReaderMockErr := &clienttest.PasswordReaderMock{
		ReadF: func(string) ([]byte, error) { return nil, pwrErr },
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, pwrErr, err)
	}
	cmd7 := NewCmd(newClient, pwReaderMockErr, termReaderMockOK, ts, errFunc)
	cmd7.SetArgs([]string{"1"})
	cmd7.Execute()

	loginErr := errors.New("some login err")
	icm.LoginF = func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
		return nil, loginErr
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, loginErr, err)
	}
	cmd8 := NewCmd(newClient, pwReaderMockOK, termReaderMockOK, ts, errFunc)
	cmd8.SetArgs([]string{"1"})
	cmd8.Execute()

	icm.LoginF = loginFOK
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, hdsWriteErr, err)
	}
	cmd9 := NewCmd(newClient, pwReaderMockOK, termReaderMockOK, ts2, errFunc)
	cmd9.SetArgs([]string{"1"})
	cmd9.Execute()

	errUseDb := errors.New("some use db error")
	icm.UseDatabaseF = func(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error) {
		return nil, errUseDb
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.ErrorIs(t, err, errUseDb)
	}
	cmd10 := NewCmd(newClient, pwReaderMockOK, termReaderMockOK, ts, errFunc)
	cmd10.SetArgs([]string{"1"})
	cmd10.Execute()

	icm.UseDatabaseF = useDatabaseFOK
	termReaderMockErr := &clienttest.TerminalReaderMock{
		ReadFromTerminalYNF: func(def string) (selected string, err error) {
			return "", errors.New("some tr error")
		},
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.Equal(t, "Canceled", err.Error())
	}
	cmd11 := NewCmd(newClient, pwReaderMockOK, termReaderMockErr, ts, errFunc)
	cmd11.SetArgs([]string{"1"})
	cmd11.Execute()

	errSet := errors.New("some set error")
	icm.SetF = func(ctx context.Context, key []byte, value []byte) (*schema.Index, error) {
		return nil, errSet
	}
	errFunc = func(err error) {
		require.Error(t, err)
		require.ErrorIs(t, err, errSet)
	}
	cmd12 := NewCmd(newClient, pwReaderMockOK, termReaderMockOK, ts, errFunc)
	cmd12.SetArgs([]string{"1"})
	cmd12.Execute()

	icm.SetF = setFOK
	errDisconnect := errors.New("some disconnect error")
	icm.DisconnectF = func() error { return errDisconnect }
	errFunc = func(err error) {
		require.Error(t, err)
		require.ErrorIs(t, err, errDisconnect)
	}
	cmd13 := NewCmd(newClient, pwReaderMockOK, termReaderMockOK, ts, errFunc)
	cmd13.SetArgs([]string{"1"})
	cmd13.Execute()
}
*/
