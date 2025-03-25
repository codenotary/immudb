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

package main

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

var homedirContent []byte

func TestImmutest(t *testing.T) {
	defer viper.Reset()

	viper.Set("database", "defaultdb")
	viper.Set("user", "immudb")
	data := map[string]string{}
	var index uint64
	icm := &clienttest.ImmuClientMock{
		GetOptionsF: func() *client.Options {
			return client.DefaultOptions()
		},
		LoginF: func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
			return &schema.LoginResponse{Token: "token"}, nil
		},
		DisconnectF: func() error { return nil },
		UseDatabaseF: func(ctx context.Context, d *schema.Database) (*schema.UseDatabaseReply, error) {
			return &schema.UseDatabaseReply{Token: "token"}, nil
		},
		SetF: func(ctx context.Context, key []byte, value []byte) (*schema.Index, error) {
			data[string(key)] = string(value)
			r := schema.Index{Index: index}
			index++
			return &r, nil
		},
	}

	pwReaderMock := &clienttest.PasswordReaderMock{}

	ts := clienttest.DefaultTokenServiceMock()
	trMock := &clienttest.TerminalReaderMock{
		ReadFromTerminalYNF: func(string) (string, error) {
			return "Y", nil
		},
	}

	execute(
		func(opts *client.Options) (client.ImmuClient, error) {
			return icm, nil
		},
		pwReaderMock,
		trMock,
		ts,
		func(err error) {
			require.NoError(t, err)
		},
		[]string{"3"})
	require.Equal(t, 3, len(data))

	icErr := errors.New("some immuclient error")
	execute(
		func(opts *client.Options) (client.ImmuClient, error) {
			return nil, icErr
		},
		pwReaderMock,
		trMock,
		ts,
		func(err error) {
			require.Error(t, err)
			require.Equal(t, icErr, err)
		},
		[]string{"3"})
}
*/
