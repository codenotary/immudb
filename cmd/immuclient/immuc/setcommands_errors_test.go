/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package immuc

/*
import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestSetCommandsErrors(t *testing.T) {
	defer os.Remove(".root-")
	immuClientMock := &clienttest.ImmuClientMock{}
	ic := &immuc{ImmuClient: immuClientMock}

	// RawSafeSet errors
	args := []string{"key1", "value1"}
	errRawSafeSet := errors.New("raw safe set error")
	immuClientMock.RawSafeSetF = func(context.Context, []byte, []byte) (vi *client.VerifiedIndex, err error) {
		return nil, errRawSafeSet
	}
	_, err := ic.RawSafeSet(args)
	require.ErrorIs(t, err, errRawSafeSet)
	immuClientMock.RawSafeSetF = func(context.Context, []byte, []byte) (vi *client.VerifiedIndex, err error) {
		return nil, nil
	}

	errRawSafeGet := errors.New("raw safe get error")
	immuClientMock.RawSafeGetF = func(context.Context, []byte, ...grpc.CallOption) (vi *client.VerifiedItem, err error) {
		return nil, errRawSafeGet
	}
	_, err = ic.RawSafeSet(args)
	require.ErrorIs(t, err, errRawSafeGet)

	// Set errors
	errSet := errors.New("set error")
	immuClientMock.SetF = func(context.Context, []byte, []byte) (*schema.Index, error) {
		return nil, errSet
	}
	_, err = ic.Set(args)
	require.ErrorIs(t, err, errSet)
	immuClientMock.SetF = func(context.Context, []byte, []byte) (*schema.Index, error) {
		return nil, nil
	}

	errGet := errors.New("get error")
	immuClientMock.GetF = func(context.Context, []byte) (*schema.StructuredItem, error) {
		return nil, errGet
	}
	_, err = ic.Set(args)
	require.ErrorIs(t, err, errGet)

	// SafeSet errors
	errSafeSet := errors.New("safe set error")
	immuClientMock.SafeSetF = func(context.Context, []byte, []byte) (*client.VerifiedIndex, error) {
		return nil, errSafeSet
	}
	_, err = ic.SafeSet(args)
	require.ErrorIs(t, err, errSafeSet)

	immuClientMock.SafeSetF = func(context.Context, []byte, []byte) (*client.VerifiedIndex, error) {
		return nil, nil
	}
	errSafeGet := errors.New("safe get errrors")
	immuClientMock.SafeGetF = func(context.Context, []byte, ...grpc.CallOption) (*client.VerifiedItem, error) {
		return nil, errSafeGet
	}
	_, err = ic.SafeSet(args)
	require.ErrorIs(t, err, errSafeGet)

	// ZAdd errors
	_, err = ic.ZAdd([]string{"set1", "X", "key1"})
	require.Error(t, err)

	errZAdd := errors.New("zadd error")
	immuClientMock.ZAddF = func(context.Context, []byte, float64, []byte, *schema.Index) (*schema.Index, error) {
		return nil, errZAdd
	}
	_, err = ic.ZAdd([]string{"set1", "1", "key1"})
	require.ErrorIs(t, err, errZAdd)

	// SafeZAdd errors
	_, err = ic.SafeZAdd([]string{"set1", "X", "key1"})
	require.Error(t, err)

	errSafeZAdd := errors.New("safe zadd error")
	immuClientMock.SafeZAddF = func(context.Context, []byte, float64, []byte, *schema.Index) (*client.VerifiedIndex, error) {
		return nil, errSafeZAdd
	}
	_, err = ic.SafeZAdd([]string{"set1", "1", "key1"})
	require.ErrorIs(t, err, errSafeZAdd)

	// CreateDatabase errors
	_, err = ic.CreateDatabase(nil)
	require.Equal(
		t,
		errors.New("ERROR: Not enough arguments. Use [command] --help for documentation "),
		err)

	errCreateDb := errors.New("create database error")
	immuClientMock.CreateDatabaseF = func(context.Context, *schema.Database) error {
		return errCreateDb
	}
	_, err = ic.CreateDatabase([]string{"db1"})
	require.ErrorIs(t, err, errCreateDb)

	// DatabaseList errors
	errDbList := errors.New("database list error")
	immuClientMock.DatabaseListF = func(context.Context) (*schema.DatabaseListResponse, error) {
		return nil, errDbList
	}
	_, err = ic.DatabaseList(nil)
	require.ErrorIs(t, err, errDbList)

	ic.options = &client.Options{CurrentDatabase: "db2"}
	immuClientMock.DatabaseListF = func(context.Context) (*schema.DatabaseListResponse, error) {
		return &schema.DatabaseListResponse{
			Databases: []*schema.Database{
				&schema.Database{Databasename: "db1"},
				&schema.Database{Databasename: "db2"},
			},
		}, nil
	}
	resp, err := ic.DatabaseList(nil)
	require.NoError(t, err)
	require.Contains(t, resp, "db1")
	require.Contains(t, resp, "*db2")

	// UseDatabase errors
	errUseDb := errors.New("use database error")
	immuClientMock.UseDatabaseF = func(context.Context, *schema.Database) (*schema.UseDatabaseReply, error) {
		return nil, errUseDb
	}
	args = []string{"db1"}
	_, err = ic.UseDatabase(args)
	require.ErrorIs(t, err, errUseDb)

	immuClientMock.UseDatabaseF = func(context.Context, *schema.Database) (*schema.UseDatabaseReply, error) {
		return &schema.UseDatabaseReply{
			Token: "sometoken",
		}, nil
	}
	immuClientMock.GetOptionsF = func() *client.Options {
		return &client.Options{TokenFileName: "sometokenfile"}
	}
	hdsMock := clienttest.DefaultHomedirServiceMock()
	ic.ts = tokenservice.NewTokenService().WithHds(hdsMock)
	errWriteFileToHomeDir := errors.New("write file to home dir errror")
	hdsMock.WriteFileToUserHomeDirF = func(content []byte, pathToFile string) error {
		return errWriteFileToHomeDir
	}
	_, err = ic.UseDatabase(args)
	require.ErrorIs(t, err, errWriteFileToHomeDir)
}
*/
