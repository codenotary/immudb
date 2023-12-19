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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetCommandsErrors(t *testing.T) {
	defer os.Remove(".root-")
	immuClientMock := &clienttest.ImmuClientMock{}
	ic := new(immuc)
	ic.ImmuClient = immuClientMock

	// GetByIndex
	_, err := ic.GetByIndex([]string{"X"})
	require.ErrorIs(t, err, errors.New(` "X" is not a valid index number`))

	immuClientMock.ByIndexF = func(ctx context.Context, index uint64) (*schema.StructuredItem, error) {
		return nil, errors.New("NotFound")
	}
	resp, err := ic.GetByIndex([]string{"0"})
	require.NoError(t, err)
	require.Equal(t, "no item exists in index:0", resp)

	immuClientMock.ByIndexF = func(ctx context.Context, index uint64) (*schema.StructuredItem, error) {
		return nil, status.New(codes.Internal, "ByIndex RPC error").Err()
	}
	resp, err = ic.GetByIndex([]string{"0"})
	require.NoError(t, err)
	require.Equal(t, " ByIndex RPC error", resp)

	errByIndex := errors.New("ByIndex error")
	immuClientMock.ByIndexF = func(ctx context.Context, index uint64) (*schema.StructuredItem, error) {
		return nil, errByIndex
	}
	_, err = ic.GetByIndex([]string{"0"})
	require.ErrorIs(t, err, errByIndex)

	// GetKey
	immuClientMock.GetF = func(ctx context.Context, key []byte) (*schema.StructuredItem, error) {
		return nil, errors.New("NotFound")
	}
	resp, err = ic.GetKey([]string{"key1"})
	require.NoError(t, err)
	require.Equal(t, "key not found: key1 ", resp)

	immuClientMock.GetF = func(ctx context.Context, key []byte) (*schema.StructuredItem, error) {
		return nil, status.New(codes.Internal, "Get RPC error").Err()
	}
	resp, err = ic.GetKey([]string{"key1"})
	require.NoError(t, err)
	require.Equal(t, " Get RPC error", resp)

	errGet := errors.New("Get error")
	immuClientMock.GetF = func(ctx context.Context, key []byte) (*schema.StructuredItem, error) {
		return nil, errGet
	}
	_, err = ic.GetKey([]string{"key1"})
	require.ErrorIs(t, err, errGet)

	// RawSafeGetKey
	immuClientMock.RawSafeGetF = func(context.Context, []byte, ...grpc.CallOption) (vi *client.VerifiedItem, err error) {
		return nil, errors.New("NotFound")
	}
	resp, err = ic.RawSafeGetKey([]string{"key1"})
	require.NoError(t, err)
	require.Equal(t, "key not found: key1 ", resp)

	immuClientMock.RawSafeGetF = func(context.Context, []byte, ...grpc.CallOption) (vi *client.VerifiedItem, err error) {
		return nil, status.New(codes.Internal, "RawSafeGet RPC error").Err()
	}
	resp, err = ic.RawSafeGetKey([]string{"key1"})
	require.NoError(t, err)
	require.Equal(t, " RawSafeGet RPC error", resp)

	errRawSafeGet := errors.New("RawSafeGet error")
	immuClientMock.RawSafeGetF = func(context.Context, []byte, ...grpc.CallOption) (vi *client.VerifiedItem, err error) {
		return nil, errRawSafeGet
	}
	_, err = ic.RawSafeGetKey([]string{"key1"})
	require.ErrorIs(t, err, errRawSafeGet)

	// SafeGetKey
	immuClientMock.SafeGetF = func(context.Context, []byte, ...grpc.CallOption) (vi *client.VerifiedItem, err error) {
		return nil, errors.New("NotFound")
	}
	resp, err = ic.SafeGetKey([]string{"key1"})
	require.NoError(t, err)
	require.Equal(t, "key not found: key1 ", resp)

	immuClientMock.SafeGetF = func(context.Context, []byte, ...grpc.CallOption) (vi *client.VerifiedItem, err error) {
		return nil, status.New(codes.Internal, "SafeGet RPC error").Err()
	}
	resp, err = ic.SafeGetKey([]string{"key1"})
	require.NoError(t, err)
	require.Equal(t, " SafeGet RPC error", resp)

	errSafeGet := errors.New("SafeGet error")
	immuClientMock.SafeGetF = func(context.Context, []byte, ...grpc.CallOption) (vi *client.VerifiedItem, err error) {
		return nil, errSafeGet
	}
	_, err = ic.SafeGetKey([]string{"key1"})
	require.ErrorIs(t, err, errSafeGet)

	// GetRawBySafeIndex
	_, err = ic.GetRawBySafeIndex([]string{"X"})
	require.Error(t, err)

	errRawBySafeIndex := errors.New("RawBySafeIndex error")
	immuClientMock.RawBySafeIndexF = func(context.Context, uint64) (*client.VerifiedItem, error) {
		return nil, errRawBySafeIndex
	}
	_, err = ic.GetRawBySafeIndex([]string{"0"})
	require.ErrorIs(t, err, errRawBySafeIndex)
}
*/
