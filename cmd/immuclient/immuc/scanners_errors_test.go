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

package immuc

/*
import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/stretchr/testify/require"
)

func TestScannersErrors(t *testing.T) {
	immuClientMock := &clienttest.ImmuClientMock{}
	ic := &immuc{ImmuClient: immuClientMock}

	// ZScan errors
	args := []string{"set1"}
	immuClientMock.ZScanF = func(context.Context, *schema.ZScanOptions) (*schema.ZStructuredItemList, error) {
		return nil, status.New(codes.Internal, "zscan RPC error").Err()
	}
	resp, err := ic.ZScan(args)
	require.NoError(t, err)
	require.Equal(t, " zscan RPC error", resp)

	errZScan := errors.New("zscan error")
	immuClientMock.ZScanF = func(context.Context, *schema.ZScanOptions) (*schema.ZStructuredItemList, error) {
		return nil, errZScan
	}
	_, err = ic.ZScan(args)
	require.ErrorIs(t, err, errZScan)

	immuClientMock.ZScanF = func(context.Context, *schema.ZScanOptions) (*schema.ZStructuredItemList, error) {
		return &schema.ZStructuredItemList{}, nil
	}
	resp, err = ic.ZScan(args)
	require.NoError(t, err)
	require.Equal(t, "0", resp)

	// IScan errors
	_, err = ic.IScan([]string{"X"})
	require.Error(t, err)

	_, err = ic.IScan([]string{"1", "X"})
	require.Error(t, err)

	args = []string{"1", "2"}
	immuClientMock.IScanF = func(context.Context, uint64, uint64) (*schema.SPage, error) {
		return nil, status.New(codes.Internal, "iscan RPC error").Err()
	}
	resp, err = ic.IScan(args)
	require.NoError(t, err)
	require.Equal(t, " iscan RPC error", resp)

	errIScan := errors.New("iscan error")
	immuClientMock.IScanF = func(context.Context, uint64, uint64) (*schema.SPage, error) {
		return nil, errIScan
	}
	_, err = ic.IScan(args)
	require.ErrorIs(t, err, errIScan)

	immuClientMock.IScanF = func(context.Context, uint64, uint64) (*schema.SPage, error) {
		return &schema.SPage{}, nil
	}
	resp, err = ic.IScan(args)
	require.NoError(t, err)
	require.Equal(t, "0", resp)

	// Scan errors
	args = []string{"prefix1"}
	immuClientMock.ScanF = func(context.Context, *schema.ScanOptions) (*schema.StructuredItemList, error) {
		return nil, status.New(codes.Internal, "scan RPC error").Err()
	}
	resp, err = ic.Scan(args)
	require.NoError(t, err)
	require.Equal(t, " scan RPC error", resp)

	errScan := errors.New("scan error")
	immuClientMock.ScanF = func(context.Context, *schema.ScanOptions) (*schema.StructuredItemList, error) {
		return nil, errScan
	}
	_, err = ic.Scan(args)
	require.ErrorIs(t, err, errScan)

	immuClientMock.ScanF = func(context.Context, *schema.ScanOptions) (*schema.StructuredItemList, error) {
		return &schema.StructuredItemList{}, nil
	}
	resp, err = ic.Scan(args)
	require.NoError(t, err)
	require.Equal(t, "0", resp)

	// Count errors
	errCount := errors.New("count error")
	immuClientMock.CountF = func(context.Context, []byte) (*schema.ItemsCount, error) {
		return nil, errCount
	}
	_, err = ic.Count(args)
	require.ErrorIs(t, err, errCount)
}
*/
