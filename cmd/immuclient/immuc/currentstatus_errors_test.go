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
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCurrentRootErrors(t *testing.T) {
	immuClientMock := &clienttest.ImmuClientMock{}
	errCurrentRoot := errors.New("CurrentRoot error")
	immuClientMock.CurrentRootF = func(ctx context.Context) (*schema.Root, error) {
		return nil, errCurrentRoot
	}
	ic := new(immuc)
	ic.ImmuClient = immuClientMock
	_, err := ic.CurrentRoot(nil)
	require.ErrorIs(t, err, errCurrentRoot)

	rpcErrMsg := "CurrentRoot RPC error"
	rpcErr := status.New(codes.Internal, rpcErrMsg).Err()
	immuClientMock.CurrentRootF = func(ctx context.Context) (*schema.Root, error) {
		return nil, rpcErr
	}
	resp, err := ic.CurrentRoot(nil)
	require.NoError(t, err)
	require.Equal(t, " CurrentRoot RPC error", resp)
}
*/
