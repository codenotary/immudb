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
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReferencesErrors(t *testing.T) {
	immuClientMock := &clienttest.ImmuClientMock{}
	ic := &immuc{ImmuClient: immuClientMock}

	// Reference errors
	args := []string{"refKey1", "key1"}
	immuClientMock.ReferenceF = func(context.Context, []byte, []byte, *schema.Index) (*schema.Index, error) {
		return nil, status.New(codes.Internal, "reference RPC error").Err()
	}
	resp, err := ic.Reference(args)
	require.NoError(t, err)
	require.Equal(t, " reference RPC error", resp)

	errReference := errors.New("reference error")
	immuClientMock.ReferenceF = func(context.Context, []byte, []byte, *schema.Index) (*schema.Index, error) {
		return nil, errReference
	}
	_, err = ic.Reference(args)
	require.ErrorIs(t, err, errReference)

	// SafeReference errors
	immuClientMock.SafeReferenceF = func(context.Context, []byte, []byte, *schema.Index) (*client.VerifiedIndex, error) {
		return nil, status.New(codes.Internal, "safe reference RPC error").Err()
	}
	resp, err = ic.SafeReference(args)
	require.NoError(t, err)
	require.Equal(t, " safe reference RPC error", resp)

	errSafeReference := errors.New("safe reference error")
	immuClientMock.SafeReferenceF = func(context.Context, []byte, []byte, *schema.Index) (*client.VerifiedIndex, error) {
		return nil, errSafeReference
	}
	_, err = ic.SafeReference(args)
	require.ErrorIs(t, err, errSafeReference)
}
*/
