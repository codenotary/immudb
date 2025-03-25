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

package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestClientUnaryInterceptor(t *testing.T) {
	f := ClientUnaryInterceptor("token")

	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"Bearer token"}, md.Get("authorization"))
		return nil
	}
	err := f(context.Background(), "", "", "", nil, invoker)
	require.NoError(t, err)
}

func TestClientStreamInterceptor(t *testing.T) {
	f := ClientStreamInterceptor("token")
	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"Bearer token"}, md.Get("authorization"))
		return nil, nil
	}
	_, err := f(context.Background(), nil, nil, "", streamer)
	require.NoError(t, err)
}
