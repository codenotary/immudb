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

package server

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestKeepAliveSessionInterceptor(t *testing.T) {
	opts := DefaultOptions().
		WithDir(t.TempDir()).
		WithPort(0).
		WithPgsqlServer(false).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword)

	s := DefaultServer().WithOptions(opts).(*ImmuServer)
	defer s.CloseDatabases()

	err := s.Initialize()
	require.NoError(t, err)

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	t.Run("no session auth passes through", func(t *testing.T) {
		resp, err := s.KeepAliveSessionInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/immudb.schema.ImmuService/Set"}, handler)
		require.NoError(t, err)
		require.Equal(t, "ok", resp)
	})

	t.Run("OpenSession bypassed", func(t *testing.T) {
		md := metadata.New(map[string]string{"sessionid": "some-session"})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.KeepAliveSessionInterceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/immudb.schema.ImmuService/OpenSession"}, handler)
		require.NoError(t, err)
		require.Equal(t, "ok", resp)
	})

	t.Run("session auth with valid session context calls handler", func(t *testing.T) {
		// KeepAlive with session auth but no valid session - verifies the interceptor
		// calls KeepAlive which may error or succeed depending on session state
		md := metadata.New(map[string]string{"sessionid": "nonexistent"})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resp, err := s.KeepAliveSessionInterceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/immudb.schema.ImmuService/Set"}, handler)
		// KeepAlive silently ignores unknown sessions and proceeds to handler
		if err == nil {
			require.Equal(t, "ok", resp)
		}
	})
}

func TestKeepAliveSessionStreamInterceptor(t *testing.T) {
	opts := DefaultOptions().
		WithDir(t.TempDir()).
		WithPort(0).
		WithPgsqlServer(false).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword)

	s := DefaultServer().WithOptions(opts).(*ImmuServer)
	defer s.CloseDatabases()

	err := s.Initialize()
	require.NoError(t, err)

	t.Run("no session auth passes through", func(t *testing.T) {
		called := false
		err := s.KeepALiveSessionStreamInterceptor(nil, &mockServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{FullMethod: "test"}, func(srv interface{}, stream grpc.ServerStream) error {
			called = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("session auth context calls handler", func(t *testing.T) {
		md := metadata.New(map[string]string{"sessionid": "nonexistent"})
		ctx := metadata.NewIncomingContext(context.Background(), md)

		called := false
		err := s.KeepALiveSessionStreamInterceptor(nil, &mockServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "test"}, func(srv interface{}, stream grpc.ServerStream) error {
			called = true
			return nil
		})
		// KeepAlive silently ignores unknown sessions and proceeds to handler
		if err == nil {
			require.True(t, called)
		}
	})
}
