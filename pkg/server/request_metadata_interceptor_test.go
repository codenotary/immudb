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
	"net"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func TestInjectRequestMetadataUnaryInterceptor(t *testing.T) {
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}

	t.Run("disabled passes through", func(t *testing.T) {
		opts := DefaultOptions().
			WithDir(t.TempDir()).
			WithMetricsServer(false).
			WithAdminPassword(auth.SysAdminPassword)
		opts.LogRequestMetadata = false

		s := DefaultServer().WithOptions(opts).(*ImmuServer)
		defer s.CloseDatabases()
		err := s.Initialize()
		require.NoError(t, err)

		resp, err := s.InjectRequestMetadataUnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
		require.NoError(t, err)
		require.Equal(t, "ok", resp)
	})

	t.Run("enabled injects metadata", func(t *testing.T) {
		opts := DefaultOptions().
			WithDir(t.TempDir()).
			WithMetricsServer(false).
			WithAdminPassword(auth.SysAdminPassword)
		opts.LogRequestMetadata = true

		s := DefaultServer().WithOptions(opts).(*ImmuServer)
		defer s.CloseDatabases()
		err := s.Initialize()
		require.NoError(t, err)

		// Without user context, withRequestMetadata returns ctx without user metadata
		resp, err := s.InjectRequestMetadataUnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
		require.NoError(t, err)
		require.Equal(t, "ok", resp)
	})
}

func TestInjectRequestMetadataStreamInterceptor(t *testing.T) {
	t.Run("disabled passes through", func(t *testing.T) {
		opts := DefaultOptions().
			WithDir(t.TempDir()).
			WithMetricsServer(false).
			WithAdminPassword(auth.SysAdminPassword)
		opts.LogRequestMetadata = false

		s := DefaultServer().WithOptions(opts).(*ImmuServer)
		defer s.CloseDatabases()
		err := s.Initialize()
		require.NoError(t, err)

		called := false
		err = s.InjectRequestMetadataStreamInterceptor(nil, &mockServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{}, func(srv interface{}, stream grpc.ServerStream) error {
			called = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("enabled wraps stream context", func(t *testing.T) {
		opts := DefaultOptions().
			WithDir(t.TempDir()).
			WithMetricsServer(false).
			WithAdminPassword(auth.SysAdminPassword)
		opts.LogRequestMetadata = true

		s := DefaultServer().WithOptions(opts).(*ImmuServer)
		defer s.CloseDatabases()
		err := s.Initialize()
		require.NoError(t, err)

		called := false
		err = s.InjectRequestMetadataStreamInterceptor(nil, &mockServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{}, func(srv interface{}, stream grpc.ServerStream) error {
			called = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, called)
	})
}

func TestIpAddrFromContext(t *testing.T) {
	t.Run("no metadata no peer", func(t *testing.T) {
		ip := ipAddrFromContext(context.Background())
		require.Empty(t, ip)
	})

	t.Run("x-forwarded-for header", func(t *testing.T) {
		md := metadata.New(map[string]string{"x-forwarded-for": "10.0.0.1"})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ip := ipAddrFromContext(ctx)
		require.Equal(t, "10.0.0.1", ip)
	})

	t.Run("x-real-ip header", func(t *testing.T) {
		md := metadata.New(map[string]string{"x-real-ip": "10.0.0.2"})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ip := ipAddrFromContext(ctx)
		require.Equal(t, "10.0.0.2", ip)
	})

	t.Run("x-forwarded-for takes precedence", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"x-forwarded-for": "10.0.0.1",
			"x-real-ip":       "10.0.0.2",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		ip := ipAddrFromContext(ctx)
		require.Equal(t, "10.0.0.1", ip)
	})

	t.Run("peer address with port", func(t *testing.T) {
		ctx := peer.NewContext(context.Background(), &peer.Peer{
			Addr: &net.TCPAddr{IP: net.IPv4(192, 168, 1, 1), Port: 5432},
		})
		ip := ipAddrFromContext(ctx)
		require.Equal(t, "192.168.1.1", ip)
	})

	t.Run("peer address without port", func(t *testing.T) {
		ctx := peer.NewContext(context.Background(), &peer.Peer{
			Addr: &net.IPAddr{IP: net.IPv4(192, 168, 1, 1)},
		})
		ip := ipAddrFromContext(ctx)
		require.Equal(t, "192.168.1.1", ip)
	})
}

func TestServerStreamWithContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), "test-key", "test-val")
	ss := &serverStreamWithContext{
		ServerStream: &mockServerStream{ctx: context.Background()},
		ctx:          ctx,
	}
	require.Equal(t, ctx, ss.Context())
}
