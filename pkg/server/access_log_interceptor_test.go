package server

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

func TestAccessLogInterceptors(t *testing.T) {
	opts := DefaultOptions().
		WithDir(t.TempDir()).
		WithLogAccess(true).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword)

	s := DefaultServer().WithOptions(opts).(*ImmuServer)
	defer s.CloseDatabases()

	logger := &mockLogger{captureLogs: true}
	s.WithLogger(logger)

	err := s.Initialize()
	require.NoError(t, err)

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.IPAddr{IP: net.IPv4(192, 168, 1, 1)}})

	t.Run("unary interceptor", func(t *testing.T) {
		called := false
		_, err := s.AccessLogInterceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "testMethod"}, func(ctx context.Context, req interface{}) (interface{}, error) {
			called = true
			return nil, nil
		})
		require.NoError(t, err)

		require.True(t, called)
		require.NotEmpty(t, logger.logs)

		lastLine := logger.logs[len(logger.logs)-1]
		require.Contains(t, lastLine, "ip=192.168.1.1")
		require.Contains(t, lastLine, "user=,")
		require.Contains(t, lastLine, "method=testMethod")
		require.NotContains(t, lastLine, "error=")
	})

	t.Run("streaming interceptor", func(t *testing.T) {
		called := false
		err := s.AccessLogStreamInterceptor(nil, &mockServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "testMethod"}, func(srv interface{}, stream grpc.ServerStream) error {
			called = true
			return fmt.Errorf("test error")
		})
		require.Error(t, err)

		require.True(t, called)
		require.NotEmpty(t, logger.logs)

		lastLine := logger.logs[len(logger.logs)-1]
		require.Contains(t, lastLine, "ip=192.168.1.1")
		require.Contains(t, lastLine, "user=,")
		require.Contains(t, lastLine, "method=testMethod")
		require.Contains(t, lastLine, "error=test error")
	})
}
