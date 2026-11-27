/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestGRPCHandlerPanicIsRecovered(t *testing.T) {
	s := DefaultServer().WithOptions(DefaultOptions().
		WithDir(t.TempDir()).
		WithPort(0).
		WithPgsqlServer(false).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword)).(*ImmuServer)
	defer s.CloseDatabases()
	require.NoError(t, s.Initialize())

	panicUnary := func(srv any, ctx context.Context, _ func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/test.Panic/Unary"}
		return interceptor(ctx, nil, info, func(context.Context, any) (any, error) { panic("boom") })
	}
	panicStream := func(any, grpc.ServerStream) error { panic("boom") }
	s.GrpcServer.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Panic",
		HandlerType: (*any)(nil),
		Methods:     []grpc.MethodDesc{{MethodName: "Unary", Handler: panicUnary}},
		Streams:     []grpc.StreamDesc{{StreamName: "Stream", Handler: panicStream, ServerStreams: true}},
	}, struct{}{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go s.GrpcServer.Serve(lis)
	defer s.GrpcServer.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	err = conn.Invoke(context.Background(), "/test.Panic/Unary", &emptypb.Empty{}, &emptypb.Empty{})
	require.Equal(t, codes.Internal, status.Code(err))

	cs, err := conn.NewStream(context.Background(), &grpc.StreamDesc{ServerStreams: true}, "/test.Panic/Stream")
	require.NoError(t, err)
	require.NoError(t, cs.SendMsg(&emptypb.Empty{}))
	require.NoError(t, cs.CloseSend())
	require.Equal(t, codes.Internal, status.Code(cs.RecvMsg(&emptypb.Empty{})))
}
