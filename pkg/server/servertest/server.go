/*
Copyright 2019-2020 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package servertest

import (
	"context"
	"log"
	"net"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

type BuffDialer func(context.Context, string) (net.Conn, error)

type bufconnServer struct {
	Lis        *bufconn.Listener
	Server     *server.ImmuServer
	Options    *server.Options
	GrpcServer *grpc.Server
	Dialer     BuffDialer
}

func NewBufconnServer(options server.Options) *bufconnServer {
	bs := &bufconnServer{
		Lis:     bufconn.Listen(bufSize),
		Options: &options,
		GrpcServer: grpc.NewServer(
			grpc.UnaryInterceptor(auth.ServerUnaryInterceptor),
			grpc.StreamInterceptor(auth.ServerStreamInterceptor),
		),
	}
	return bs
}

func (bs *bufconnServer) Start() {
	bs.Server = server.DefaultServer().WithOptions(*bs.Options).(*server.ImmuServer)
	bs.Dialer = func(ctx context.Context, s string) (net.Conn, error) {
		return bs.Lis.Dial()
	}
	schema.RegisterImmuServiceServer(bs.GrpcServer, bs.Server)
	go func() {
		if err := bs.GrpcServer.Serve(bs.Lis); err != nil {
			log.Fatal(err)
		}
	}()
	bs.Server.Start()
}
