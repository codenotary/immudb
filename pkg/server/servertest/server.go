/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"sync"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/server"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

type BuffDialer func(context.Context, string) (net.Conn, error)

type bufconnServer struct {
	immuServer *server.ImmuServer
	m          sync.Mutex
	pgsqlwg    sync.WaitGroup
	Lis        *bufconn.Listener
	Server     *ServerMock
	Options    *server.Options
	GrpcServer *grpc.Server
	Dialer     BuffDialer
	quit       chan struct{}
}

func NewBufconnServer(options *server.Options) *bufconnServer {
	options.Port = 0
	immuserver := server.DefaultServer().WithOptions(options).(*server.ImmuServer)
	bs := &bufconnServer{
		quit:    make(chan struct{}),
		Lis:     bufconn.Listen(bufSize),
		Options: options,
		GrpcServer: grpc.NewServer(
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(server.ErrorMapper, immuserver.KeepAliveSessionInterceptor, auth.ServerUnaryInterceptor, immuserver.SessionAuthInterceptor)),
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(server.ErrorMapperStream, immuserver.KeepALiveSessionStreamInterceptor, auth.ServerStreamInterceptor)),
		),
		immuServer: immuserver,
	}

	return bs
}

func (bs *bufconnServer) Start() error {
	bs.m.Lock()
	defer bs.m.Unlock()

	bs.Dialer = func(ctx context.Context, s string) (net.Conn, error) {
		return bs.Lis.Dial()
	}

	bs.Server = &ServerMock{Srv: bs.immuServer}

	bs.pgsqlwg.Add(1)

	if err := bs.Server.Initialize(); err != nil {
		return err
	}
	// in order to know the port of pgsql listener (auto assigned by os thanks 0 value) we need to wait
	bs.pgsqlwg.Done()

	schema.RegisterImmuServiceServer(bs.GrpcServer, bs.Server)

	go func() {
		if err := bs.GrpcServer.Serve(bs.Lis); err != nil {
			log.Println(err)
		}
	}()

	if bs.Options.PgsqlServer {
		go func() {
			if err := bs.Server.Srv.PgsqlSrv.Serve(); err != nil {
				log.Println(err)
			}
		}()
	}

	return nil
}

func (bs *bufconnServer) Stop() error {
	bs.m.Lock()
	defer bs.m.Unlock()
	if err := bs.Server.Srv.CloseDatabases(); err != nil {
		return err
	}
	if err := bs.Server.Srv.PgsqlSrv.Stop(); err != nil {
		return err
	}

	bs.GrpcServer.Stop()

	return nil
}

func (bs *bufconnServer) WaitForPgsqlListener() {
	bs.m.Lock()
	defer bs.m.Unlock()
	bs.pgsqlwg.Wait()
}
