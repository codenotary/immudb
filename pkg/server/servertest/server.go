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

package servertest

import (
	"context"
	"log"
	"net"
	"sync"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/rs/xid"
	"google.golang.org/grpc"

	"github.com/codenotary/immudb/pkg/server"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

type BuffDialer func(context.Context, string) (net.Conn, error)

type BufconnServer struct {
	immuServer *server.ImmuServer
	m          sync.Mutex
	pgsqlwg    sync.WaitGroup
	Lis        *bufconn.Listener
	Server     *ServerMock
	Options    *server.Options
	GrpcServer *grpc.Server
	Dialer     BuffDialer
	quit       chan struct{}
	uuid       xid.ID
}

// NewBuffconnServer creates new test server instance that uses grpc's buffconn connection method
// to talk to its clients - communication happens using memory buffers instead of TCP connections.
func NewBufconnServer(options *server.Options) *BufconnServer {
	options.Port = 0
	immuserver := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	uuid := xid.New()

	bs := &BufconnServer{
		quit:       make(chan struct{}),
		Lis:        bufconn.Listen(bufSize),
		Options:    options,
		immuServer: immuserver,
		Server:     &ServerMock{Srv: immuserver},
		uuid:       uuid,
	}

	return bs
}

func (bs *BufconnServer) GetUUID() xid.ID {
	return bs.uuid
}

func (bs *BufconnServer) SetUUID(id xid.ID) {
	bs.uuid = id
}

func (bs *BufconnServer) setupGrpcServer() {
	uuidContext := server.NewUUIDContext(bs.uuid)

	bs.GrpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			server.ErrorMapper,
			bs.immuServer.KeepAliveSessionInterceptor,
			uuidContext.UUIDContextSetter,
			auth.ServerUnaryInterceptor,
			bs.immuServer.SessionAuthInterceptor,
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			server.ErrorMapperStream,
			bs.immuServer.KeepALiveSessionStreamInterceptor,
			uuidContext.UUIDStreamContextSetter,
			auth.ServerStreamInterceptor,
		)),
	)
}

func (bs *BufconnServer) Start() error {
	bs.m.Lock()
	defer bs.m.Unlock()

	bs.setupGrpcServer()

	bs.Dialer = func(ctx context.Context, s string) (net.Conn, error) {
		return bs.Lis.Dial()
	}

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

func (bs *BufconnServer) Stop() error {
	bs.m.Lock()
	defer bs.m.Unlock()
	if err := bs.Server.Srv.CloseDatabases(); err != nil {
		return err
	}

	if bs.Server.Srv.PgsqlSrv != nil {
		if err := bs.Server.Srv.PgsqlSrv.Stop(); err != nil {
			return err
		}
	}

	if bs.GrpcServer != nil {
		bs.GrpcServer.Stop()
		bs.GrpcServer = nil
	}

	return nil
}

func (bs *BufconnServer) WaitForPgsqlListener() {
	bs.m.Lock()
	defer bs.m.Unlock()
	bs.pgsqlwg.Wait()
}

func (bs *BufconnServer) NewClient(options *client.Options) client.ImmuClient {
	return client.NewClient().WithOptions(
		options.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}),
	)
}

func (bs *BufconnServer) NewAuthenticatedClient(options *client.Options) (client.ImmuClient, error) {
	client := bs.NewClient(options)

	err := client.OpenSession(
		context.Background(),
		[]byte(auth.SysAdminUsername),
		[]byte(bs.Server.Srv.Options.AdminPassword),
		bs.Server.Srv.Options.GetDefaultDBName(),
	)
	if err != nil {
		return nil, err
	}

	return client, nil
}
