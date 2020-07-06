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
package gw

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
}

const clientDir = "./handlers_test_client_dir"

var username string
var plainPass string

func newServer() *server.ImmuServer {
	is := server.DefaultServer()
	is = is.WithOptions(
		is.Options.
			WithAuth(true).
			WithInMemoryStore(true).
			WithCorruptionCheck(false).
			WithMetricsServer(false))
	auth.AuthEnabled = is.Options.GetAuth()
	username, plainPass = auth.SysAdminUsername, auth.SysAdminPassword
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer(
		grpc.UnaryInterceptor(auth.ServerUnaryInterceptor),
		grpc.StreamInterceptor(auth.ServerStreamInterceptor),
	)
	schema.RegisterImmuServiceServer(s, is)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()
	return is
}

func newClient(withToken bool, token string) client.ImmuClient {
	os.RemoveAll(clientDir)
	if err := os.Mkdir(clientDir, 0755); err != nil {
		log.Fatalf("error creating client dir %s: %v", clientDir, err)
	}
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}
	if withToken {
		dialOptions = append(
			dialOptions,
			grpc.WithUnaryInterceptor(auth.ClientUnaryInterceptor(token)),
			grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)),
		)
	}
	immuclient := client.DefaultClient().WithOptions(
		client.DefaultOptions().WithAuth(withToken).WithDialOptions(&dialOptions))
	clientConn, _ := immuclient.Connect(context.TODO())
	immuclient.WithClientConn(clientConn)
	serviceClient := schema.NewImmuServiceClient(clientConn)
	immuclient.WithServiceClient(serviceClient)
	rootService := client.NewRootService(
		serviceClient,
		cache.NewFileCache(clientDir),
		logger.NewSimpleLogger("handlers_test", os.Stdout))
	immuclient.WithRootService(rootService)
	return immuclient
}

type ntpMock struct {
	t time.Time
}

func (n *ntpMock) Now() time.Time {
	return n.t
}
func newNtpMock() (timestamp.TsGenerator, error) {
	i, err := strconv.ParseInt("1405544146", 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	tm := time.Unix(i, 0)
	return &ntpMock{tm}, nil
}

func cleanup() {
	if err := os.RemoveAll(clientDir); err != nil {
		log.Println(err)
	}
}

func login() string {
	ic := newClient(false, "")
	ctx := context.Background()
	r, err := ic.Login(ctx, []byte(username), []byte(plainPass))
	if err != nil {
		log.Fatal(err)
	}
	return string(r.GetToken())
}

func TestGw(t *testing.T) {
	cleanup()
	defer cleanup()
	immuServer := newServer()
	immuServer.Start()
	nm, _ := newNtpMock()
	token := login()
	tss := client.NewTimestampService(nm)
	immuClient := newClient(true, token).WithTimestampService(tss)
	resp, err := immuClient.UseDatabase(context.Background(), &schema.Database{
		Databasename: immuServer.Options.GetDefaultDbName(),
	})
	if err != nil {
		panic(err)
	}
	immuClient = newClient(true, resp.Token).WithTimestampService(tss)
	require.NoError(t, immuClient.HealthCheck(context.Background()))
	mux := runtime.NewServeMux()
	testSafeSetHandler(t, mux, immuClient)
	testSetHandler(t, mux, immuClient)
	testSafeGetHandler(t, mux, immuClient)
	testHistoryHandler(t, mux, immuClient)
	testSafeReferenceHandler(t, mux, immuClient)
	testSafeZAddHandler(t, mux, immuClient)
}
