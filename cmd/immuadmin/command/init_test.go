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

package immuadmin

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
	"os"
	"testing"
)

func TestOptions(t *testing.T) {
	opts := Options()
	assert.IsType(t, &client.Options{}, opts)
}

func TestOptionsMtls(t *testing.T) {
	viper.Set("mtls", true)
	opts := Options()
	assert.IsType(t, &client.Options{}, opts)
	viper.Reset()
}

const bufSize = 1024 * 1024

var lis *bufconn.Listener

var immuServer *server.ImmuServer
var cli client.ImmuClient
var username string
var plainPass string

func setup() {
	cleanup()
	immuServer = newServer(true)
	immuServer.Start()
	nm, _ := timestamp.NewTdefault()
	tss := client.NewTimestampService(nm)
	token := login()
	cli = newClient(true, token).WithTimestampService(tss)
	resp, err := cli.UseDatabase(context.Background(), &schema.Database{
		Databasename: immuServer.Options.GetDefaultDbName(),
	})
	if err != nil {
		panic(err)
	}
	cli = newClient(true, resp.Token).WithTimestampService(tss)
}

func newServer(authRequired bool) *server.ImmuServer {
	is := server.DefaultServer()
	is = is.WithOptions(is.Options.WithAuth(authRequired).WithInMemoryStore(true))
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

	immuclient := client.DefaultClient().WithOptions(client.DefaultOptions().WithAuth(withToken).WithDialOptions(&dialOptions))
	clientConn, _ := immuclient.Connect(context.TODO())
	immuclient.WithClientConn(clientConn)
	serviceClient := schema.NewImmuServiceClient(clientConn)
	immuclient.WithServiceClient(serviceClient)
	rootService := client.NewRootService(serviceClient, cache.NewFileCache("."), logger.NewSimpleLogger("test", os.Stdout))
	immuclient.WithRootService(rootService)

	return immuclient
}
func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
}
func cleanup() {
	// delete files and folders created by tests
	if err := os.Remove(".root-"); err != nil {
		log.Println(err)
	}
}

func login() string {
	c := newClient(false, "")
	ctx := context.Background()
	r, err := c.Login(ctx, []byte(username), []byte(plainPass))
	if err != nil {
		log.Fatal(err)
	}
	return string(r.GetToken())
}
