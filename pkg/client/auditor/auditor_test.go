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

package auditor

import (
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
	"os"
	"testing"
	"time"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

var dirname = "./test"

func TestDefaultAuditor(t *testing.T) {
	da, err := makeAuditor()
	assert.Nil(t, err)
	assert.IsType(t, &defaultAuditor{}, da)
	os.RemoveAll(dirname)
}

func TestDefaultAuditorRunOnEmptyDb(t *testing.T) {
	immuServer := newServer()
	immuServer.Start()
	da, _ := makeAuditor()
	auditorDone := make(chan struct{}, 2)
	err := da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	os.RemoveAll(dirname)
}

func TestDefaultAuditorRunOnDb(t *testing.T) {
	immuServer := newServer()
	immuServer.Start()
	token := login()

	nm, _ := timestamp.NewTdefault()
	tss := client.NewTimestampService(nm)
	cli := newClient(true, token).WithTimestampService(tss)
	resp, err := cli.UseDatabase(context.Background(), &schema.Database{
		Databasename: immuServer.Options.GetDefaultDbName(),
	})
	if err != nil {
		panic(err)
	}
	cli = newClient(true, resp.Token).WithTimestampService(tss)
	cli.Set(context.TODO(), []byte(`key`), []byte(`val`))
	da, _ := makeAuditor()
	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	os.RemoveAll(dirname)
}

func makeAuditor() (Auditor, error) {
	ds := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}
	da, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		&ds,
		"immudb",
		"immudb",
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.Root, *schema.Root) {},
		nil)
	return da, err
}

func newServer() *server.ImmuServer {
	is := server.DefaultServer()
	is = is.WithOptions(is.Options.WithAuth(true).WithInMemoryStore(true))
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

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
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

func login() string {
	c := newClient(false, "")
	ctx := context.Background()
	r, err := c.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		log.Fatal(err)
	}
	return string(r.GetToken())
}
