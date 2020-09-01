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
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

func TestDefaultAuditorRunOnDbWithSignature(t *testing.T) {
	pkey_path := "./../../../test/signer/ec3.key"
	bs := servertest.NewBufconnServer(server.Options{}.WithAuth(true).WithInMemoryStore(true).WithSignaturePrivateKey(pkey_path))
	bs.Start()

	ctx := context.Background()
	pr := &PasswordReader{
		Pass: []string{"immudb"},
	}

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(client.NewHomedirService())
	cliopt := client.DefaultOptions().WithDialOptions(&dialOptions).WithPasswordReader(pr).WithTokenService(ts)

	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions

	cli, _ := client.NewImmuClient(cliopt)
	lresp, err := cli.Login(ctx, []byte("immudb"), []byte("immudb"))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lresp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = cli.Set(ctx, []byte(`key`), []byte(`val`))
	require.NoError(t, err)

	ds := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}

	var clientConn *grpc.ClientConn
	clientConn, err = grpc.Dial("add", ds...)
	require.NoError(t, err)
	serviceClient := schema.NewImmuServiceClient(clientConn)

	da, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		&ds,
		"immudb",
		"immudb",
		"validate",
		serviceClient,
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.Root, *schema.Root) {},
		logger.NewSimpleLogger("test", os.Stdout))

	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	os.RemoveAll(dirname)
}

func TestDefaultAuditorRunOnDbWithFailSignature(t *testing.T) {
	serviceClient := clienttest.ImmuServiceClientMock{}
	serviceClient.CurrentRootF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.Root, error) {
		return schema.NewRoot(), nil
	}
	serviceClient.LoginF = func(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{
			Token: "token",
		}, nil
	}
	serviceClient.DatabaseListF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error) {
		return &schema.DatabaseListResponse{
			Databases: []*schema.Database{&schema.Database{Databasename: "sysdb"}},
		}, nil
	}
	serviceClient.UseDatabaseF = func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error) {
		return &schema.UseDatabaseReply{
			Token: "sometoken",
		}, nil
	}
	serviceClient.LogoutF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
		return &empty.Empty{}, nil
	}

	da, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		&[]grpc.DialOption{
			grpc.WithInsecure(),
		},
		"immudb",
		"immudb",
		"validate",
		serviceClient,
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.Root, *schema.Root) {},
		logger.NewSimpleLogger("test", os.Stdout))

	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	os.RemoveAll(dirname)
}

func TestDefaultAuditorRunOnDbWithWrongAuditSignatureMode(t *testing.T) {
	serviceClient := clienttest.ImmuServiceClientMock{}
	_, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		&[]grpc.DialOption{
			grpc.WithInsecure(),
		},
		"immudb",
		"immudb",
		"wrong",
		serviceClient,
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.Root, *schema.Root) {},
		logger.NewSimpleLogger("test", os.Stdout))
	assert.Errorf(t, err, "auditSignature allowed values are 'validate' or 'ignore'")
}

type PasswordReader struct {
	Pass       []string
	callNumber int
}

func (pr *PasswordReader) Read(msg string) ([]byte, error) {
	if len(pr.Pass) <= pr.callNumber {
		log.Fatal("Application requested the password more times than number of passwords supplied")
	}
	pass := []byte(pr.Pass[pr.callNumber])
	pr.callNumber++
	return pass, nil
}

func makeAuditor() (Auditor, error) {
	ds := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}

	var clientConn *grpc.ClientConn
	clientConn, err := grpc.Dial("add", ds...)
	if err != nil {
		return nil, err
	}
	serviceClient := schema.NewImmuServiceClient(clientConn)

	da, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		&ds,
		"immudb",
		"immudb",
		"ignore",
		serviceClient,
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.Root, *schema.Root) {},
		logger.NewSimpleLogger("test", os.Stdout))
	return da, err
}

func newServer() *server.ImmuServer {
	is := server.DefaultServer()
	is = is.WithOptions(is.Options.WithAuth(true).WithInMemoryStore(true)).(*server.ImmuServer)
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
