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
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/client/rootservice"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

var dirname = "./test"

func TestDefaultAuditor(t *testing.T) {
	da, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		&[]grpc.DialOption{},
		"immudb",
		"immudb",
		"ignore",
		TamperingAlertConfig{},
		nil,
		nil,
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.Root, *schema.Root) {},
		logger.NewSimpleLogger("test", os.Stdout))
	assert.Nil(t, err)
	assert.IsType(t, &defaultAuditor{}, da)
	os.RemoveAll(dirname)
}

func TestDefaultAuditorRunOnEmptyDb(t *testing.T) {
	bs := servertest.NewBufconnServer(server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword))
	bs.Start()

	ds := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}

	var clientConn *grpc.ClientConn
	clientConn, err := grpc.Dial("add", ds...)
	require.NoError(t, err)
	serviceClient := schema.NewImmuServiceClient(clientConn)

	da, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		&ds,
		"immudb",
		"immudb",
		"ignore",
		TamperingAlertConfig{},
		serviceClient,
		rootservice.NewImmudbUUIDProvider(serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.Root, *schema.Root) {},
		logger.NewSimpleLogger("test", os.Stdout))
	assert.Nil(t, err)
	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	os.RemoveAll(dirname)
}

func TestDefaultAuditorRunOnDb(t *testing.T) {
	bs := servertest.NewBufconnServer(server.Options{}.WithAuth(true).WithInMemoryStore(true).WithAdminPassword(auth.SysAdminPassword))
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
		"ignore",
		TamperingAlertConfig{},
		serviceClient,
		rootservice.NewImmudbUUIDProvider(serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.Root, *schema.Root) {},
		logger.NewSimpleLogger("test", os.Stdout))
	require.NoError(t, err)

	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	os.RemoveAll(dirname)
}

func TestDefaultAuditorRunOnDbWithSignature(t *testing.T) {
	pkey_path := "./../../../test/signer/ec3.key"
	bs := servertest.NewBufconnServer(
		server.Options{}.
			WithAuth(true).
			WithInMemoryStore(true).
			WithSigningKey(pkey_path).
			WithAdminPassword(auth.SysAdminPassword))
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
		TamperingAlertConfig{},
		serviceClient,
		rootservice.NewImmudbUUIDProvider(serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.Root, *schema.Root) {},
		logger.NewSimpleLogger("test", os.Stdout))
	assert.Nil(t, err)

	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	assert.Nil(t, err)
	os.RemoveAll(dirname)
}

func TestDefaultAuditorRunOnDbWithFailSignature(t *testing.T) {
	serviceClient := clienttest.NewImmuServiceClientMock()
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
			Databases: []*schema.Database{{Databasename: "sysdb"}},
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
		TamperingAlertConfig{},
		serviceClient,
		rootservice.NewImmudbUUIDProvider(serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.Root, *schema.Root) {},
		logger.NewSimpleLogger("test", os.Stdout))
	assert.Nil(t, err)

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
		TamperingAlertConfig{},
		&serviceClient,
		rootservice.NewImmudbUUIDProvider(&serviceClient),
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

func TestPublishTamperingAlert(t *testing.T) {
	a := &defaultAuditor{
		alertConfig: TamperingAlertConfig{
			URL:      "http://some-non-existent-url.com",
			Username: "some-username",
			Password: "some-password",
			Client:   &http.Client{},
			PublishFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     http.StatusText(http.StatusNoContent),
					StatusCode: http.StatusNoContent,
					Body:       ioutil.NopCloser(strings.NewReader("All good")),
				}, nil
			},
		},
	}
	// test happy path
	err := a.publishTamperingAlert(
		"some-db",
		Root{Index: 1, Hash: "root-hash-1"},
		Root{Index: 2, Hash: "root-hash-2"},
	)
	require.NoError(t, err)

	// test unexpected HTTP status code
	a.alertConfig.PublishFunc = func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			Status:     http.StatusText(http.StatusInternalServerError),
			StatusCode: http.StatusInternalServerError,
			Body:       ioutil.NopCloser(strings.NewReader("Some error")),
		}, nil
	}
	err = a.publishTamperingAlert(
		"some-db2",
		Root{
			Index:     11,
			Hash:      "root-hash-11",
			Signature: Signature{Signature: "sig11", PublicKey: "pk11"}},
		Root{
			Index:     22,
			Hash:      "root-hash-22",
			Signature: Signature{Signature: "sig22", PublicKey: "pk22"}},
	)
	require.Error(t, err)
	require.Equal(
		t,
		"POST http://some-non-existent-url.com request with body "+
			`{"username":"some-username","password":"some-password",`+
			`"db":"some-db2","previous_root":{"index":11,"hash":"root-hash-11",`+
			`"signature":{"signature":"sig11","public_key":"pk11"}},`+
			`"current_root":{"index":22,"hash":"root-hash-22",`+
			`"signature":{"signature":"sig22","public_key":"pk22"}}}: got unexpected `+
			"response status Internal Server Error with response body Some error",
		err.Error())

	// test error sending request (real HTTP request)
	a.alertConfig.Client = nil
	a.alertConfig.RequestTimeout = 1 * time.Second
	err = a.publishTamperingAlert(
		"some-db3",
		Root{Index: 111, Hash: "root-hash-111"},
		Root{Index: 222, Hash: "root-hash-222"},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such host")

	// test error creating request
	a.alertConfig.Client = nil
	a.alertConfig.RequestTimeout = 1 * time.Second
	a.alertConfig.URL = string([]byte{0})
	err = a.publishTamperingAlert(
		"some-db4",
		Root{Index: 1111, Hash: "root-hash-1111"},
		Root{Index: 2222, Hash: "root-hash-2222"},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid control character in URL")
}
