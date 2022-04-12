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

package integration

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/codenotary/immudb/pkg/client/homedir"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/auditor"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var dirname = "./test"

func TestDefaultAuditorRunOnEmptyDb(t *testing.T) {
	defer os.RemoveAll(dirname)

	bs := servertest.NewBufconnServer(server.DefaultOptions().WithDir(dirname).WithAuth(true).WithAdminPassword(auth.SysAdminPassword))
	bs.Start()
	defer bs.Stop()

	ds := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}

	var clientConn *grpc.ClientConn
	clientConn, err := grpc.Dial("add", ds...)
	require.NoError(t, err)
	serviceClient := schema.NewImmuServiceClient(clientConn)

	da, err := auditor.DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		ds,
		"immudb",
		"immudb",
		nil,
		nil,
		auditor.AuditNotificationConfig{},
		serviceClient,
		state.NewUUIDProvider(serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", os.Stdout),
		nil)
	require.Nil(t, err)
	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	require.Nil(t, err)
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

func TestDefaultAuditorRunOnDb(t *testing.T) {
	defer os.RemoveAll(dirname)

	bs := servertest.NewBufconnServer(server.DefaultOptions().WithDir(dirname).WithAuth(true).WithAdminPassword(auth.SysAdminPassword))
	bs.Start()
	defer bs.Stop()

	ctx := context.Background()
	pr := &PasswordReader{
		Pass: []string{"immudb"},
	}

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf).WithHds(homedir.NewHomedirService())
	cliopt := client.DefaultOptions().WithDialOptions(dialOptions).WithPasswordReader(pr)

	cliopt.PasswordReader = pr
	cliopt.DialOptions = dialOptions

	cli, _ := client.NewImmuClient(cliopt)
	cli.WithTokenService(ts)
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

	da, err := auditor.DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		ds,
		"immudb",
		"immudb",
		nil,
		nil,
		auditor.AuditNotificationConfig{},
		serviceClient,
		state.NewUUIDProvider(serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", os.Stdout),
		nil)
	require.NoError(t, err)

	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	require.Nil(t, err)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	require.Nil(t, err)
}

func TestRepeatedAuditorRunOnDb(t *testing.T) {
	defer os.RemoveAll(dirname)

	bs := servertest.NewBufconnServer(server.DefaultOptions().WithDir(dirname).WithAuth(true).WithAdminPassword(auth.SysAdminPassword))
	bs.Start()
	defer bs.Stop()

	ctx := context.Background()
	pr := &PasswordReader{
		Pass: []string{"immudb"},
	}

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf).WithHds(homedir.NewHomedirService())
	cliopt := client.DefaultOptions().WithDialOptions(dialOptions).WithPasswordReader(pr)

	cliopt.PasswordReader = pr
	cliopt.DialOptions = dialOptions

	cli, _ := client.NewImmuClient(cliopt)
	cli.WithTokenService(ts)
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

	alertConfig := auditor.AuditNotificationConfig{
		URL:      "http://some-non-existent-url.com",
		Username: "some-username",
		Password: "some-password",
		PublishFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				Status:     http.StatusText(http.StatusNoContent),
				StatusCode: http.StatusNoContent,
				Body:       ioutil.NopCloser(strings.NewReader("All good")),
			}, nil
		},
	}

	da, err := auditor.DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		ds,
		"immudb",
		"immudb",
		[]string{"SomeNonExistentDb", ""},
		nil,
		alertConfig,
		serviceClient,
		state.NewUUIDProvider(serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", os.Stdout),
		nil)
	require.NoError(t, err)

	auditorStop := make(chan struct{}, 1)
	auditorDone := make(chan struct{}, 1)

	go da.Run(time.Duration(100)*time.Millisecond, false, auditorStop, auditorDone)

	time.Sleep(time.Duration(2) * time.Second)

	auditorStop <- struct{}{}
	<-auditorDone
}

func TestDefaultAuditorRunOnDbWithSignature(t *testing.T) {
	pk, err := signer.ParsePublicKeyFile("./../../test/signer/ec3.pub")
	require.NoError(t, err)

	testDefaultAuditorRunOnDbWithSignature(t, pk)
}

func TestDefaultAuditorRunOnDbWithSignatureFromState(t *testing.T) {
	testDefaultAuditorRunOnDbWithSignature(t, nil)
}

func testDefaultAuditorRunOnDbWithSignature(t *testing.T, pk *ecdsa.PublicKey) {
	defer os.RemoveAll(dirname)

	pKeyPath := "./../../test/signer/ec3.key"
	bs := servertest.NewBufconnServer(
		server.DefaultOptions().
			WithDir(dirname).
			WithAuth(true).
			WithSigningKey(pKeyPath).
			WithAdminPassword(auth.SysAdminPassword))
	bs.Start()
	defer bs.Stop()

	ctx := context.Background()
	pr := &PasswordReader{
		Pass: []string{"immudb"},
	}

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	tkf := cmdtest.RandString()
	ts := tokenservice.NewFileTokenService().WithTokenFileName(tkf).WithHds(homedir.NewHomedirService())
	cliopt := client.DefaultOptions().WithDialOptions(dialOptions).WithPasswordReader(pr)

	cliopt.PasswordReader = pr
	cliopt.DialOptions = dialOptions

	cli, _ := client.NewImmuClient(cliopt)
	cli.WithTokenService(ts)
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

	da, err := auditor.DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		ds,
		"immudb",
		"immudb",
		nil,
		pk,
		auditor.AuditNotificationConfig{},
		serviceClient,
		state.NewUUIDProvider(serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", os.Stdout),
		nil)
	require.Nil(t, err)

	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	require.Nil(t, err)
	err = da.Run(time.Duration(10), true, context.TODO().Done(), auditorDone)
	require.Nil(t, err)
}
