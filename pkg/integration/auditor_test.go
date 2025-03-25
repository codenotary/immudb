/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package integration

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/codenotary/immudb/pkg/client/homedir"
	"github.com/codenotary/immudb/pkg/client/tokenservice"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/auditor"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type mockHomedir struct {
	files map[string][]byte
	m     sync.RWMutex
}

func newMockHomedir() homedir.HomedirService {
	return &mockHomedir{
		files: make(map[string][]byte),
	}
}

func (h *mockHomedir) WriteFileToUserHomeDir(content []byte, pathToFile string) error {
	h.m.Lock()
	defer h.m.Unlock()

	h.files[pathToFile] = content
	return nil
}

func (h *mockHomedir) FileExistsInUserHomeDir(pathToFile string) (bool, error) {
	h.m.RLock()
	defer h.m.RUnlock()

	_, exists := h.files[pathToFile]
	return exists, nil
}

func (h *mockHomedir) ReadFileFromUserHomeDir(pathToFile string) (string, error) {
	h.m.RLock()
	defer h.m.RUnlock()

	data, exists := h.files[pathToFile]
	if !exists {
		return "", os.ErrNotExist
	}

	return string(data), nil
}

func (h *mockHomedir) DeleteFileFromUserHomeDir(pathToFile string) error {
	h.m.Lock()
	defer h.m.Unlock()

	delete(h.files, pathToFile)
	return nil
}

func TestDefaultAuditorRunOnEmptyDb(t *testing.T) {
	bs := servertest.NewBufconnServer(server.
		DefaultOptions().
		WithDir(t.TempDir()),
	)

	bs.Start()
	defer bs.Stop()

	ds := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

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
		cache.NewHistoryFileCache(t.TempDir()),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", os.Stdout),
		nil,
	)
	require.NoError(t, err)
	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.Background().Done(), auditorDone)
	require.NoError(t, err)
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
	bs := servertest.NewBufconnServer(server.
		DefaultOptions().
		WithDir(t.TempDir()),
	)
	bs.Start()
	defer bs.Stop()

	ctx := context.Background()
	pr := &PasswordReader{
		Pass: []string{"immudb"},
	}

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	tkf := cmdtest.RandString()
	ts := tokenservice.
		NewFileTokenService().
		WithTokenFileName(tkf).
		WithHds(newMockHomedir())
	cliopt := client.
		DefaultOptions().
		WithDialOptions(dialOptions).
		WithPasswordReader(pr).
		WithDir(t.TempDir())

	cliopt.PasswordReader = pr
	cliopt.DialOptions = dialOptions

	cli, err := client.NewImmuClient(cliopt)
	require.NoError(t, err)
	cli.WithTokenService(ts)

	lresp, err := cli.Login(ctx, []byte("immudb"), []byte("immudb"))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lresp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = cli.Set(ctx, []byte(`key`), []byte(`val`))
	require.NoError(t, err)

	ds := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	var clientConn *grpc.ClientConn
	clientConn, err = grpc.Dial("add", ds...)
	require.NoError(t, err)
	serviceClient := schema.NewImmuServiceClient(clientConn)

	auditorDir := t.TempDir()

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
		cache.NewHistoryFileCache(auditorDir),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", os.Stdout),
		nil)
	require.NoError(t, err)

	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.Background().Done(), auditorDone)
	require.NoError(t, err)
	err = da.Run(time.Duration(10), true, context.Background().Done(), auditorDone)
	require.NoError(t, err)
}

func TestRepeatedAuditorRunOnDb(t *testing.T) {
	bs := servertest.NewBufconnServer(
		server.DefaultOptions().
			WithDir(t.TempDir()).
			WithAuth(true).
			WithAdminPassword(auth.SysAdminPassword),
	)
	bs.Start()
	defer bs.Stop()

	ctx := context.Background()
	pr := &PasswordReader{
		Pass: []string{"immudb"},
	}

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	tkf := cmdtest.RandString()
	ts := tokenservice.
		NewFileTokenService().
		WithTokenFileName(tkf).
		WithHds(newMockHomedir())
	cliopt := client.DefaultOptions().
		WithDir(t.TempDir()).
		WithDialOptions(dialOptions).
		WithPasswordReader(pr)

	cliopt.PasswordReader = pr
	cliopt.DialOptions = dialOptions

	cli, err := client.NewImmuClient(cliopt)
	require.NoError(t, err)
	cli.WithTokenService(ts)
	lresp, err := cli.Login(ctx, []byte("immudb"), []byte("immudb"))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lresp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = cli.Set(ctx, []byte(`key`), []byte(`val`))
	require.NoError(t, err)

	ds := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
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

	auditorDir := t.TempDir()

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
		cache.NewHistoryFileCache(auditorDir),
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
	pKeyPath := "./../../test/signer/ec3.key"
	bs := servertest.NewBufconnServer(
		server.DefaultOptions().
			WithDir(t.TempDir()).
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
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	tkf := cmdtest.RandString()
	ts := tokenservice.
		NewFileTokenService().
		WithTokenFileName(tkf).
		WithHds(newMockHomedir())
	cliopt := client.
		DefaultOptions().
		WithDir(t.TempDir()).
		WithDialOptions(dialOptions).
		WithPasswordReader(pr)

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
		grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	var clientConn *grpc.ClientConn
	clientConn, err = grpc.Dial("add", ds...)
	require.NoError(t, err)
	serviceClient := schema.NewImmuServiceClient(clientConn)

	auditorDir := t.TempDir()

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
		cache.NewHistoryFileCache(auditorDir),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", os.Stdout),
		nil)
	require.NoError(t, err)

	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.Background().Done(), auditorDone)
	require.NoError(t, err)
	err = da.Run(time.Duration(10), true, context.Background().Done(), auditorDone)
	require.NoError(t, err)
}
