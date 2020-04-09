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

package client

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

var immuServer *server.ImmuServer
var client *ImmuClient

const BkpFileName = "client_test.backup.bkp"
const ExpectedBkpFileName = "./../../test/client_test.expected.bkp"

var testData = struct {
	keys    [][]byte
	values  [][]byte
	refKeys [][]byte
	set     []byte
	scores  []float64
}{
	keys:    [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
	values:  [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")},
	refKeys: [][]byte{[]byte("refKey1"), []byte("refKey2"), []byte("refKey3")},
	set:     []byte("set1"),
	scores:  []float64{1.0, 2.0, 3.0},
}

var slog = logger.NewSimpleLoggerWithLevel("client_test", os.Stderr, logger.LogDebug)

func newServer() *server.ImmuServer {
	localImmuServer := server.DefaultServer()
	dbDir := filepath.Join(localImmuServer.Options.Dir, localImmuServer.Options.DbName)
	var err error
	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	localImmuServer.Store, err = store.Open(store.DefaultOptions(dbDir, slog))
	if err != nil {
		log.Fatal(err)
	}

	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	schema.RegisterImmuServiceServer(s, localImmuServer)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()
	return localImmuServer
}

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
}
func newClient() *ImmuClient {
	return DefaultClient().
		WithOptions(
			DefaultOptions().
				WithDialOptions(false, grpc.WithContextDialer(bufDialer), grpc.WithInsecure()))
}

var token string

func contextWithAuth() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), auth.AuthContextKey, "Bearer "+string(token))
}
func login() {
	if err := auth.GenerateKeys(); err != nil {
		log.Fatal(err)
	}

	plainPassword, err := auth.AdminUser.GenerateAndSetPassword()
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	r, err := client.Connected(ctx, func() (interface{}, error) {
		return client.Login(ctx, []byte(auth.AdminUser.Username), []byte(plainPassword))
	})
	if err != nil {
		log.Fatal(err)
	}
	token = string(r.(*schema.LoginResponse).GetToken())
}

func init() {
	immuServer = newServer()
	client = newClient()
	login()
}

func cleanup() {
	// delete files and folders created by tests
	if err := os.Remove(".root"); err != nil {
		log.Println(err)
	}
	if err := os.RemoveAll("immudb"); err != nil {
		log.Println(err)
	}
}
func cleanupBackup() {
	if err := os.Remove(BkpFileName); err != nil {
		log.Println(err)
	}
}

func testSafeSetAndSafeGet(ctx context.Context, t *testing.T, key []byte, value []byte) {
	r, err := client.Connected(ctx, func() (interface{}, error) {
		_, err2 := client.SafeSet(ctx, key, value)
		require.NoError(t, err2)
		return client.SafeGet(ctx, key)
	})
	require.NoError(t, err)
	require.NotNil(t, r)
	vi := r.(*VerifiedItem)
	require.Equal(t, key, vi.Key)
	require.Equal(t, value, vi.Value)
	require.True(t, vi.Verified)
}

func testSafeReference(ctx context.Context, t *testing.T, referenceKey []byte, key []byte, value []byte) {
	r, err := client.Connected(ctx, func() (interface{}, error) {
		_, err2 := client.SafeReference(ctx, referenceKey, key)
		require.NoError(t, err2)
		return client.SafeGet(ctx, referenceKey)
	})
	require.NoError(t, err)
	require.NotNil(t, r)
	vi := r.(*VerifiedItem)
	require.Equal(t, key, vi.Key)
	require.Equal(t, value, vi.Value)
	require.True(t, vi.Verified)
}

func testSafeZAdd(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte) {
	r, err := client.Connected(ctx, func() (interface{}, error) {
		for i := 0; i < len(scores); i++ {
			_, err := client.SafeZAdd(ctx, set, scores[i], keys[i])
			require.NoError(t, err)
		}
		return client.ZScan(ctx, set)
	})
	require.NoError(t, err)
	require.NotNil(t, r)
	itemList := r.(*schema.ItemList)
	require.Len(t, itemList.Items, len(keys))

	for i := 0; i < len(keys); i++ {
		require.Equal(t, keys[i], itemList.Items[i].Key)
		require.Equal(t, values[i], itemList.Items[i].Value)
	}
}

func testBackup(ctx context.Context, t *testing.T) {
	bkpFile, err := os.Create(BkpFileName)
	require.NoError(t, err)
	r, err := client.Connected(ctx, func() (interface{}, error) {
		return client.Backup(ctx, bkpFile)
	})
	require.NoError(t, err)
	n := r.(int64)
	require.Equal(t, int64(26), n)

	bkpBytesActual, err := ioutil.ReadFile(BkpFileName)
	require.NoError(t, err)
	require.NotEmpty(t, bkpBytesActual)
	bkpBytesExpected, err := ioutil.ReadFile(ExpectedBkpFileName)
	require.NoError(t, err)
	require.NotEmpty(t, bkpBytesExpected)
	require.Equal(t, bkpBytesExpected, bkpBytesActual)
}

func TestImmuClient(t *testing.T) {
	cleanup()
	cleanupBackup()
	defer cleanup()
	defer cleanupBackup()

	ctx := contextWithAuth()

	testSafeSetAndSafeGet(ctx, t, testData.keys[0], testData.values[0])
	testSafeSetAndSafeGet(ctx, t, testData.keys[1], testData.values[1])
	testSafeSetAndSafeGet(ctx, t, testData.keys[2], testData.values[2])

	testSafeReference(ctx, t, testData.refKeys[0], testData.keys[0], testData.values[0])
	testSafeReference(ctx, t, testData.refKeys[1], testData.keys[1], testData.values[1])
	testSafeReference(ctx, t, testData.refKeys[2], testData.keys[2], testData.values[2])

	testSafeZAdd(ctx, t, testData.set, testData.scores, testData.keys, testData.values)

	testBackup(ctx, t)
}

func TestRestore(t *testing.T) {
	cleanup()
	defer cleanup()

	ctx := contextWithAuth()

	// this only succeeds if only this test function is run, otherwise the key may
	// be present from other test function that run before this:
	// r1, err := client.Connected(ctx, func() (interface{}, error) {
	// 	return client.SafeGet(ctx, testData.keys[1])
	// })
	// require.Error(t, err)
	// require.Nil(t, r1)

	bkpFileForRead, err := os.Open(ExpectedBkpFileName)
	require.NoError(t, err)
	r2, err := client.Connected(ctx, func() (interface{}, error) {
		return client.Restore(ctx, bkpFileForRead, 20)
	})
	require.NoError(t, err)
	n2 := r2.(int64)
	require.Equal(t, int64(26), n2)

	r3, err := client.Connected(ctx, func() (interface{}, error) {
		return client.SafeGet(ctx, testData.keys[1])
	})
	require.NoError(t, err)
	require.NotNil(t, r3)
	vi := r3.(*VerifiedItem)
	require.Equal(t, testData.keys[1], vi.Key)
	require.Equal(t, testData.values[1], vi.Value)
	require.True(t, vi.Verified)

	r4, err := client.Connected(ctx, func() (interface{}, error) {
		return client.SafeGet(ctx, testData.refKeys[2])
	})
	require.NoError(t, err)
	require.NotNil(t, r4)
	viFromRef := r4.(*VerifiedItem)
	require.Equal(t, testData.keys[2], viFromRef.Key)
	require.Equal(t, testData.values[2], viFromRef.Value)
	require.True(t, viFromRef.Verified)

	r5, err := client.Connected(ctx, func() (interface{}, error) {
		return client.ZScan(ctx, testData.set)
	})
	require.NoError(t, err)
	require.NotNil(t, r5)
	itemList := r5.(*schema.ItemList)
	require.Len(t, itemList.Items, len(testData.keys))

	for i := 0; i < len(testData.keys); i++ {
		require.Equal(t, testData.keys[i], itemList.Items[i].Key)
		require.Equal(t, testData.values[i], itemList.Items[i].Value)
	}
}
