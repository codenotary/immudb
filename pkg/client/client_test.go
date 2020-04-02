package client

import (
	"context"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

var client *ImmuClient

func init() {
	immuServer := server.DefaultServer()
	dbDir := filepath.Join(immuServer.Options.Dir, immuServer.Options.DbName)
	var err error
	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		log.Fatal(err)
	}
	immuServer.Store, err = store.Open(store.DefaultOptions(dbDir))
	if err != nil {
		log.Fatal(err)
	}

	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	schema.RegisterImmuServiceServer(s, immuServer)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	client = DefaultClient().
		WithOptions(
			DefaultOptions().
				WithDialOptions(false, grpc.WithContextDialer(bufDialer), grpc.WithInsecure()))
}

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
}

func cleanup() {
	// delete client file and server folder
	if err := os.Remove(".root"); err != nil {
		log.Println(err)
	}
	if err := os.RemoveAll("immudb"); err != nil {
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

func TestImmuClient(t *testing.T) {
	defer cleanup()

	ctx := context.Background()

	key1 := []byte("key1")
	key2 := []byte("key2")
	value1 := []byte("value1")
	value2 := []byte("value2")
	testSafeSetAndSafeGet(ctx, t, key1, value1)
	testSafeSetAndSafeGet(ctx, t, key2, value2)

	refKey1 := []byte("refKey1")
	refKey2 := []byte("refKey2")
	testSafeReference(ctx, t, refKey1, key1, value1)
	testSafeReference(ctx, t, refKey2, key2, value2)

	set := []byte("set1")
	score1, score2, score3 := 1.0, 2.0, 3.0
	key3 := []byte("key3")
	value3 := []byte("value3")
	r3, err := client.Connected(ctx, func() (interface{}, error) {
		_, err2 := client.SafeSet(ctx, key3, value3)
		require.NoError(t, err2)

		_, err2 = client.SafeZAdd(ctx, set, score1, key1)
		require.NoError(t, err2)
		_, err2 = client.SafeZAdd(ctx, set, score2, key2)
		require.NoError(t, err2)
		_, err2 = client.SafeZAdd(ctx, set, score3, key3)
		require.NoError(t, err2)

		return client.ZScan(ctx, set)
	})
	require.NoError(t, err)
	require.NotNil(t, r3)
	itemList := r3.(*schema.ItemList)
	require.Len(t, itemList.Items, 3)
	require.Equal(t, key1, itemList.Items[0].Key)
	require.Equal(t, value1, itemList.Items[0].Value)
	require.Equal(t, key2, itemList.Items[1].Key)
	require.Equal(t, value2, itemList.Items[1].Value)
	require.Equal(t, key3, itemList.Items[2].Key)
	require.Equal(t, value3, itemList.Items[2].Value)
}
