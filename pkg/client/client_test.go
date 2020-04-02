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

func TestImmuClient(t *testing.T) {
	ctx := context.Background()
	key := []byte("key1")
	value := []byte("value1")
	r, err := client.Connected(ctx, func() (interface{}, error) {
		_, err2 := client.SafeSet(ctx, key, value)
		require.NoError(t, err2)
		return client.SafeGet(ctx, key)
	})
	vi := r.(*VerifiedItem)
	require.NoError(t, err)
	require.NotNil(t, vi)
	require.Equal(t, key, vi.Key)
	require.Equal(t, value, vi.Value)
	require.True(t, vi.Verified)

	// delete client file and server folder
	if err := os.Remove(".root"); err != nil {
		log.Println(err)
	}
	if err := os.RemoveAll("immudb"); err != nil {
		log.Println(err)
	}
}
