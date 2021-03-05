package client

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/codenotary/immudb/pkg/streamutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestImmuClient_StreamVerifiedSetAndGet(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(
		&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()},
	))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)
	defer client.Disconnect()

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	nbFiles := 2
	fileNames := make([]string, 0, nbFiles)
	hashes := make([][]byte, 0, nbFiles)
	for i := 1; i <= 2; i++ {
		tmpFile, err := streamtest.GenerateDummyFile(
			fmt.Sprintf("TestImmuClient_StreamVerifiedSetAndGet_InputFile_%d", i),
			1<<(10+i))
		require.NoError(t, err)
		defer tmpFile.Close()
		defer os.Remove(tmpFile.Name())

		hash := sha256.New()
		_, err = io.Copy(hash, tmpFile)
		require.NoError(t, err)
		hashSum := hash.Sum(nil)

		fileNames = append(fileNames, tmpFile.Name())
		hashes = append(hashes, hashSum)
	}

	kvs, err := streamutils.GetKeyValuesFromFiles(fileNames...)

	meta, err := client.StreamVerifiedSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, meta)

	for i, fileName := range fileNames {
		entry, err := client.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{
			KeyRequest: &schema.KeyRequest{Key: []byte(fileName)},
		})
		require.NoError(t, err)
		newSha1 := sha256.Sum256(entry.Value)
		require.Equal(t, hashes[i], newSha1[:])
	}
}
