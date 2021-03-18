package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"net"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func skipTestIfNoImmudbServer(t *testing.T) {
	_, err := net.DialTimeout(
		"tcp", fmt.Sprintf(":%d", DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf(
			"Please launch an immudb server at port %d to run this test.",
			DefaultOptions().Port))
	}
}

func newImmuClient(t *testing.T) (ImmuClient, context.Context) {
	cliIF, err := NewImmuClient(DefaultOptions())
	cli := cliIF.(*immuClient)
	require.NoError(t, err)
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)
	return cliIF, ctx
}

func inputTestFileToStreamKV(
	t *testing.T,
	fileName string,
) (*os.File, *stream.KeyValue, error) {

	f, err := os.Open(fileName)
	if err != nil {
		return nil, nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	kv := stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(fileName))),
			Size:    len(fileName),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(f),
			Size:    int(fi.Size()),
		},
	}

	return f, &kv, nil
}

func streamSetFiles(
	ctx context.Context,
	t *testing.T,
	cli ImmuClient,
	fileNames []string,
) (vSizes []int) {

	kvs := make([]*stream.KeyValue, len(fileNames))
	vSizes = make([]int, len(fileNames))
	for i, fileName := range fileNames {
		f, kv, err := inputTestFileToStreamKV(t, fileName)
		require.NoError(t, err)
		defer f.Close()
		kvs[i] = kv
		vSizes[i] = kv.Value.Size
	}

	txMeta, err := cli.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, txMeta)

	return
}

func zAddFiles(
	ctx context.Context,
	t *testing.T,
	cli ImmuClient,
	fileNames []string,
	set string,
	scores []float64,
) {
	var err error
	setBytes := []byte(set)
	for i, score := range scores {
		_, err = cli.ZAdd(ctx, setBytes, score, []byte(fileNames[i]))
		require.NoError(t, err)
	}
}

func TestImmuServer_StreamZScan(t *testing.T) {
	skipTestIfNoImmudbServer(t)

	cliIF, ctx := newImmuClient(t)

	tmpFile1, err := streamtest.GenerateDummyFile("myfile1.pdf", (8<<20)-1)
	require.NoError(t, err)
	defer tmpFile1.Close()
	defer os.Remove(tmpFile1.Name())
	tmpFile2, err := streamtest.GenerateDummyFile("myFile2.mp4", (16<<20)-1)
	require.NoError(t, err)
	defer tmpFile2.Close()
	defer os.Remove(tmpFile2.Name())

	fileNames := []string{
		tmpFile1.Name(),
		tmpFile2.Name(),
	}

	vSizes := streamSetFiles(ctx, t, cliIF, fileNames)
	require.Equal(t, len(fileNames), len(vSizes))

	set := "FileSet"
	scores := []float64{11, 22}
	zAddFiles(ctx, t, cliIF, fileNames, set, scores)

	zEntries, err := cliIF.StreamZScan(ctx, &schema.ZScanRequest{Set: []byte("FileSet")})
	require.NoError(t, err)
	require.NotNil(t, zEntries)
	require.Len(t, zEntries.Entries, 2)

	for i, fileName := range fileNames {
		require.Equal(t, fileName, string(zEntries.Entries[i].Key))
		require.Equal(t, set, string(zEntries.Entries[i].Set))
		require.Equal(t, scores[i], zEntries.Entries[i].Score)
		require.Equal(t, fileName, string(zEntries.Entries[i].Entry.Key))
		require.Equal(t, vSizes[i], len(zEntries.Entries[i].Entry.Value))
	}
}

func TestImmuServer_StreamHistory(t *testing.T) {
	skipTestIfNoImmudbServer(t)

	cliIF, ctx := newImmuClient(t)

	tmpFile1, err := streamtest.GenerateDummyFile("myfile1.pdf", (8<<20)-1)
	require.NoError(t, err)
	defer tmpFile1.Close()
	defer os.Remove(tmpFile1.Name())

	fileNames := []string{tmpFile1.Name()}

	vSizes1 := streamSetFiles(ctx, t, cliIF, fileNames)
	require.Equal(t, 1, len(vSizes1))
	vSizes2 := streamSetFiles(ctx, t, cliIF, fileNames)
	require.Equal(t, 1, len(vSizes2))
	vSizes := []int{vSizes1[0], vSizes2[0]}

	hEntries, err :=
		cliIF.StreamHistory(ctx, &schema.HistoryRequest{Key: []byte(tmpFile1.Name())})
	require.NoError(t, err)
	require.Equal(t, 2, len(hEntries.Entries))
	for i, hEntry := range hEntries.Entries {
		require.Equal(t, tmpFile1.Name(), string(hEntry.Key))
		require.Equal(t, vSizes[i], len(hEntry.Value))
	}
}
