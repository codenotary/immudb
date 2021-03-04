/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestImmuClient_SetGetStream(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	tmpFile, err := ioutil.TempFile(os.TempDir(), "go-stream-test-")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	err = tmpFile.Truncate(1_000_000)
	require.NoError(t, err)
	defer tmpFile.Close()

	hOrig := sha256.New()
	_, err = io.Copy(hOrig, tmpFile)
	require.NoError(t, err)
	oriSha := hOrig.Sum(nil)

	fi, err := tmpFile.Stat()
	require.NoError(t, err)

	tmpFile.Seek(0, io.SeekStart)

	kv := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile.Name()))),
			Size:    len(tmpFile.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile),
			Size:    int(fi.Size()),
		},
	}

	meta, err := client.StreamSet(ctx, []*stream.KeyValue{kv})
	require.NoError(t, err)
	require.NotNil(t, meta)

	entry, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile.Name())})
	require.NoError(t, err)
	require.NotNil(t, meta)

	newSha := sha256.Sum256(entry.Value)

	client.Disconnect()

	require.Equal(t, oriSha, newSha[:])
}

func TestImmuClient_Set32MBStream(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	tmpFile, err := ioutil.TempFile(os.TempDir(), "go-stream-test-")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	err = tmpFile.Truncate((1 << 25) - 1)
	require.NoError(t, err)
	defer tmpFile.Close()

	fi, err := tmpFile.Stat()
	require.NoError(t, err)

	tmpFile.Seek(0, io.SeekStart)

	kv := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile.Name()))),
			Size:    len(tmpFile.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile),
			Size:    int(fi.Size()),
		},
	}

	meta, err := client.StreamSet(ctx, []*stream.KeyValue{kv})
	require.NoError(t, err)
	require.NotNil(t, meta)

	_, err = client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile.Name())})
	require.NoError(t, err)
	require.NotNil(t, meta)

	client.Disconnect()
}

func TestImmuClient_SetMaxValueExceeded(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	tmpFile, err := ioutil.TempFile(os.TempDir(), "go-stream-test-")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	err = tmpFile.Truncate(1 << 25)
	require.NoError(t, err)
	defer tmpFile.Close()

	fi, err := tmpFile.Stat()
	require.NoError(t, err)

	tmpFile.Seek(0, io.SeekStart)

	kv := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile.Name()))),
			Size:    len(tmpFile.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile),
			Size:    int(fi.Size()),
		},
	}

	_, err = client.StreamSet(ctx, []*stream.KeyValue{kv})
	require.Equal(t, stream.ErrMaxValueLenExceeded, err)
}

func TestImmuClient_SetMaxTxValuesExceeded(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	tmpFile, err := ioutil.TempFile(os.TempDir(), "go-stream-test-")
	require.NoError(t, err)

	err = tmpFile.Truncate(1 << 24)
	require.NoError(t, err)

	fi, err := tmpFile.Stat()
	require.NoError(t, err)

	tmpFile.Seek(0, io.SeekStart)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	kv1 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile.Name()))),
			Size:    len(tmpFile.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile),
			Size:    int(fi.Size()),
		},
	}

	tmpFile1, err := ioutil.TempFile(os.TempDir(), "go-stream-test-")
	require.NoError(t, err)
	err = tmpFile1.Truncate(1 << 24)
	require.NoError(t, err)
	tmpFile1.Seek(0, io.SeekStart)
	defer tmpFile1.Close()
	defer os.Remove(tmpFile1.Name())

	kv2 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile1.Name()))),
			Size:    len(tmpFile1.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile1),
			Size:    int(fi.Size()),
		},
	}

	tmpFile2, err := ioutil.TempFile(os.TempDir(), "go-stream-test-")
	require.NoError(t, err)
	err = tmpFile2.Truncate(1 << 24)
	require.NoError(t, err)
	tmpFile2.Seek(0, io.SeekStart)
	defer tmpFile2.Close()
	defer os.Remove(tmpFile2.Name())

	kv3 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile2.Name()))),
			Size:    len(tmpFile2.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile2),
			Size:    int(fi.Size()),
		},
	}

	_, err = client.StreamSet(ctx, []*stream.KeyValue{kv1, kv2, kv3})
	require.Equal(t, stream.ErrMaxTxValuesLenExceeded, err)
}

func TestImmuClient_SetGetSmallMessage(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	tmpFile, err := ioutil.TempFile(os.TempDir(), "go-stream-test-")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	err = tmpFile.Truncate(1)
	require.NoError(t, err)
	defer tmpFile.Close()

	hOrig := sha256.New()
	_, err = io.Copy(hOrig, tmpFile)
	require.NoError(t, err)
	oriSha := hOrig.Sum(nil)

	fi, err := tmpFile.Stat()
	require.NoError(t, err)

	tmpFile.Seek(0, io.SeekStart)

	kv := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile.Name()))),
			Size:    len(tmpFile.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile),
			Size:    int(fi.Size()),
		},
	}

	meta, err := client.StreamSet(ctx, []*stream.KeyValue{kv})
	require.NoError(t, err)
	require.NotNil(t, meta)

	entry, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile.Name())})
	require.NoError(t, err)
	require.NotNil(t, meta)

	newSha := sha256.Sum256(entry.Value)

	client.Disconnect()

	require.Equal(t, oriSha, newSha[:])
}

func TestImmuClient_SetMultipleKeys(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	key1 := []byte("key1")
	val1 := []byte("val1")
	key2 := []byte("key2")
	val2 := []byte("val2")
	kv1 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(key1)),
			Size:    len(key1),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(val1)),
			Size:    len(val1),
		},
	}
	kv2 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(key2)),
			Size:    len(key2),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(val2)),
			Size:    len(val2),
		},
	}

	kvs := []*stream.KeyValue{kv1, kv2}
	meta, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, meta)

	entry1, err := client.StreamGet(ctx, &schema.KeyRequest{Key: key1})
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, val1, entry1.Value)

	require.Equal(t, sha256.Sum256(val1), sha256.Sum256(entry1.Value))

	entry2, err := client.StreamGet(ctx, &schema.KeyRequest{Key: key2})
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, val2, entry2.Value)

	client.Disconnect()
}

func TestImmuClient_SetMultipleLargeEntries(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	tmpFile1, err := ioutil.TempFile(os.TempDir(), "go-stream-test-")
	require.NoError(t, err)
	defer os.Remove(tmpFile1.Name())

	err = tmpFile1.Truncate(1 << 14)
	require.NoError(t, err)
	defer tmpFile1.Close()

	tmpFile1.Seek(0, io.SeekStart)

	hOrig1 := sha256.New()
	_, err = io.Copy(hOrig1, tmpFile1)
	require.NoError(t, err)
	oriSha1 := hOrig1.Sum(nil)

	fi, err := tmpFile1.Stat()
	require.NoError(t, err)

	tmpFile1.Seek(0, io.SeekStart)

	kv1 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile1.Name()))),
			Size:    len(tmpFile1.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile1),
			Size:    int(fi.Size()),
		},
	}

	tmpFile2, err := ioutil.TempFile(os.TempDir(), "go-stream-test-")
	require.NoError(t, err)
	defer os.Remove(tmpFile2.Name())

	err = tmpFile2.Truncate(1 << 13)
	require.NoError(t, err)
	defer tmpFile2.Close()

	tmpFile2.Seek(0, io.SeekStart)
	hOrig2 := sha256.New()
	_, err = io.Copy(hOrig2, tmpFile2)
	require.NoError(t, err)
	oriSha2 := hOrig2.Sum(nil)

	fi, err = tmpFile2.Stat()
	require.NoError(t, err)

	tmpFile2.Seek(0, io.SeekStart)

	kv2 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile2.Name()))),
			Size:    len(tmpFile2.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile2),
			Size:    int(fi.Size()),
		},
	}
	kvs := []*stream.KeyValue{kv1, kv2}
	meta, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, meta)

	entry1, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile1.Name())})
	require.NoError(t, err)

	newSha1 := sha256.Sum256(entry1.Value)
	require.Equal(t, oriSha1, newSha1[:])

	entry2, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile2.Name())})
	require.NoError(t, err)

	newSha2 := sha256.Sum256(entry2.Value)
	require.Equal(t, oriSha2, newSha2[:])

	client.Disconnect()
}

func TestImmuClient_SetMultipleLargeEntriesWithRealFiles(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	filename1 := "/home/falce/vchain/immudb/src/test/Graph_Algorithms_Neo4j.pdf"
	tmpFile1, err := os.Open(filename1)
	require.NoError(t, err)
	defer tmpFile1.Close()

	hOrig1 := sha256.New()
	_, err = io.Copy(hOrig1, tmpFile1)
	require.NoError(t, err)
	oriSha1 := hOrig1.Sum(nil)

	fi, err := tmpFile1.Stat()
	require.NoError(t, err)

	tmpFile1.Seek(0, io.SeekStart)

	kv1 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile1.Name()))),
			Size:    len(tmpFile1.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile1),
			Size:    int(fi.Size()),
		},
	}

	filename2 := "/home/falce/vchain/immudb/src/test/PARA0119.mp4"
	tmpFile2, err := os.Open(filename2)
	require.NoError(t, err)
	defer tmpFile2.Close()

	hOrig2 := sha256.New()
	_, err = io.Copy(hOrig2, tmpFile2)
	require.NoError(t, err)
	oriSha2 := hOrig2.Sum(nil)

	fi2, err := tmpFile2.Stat()
	require.NoError(t, err)

	tmpFile2.Seek(0, io.SeekStart)

	kv2 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile2.Name()))),
			Size:    len(tmpFile2.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile2),
			Size:    int(fi2.Size()),
		},
	}
	kvs := []*stream.KeyValue{kv1, kv2}
	meta, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, meta)

	entry1, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile1.Name())})
	require.NoError(t, err)

	newSha1 := sha256.Sum256(entry1.Value)
	assert.Equal(t, oriSha1, newSha1[:])

	entry2, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile2.Name())})
	require.NoError(t, err)

	newSha2 := sha256.Sum256(entry2.Value)
	assert.Equal(t, oriSha2, newSha2[:])

	client.Disconnect()

	err = ioutil.WriteFile(string(entry1.Key)+"_rec", entry1.Value, 0644)
	require.NoError(t, err)
	err = ioutil.WriteFile(string(entry2.Key)+"_rec", entry2.Value, 0644)
	require.NoError(t, err)
}

func TestImmuClient_SetMultipleKeysLoop(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	kvs := []*stream.KeyValue{}

	for i := 1; i <= 100; i++ {
		kv := &stream.KeyValue{
			Key: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer([]byte(fmt.Sprintf("key-%d", i)))),
				Size:    len([]byte(fmt.Sprintf("key-%d", i))),
			},
			Value: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer([]byte(fmt.Sprintf("val-%d", i)))),
				Size:    len([]byte(fmt.Sprintf("val-%d", i))),
			},
		}
		kvs = append(kvs, kv)
	}

	meta, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, meta)

	for i := 1; i <= 100; i++ {
		_, err = client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(fmt.Sprintf("key-%d", i))})
		require.NoError(t, err)
		require.NotNil(t, meta)
	}

	client.Disconnect()
}

func TestImmuClient_StreamScan(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	kvs := []*stream.KeyValue{}

	for i := 1; i <= 100; i++ {
		kv := &stream.KeyValue{
			Key: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer([]byte(fmt.Sprintf("key-%d", i)))),
				Size:    len([]byte(fmt.Sprintf("key-%d", i))),
			},
			Value: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer([]byte(fmt.Sprintf("val-%d", i)))),
				Size:    len([]byte(fmt.Sprintf("val-%d", i))),
			},
		}
		kvs = append(kvs, kv)
	}

	meta, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, meta)

	scanResp, err := client.StreamScan(ctx, &schema.ScanRequest{
		Prefix:  []byte("key"),
		SinceTx: meta.Id,
	})

	client.Disconnect()

	require.Len(t, scanResp.Entries, 100)
}

func TestImmuClient_SetEmptyReader(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	kv1 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`myKey1`))),
			Size:    len([]byte(`myKey1`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte{})),
			Size:    int(50),
		},
	}

	kv2 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`myKey2`))),
			Size:    len([]byte(`myKey2`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`myKey2`))),
			Size:    len([]byte(`myKey2`)),
		},
	}
	kvs := []*stream.KeyValue{kv1, kv2}
	meta, err := client.StreamSet(ctx, kvs)
	require.Equal(t, stream.ErrReaderIsEmpty, err)
	require.Nil(t, meta)

	client.Disconnect()
}

func TestImmuClient_SetSizeTooLarge(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	kv1 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`myKey1`))),
			Size:    len([]byte(`myKey1`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`myVal1`))),
			Size:    len([]byte(`myVal1`)) + 10,
		},
	}

	kvs := []*stream.KeyValue{kv1}
	meta, err := client.StreamSet(ctx, kvs)
	require.Equal(t, stream.ErrNotEnoughDataOnStream, err)
	require.Nil(t, meta)

	client.Disconnect()
}

func TestImmuClient_SetSizeTooLargeOnABigMessage(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	f, _ := streamtest.GenerateDummyFile("myFile", 20_000_000)
	defer f.Close()
	defer os.Remove(f.Name())

	kv := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`myKey1`))),
			Size:    len([]byte(`myKey1`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(f),
			Size:    22_000_000,
		},
	}

	kvs := []*stream.KeyValue{kv}
	meta, err := client.StreamSet(ctx, kvs)
	require.Equal(t, stream.ErrNotEnoughDataOnStream, err)
	require.Nil(t, meta)

	f1, _ := streamtest.GenerateDummyFile("myFile1", 10_000_000)
	defer f.Close()
	defer os.Remove(f.Name())
	f2, _ := streamtest.GenerateDummyFile("myFile2", 10_000_000)
	defer f.Close()
	defer os.Remove(f.Name())

	kv1 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`myFile1`))),
			Size:    len([]byte(`myFile1`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(f1),
			Size:    10_000_000,
		},
	}
	kv2 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`myFile2`))),
			Size:    len([]byte(`myFile2`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(f2),
			Size:    12_000_000,
		},
	}

	kvs2 := []*stream.KeyValue{kv1, kv2}
	meta, err = client.StreamSet(ctx, kvs2)
	require.Equal(t, stream.ErrNotEnoughDataOnStream, err)
	require.Nil(t, meta)

	client.Disconnect()
}
