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
	"github.com/codenotary/immudb/pkg/streamutils"
	"github.com/prometheus/common/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"io"
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

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", 1_000_000)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	hOrig := sha256.New()
	_, err = io.Copy(hOrig, tmpFile)
	require.NoError(t, err)
	oriSha := hOrig.Sum(nil)

	tmpFile.Seek(0, io.SeekStart)

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}

	meta, err := client.StreamSet(ctx, kvs)
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

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", (1<<25)-1)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}

	meta, err := client.StreamSet(ctx, kvs)
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

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", 1<<25)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}

	_, err = client.StreamSet(ctx, kvs)
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

	tmpFile1, err := streamtest.GenerateDummyFile("myFile1", 1<<24)
	require.NoError(t, err)
	defer tmpFile1.Close()
	defer os.Remove(tmpFile1.Name())

	tmpFile2, err := streamtest.GenerateDummyFile("tmpFile2", 1<<24)
	require.NoError(t, err)
	defer tmpFile2.Close()
	defer os.Remove(tmpFile2.Name())

	tmpFile3, err := streamtest.GenerateDummyFile("tmpFile3", 1<<24)
	require.NoError(t, err)
	defer tmpFile3.Close()
	defer os.Remove(tmpFile3.Name())

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile1.Name(), tmpFile2.Name(), tmpFile3.Name())
	if err != nil {
		t.Error(err)
	}

	_, err = client.StreamSet(ctx, kvs)
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

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", 1)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	hOrig := sha256.New()
	_, err = io.Copy(hOrig, tmpFile)
	require.NoError(t, err)
	oriSha := hOrig.Sum(nil)

	tmpFile.Seek(0, io.SeekStart)

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}
	meta, err := client.StreamSet(ctx, kvs)
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

	tmpFile1, err := streamtest.GenerateDummyFile("myFile1", 1<<14)
	require.NoError(t, err)
	defer tmpFile1.Close()
	defer os.Remove(tmpFile1.Name())

	hOrig1 := sha256.New()
	_, err = io.Copy(hOrig1, tmpFile1)
	require.NoError(t, err)
	oriSha1 := hOrig1.Sum(nil)

	tmpFile2, err := streamtest.GenerateDummyFile("myFile1", 1<<13)
	require.NoError(t, err)
	defer tmpFile2.Close()
	defer os.Remove(tmpFile2.Name())

	hOrig2 := sha256.New()
	_, err = io.Copy(hOrig2, tmpFile2)
	require.NoError(t, err)
	oriSha2 := hOrig2.Sum(nil)

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile1.Name(), tmpFile2.Name())

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

	kvs1, err := streamutils.GetKeyValuesFromFiles(f.Name())
	kvs1[0].Value.Size = 22_000_000

	meta, err := client.StreamSet(ctx, kvs1)
	require.Equal(t, stream.ErrNotEnoughDataOnStream, err)
	require.Nil(t, meta)

	f1, _ := streamtest.GenerateDummyFile("myFile1", 10_000_000)
	defer f.Close()
	defer os.Remove(f.Name())
	f2, _ := streamtest.GenerateDummyFile("myFile2", 10_000_000)
	defer f.Close()
	defer os.Remove(f.Name())

	kvs2, err := streamutils.GetKeyValuesFromFiles(f1.Name(), f2.Name())
	kvs2[1].Value.Size = 12_000_000

	meta, err = client.StreamSet(ctx, kvs2)
	require.Equal(t, stream.ErrNotEnoughDataOnStream, err)
	require.Nil(t, meta)

	client.Disconnect()
}

func TestImmuClient_ExecAll(t *testing.T) {
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

	aOps := &stream.ExecAllRequest{
		Operations: []*stream.Op{
			{
				Operation: &stream.Op_KeyValue{
					KeyValue: &stream.KeyValue{
						Key: &stream.ValueSize{
							Content: bytes.NewBuffer([]byte(`exec-all-key`)),
							Size:    len([]byte(`exec-all-key`)),
						},
						Value: &stream.ValueSize{
							Content: bytes.NewBuffer([]byte(`exec-all-val`)),
							Size:    len([]byte(`exec-all-val`)),
						},
					},
				},
			},
			{
				Operation: &stream.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:      []byte(`exec-all-set`),
						Score:    85.4,
						Key:      []byte(`exec-all-key`),
						AtTx:     0,
						BoundRef: true,
					},
				},
			},
			{
				Operation: &stream.Op_KeyValue{
					KeyValue: &stream.KeyValue{
						Key: &stream.ValueSize{
							Content: bytes.NewBuffer([]byte(`exec-all-key2`)),
							Size:    len([]byte(`exec-all-key2`)),
						},
						Value: &stream.ValueSize{
							Content: bytes.NewBuffer([]byte(`exec-all-val2`)),
							Size:    len([]byte(`exec-all-val2`)),
						},
					},
				},
			},
			{
				Operation: &stream.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:      []byte(`exec-all-set`),
						Score:    85.4,
						Key:      []byte(`exec-all-key2`),
						AtTx:     0,
						BoundRef: true,
					},
				},
			},
		},
	}

	meta, err := client.StreamExecAll(ctx, aOps)
	require.NoError(t, err)
	require.NotNil(t, meta)

	entry1, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(`exec-all-key`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`exec-all-val`), entry1.Value)

	entry2, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(`exec-all-key2`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`exec-all-val2`), entry2.Value)

	client.Disconnect()
}

func TestImmuClient_Errors(t *testing.T) {
	client := DefaultClient().(*immuClient)
	ctx := context.TODO()

	_, err := client.StreamVerifiedSet(ctx, nil)
	require.Error(t, err)
	require.Equal(t, "no key-values specified", err.Error())

	// test ErrNotConnected errors
	fs := []func() (string, error){
		func() (string, error) { _, err := client.streamSet(ctx); return "streamSet", err },
		func() (string, error) { _, err := client.streamGet(ctx, nil); return "streamGet", err },
		func() (string, error) { _, err := client.streamVerifiableSet(ctx); return "streamVerifiableSet", err },
		func() (string, error) {
			_, err := client.streamVerifiableGet(ctx, nil)
			return "streamVerifiableGet", err
		},
		func() (string, error) { _, err := client.streamScan(ctx, nil); return "streamScan", err },
		func() (string, error) { _, err := client.streamZScan(ctx, nil); return "streamZScan", err },
		func() (string, error) { _, err := client.streamExecAll(ctx); return "streamExecAll", err },
		func() (string, error) { _, err := client.streamHistory(ctx, nil); return "streamHistory", err },
		func() (string, error) { _, err := client.StreamSet(ctx, nil); return "StreamSet", err },
		func() (string, error) { _, err := client.StreamGet(ctx, nil); return "StreamGet", err },
		func() (string, error) {
			_, err := client.StreamVerifiedSet(ctx, []*stream.KeyValue{{}})
			return "StreamVerifiedSet", err
		},
		func() (string, error) { _, err := client.StreamVerifiedGet(ctx, nil); return "StreamVerifiedGet", err },
		func() (string, error) { _, err := client.StreamScan(ctx, nil); return "StreamScan", err },
		func() (string, error) { _, err := client.StreamZScan(ctx, nil); return "StreamZScan", err },
		func() (string, error) { _, err := client.StreamHistory(ctx, nil); return "StreamHistory", err },
		func() (string, error) { _, err := client.StreamExecAll(ctx, nil); return "StreamExecAll", err },
	}
	for _, f := range fs {
		fn, err := f()
		require.Equal(t, ErrNotConnected, err, fn)
	}
}

func TestImmuClient_StreamWithSignature(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithSigningKey("./../../test/signer/ec1.key")
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}

	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts).WithServerSigningPubKey("./../../test/signer/ec1.pub"))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	_, err = client.StreamVerifiedSet(ctx, []*stream.KeyValue{{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`key`))),
			Size:    len([]byte(`key`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`val`))),
			Size:    len([]byte(`val`)),
		},
	}})
	require.Nil(t, err)

	_, err = client.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{KeyRequest: &schema.KeyRequest{Key: []byte(`key`)}})

	require.Nil(t, err)
}

func TestImmuClient_StreamWithSignatureErrors(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithSigningKey("./../../test/signer/ec1.key")
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}

	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts).WithServerSigningPubKey("./../../test/signer/ec3.pub"))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	_, err = client.StreamVerifiedSet(ctx, []*stream.KeyValue{{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`key`))),
			Size:    len([]byte(`key`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`val`))),
			Size:    len([]byte(`val`)),
		},
	}})
	require.Error(t, err)

	_, err = client.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{KeyRequest: &schema.KeyRequest{Key: []byte(`key`)}})

	require.Error(t, err)
}

func TestImmuClient_StreamWithSignatureErrorsMissingServerKey(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}

	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts).WithServerSigningPubKey("./../../test/signer/ec3.pub"))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	_, err = client.StreamVerifiedSet(ctx, []*stream.KeyValue{{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`key`))),
			Size:    len([]byte(`key`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`val`))),
			Size:    len([]byte(`val`)),
		},
	}})
	require.Error(t, err)

	_, err = client.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{KeyRequest: &schema.KeyRequest{Key: []byte(`key`)}})

	require.Error(t, err)
}

func TestImmuClient_StreamWithSignatureErrorsWrongClientKey(t *testing.T) {
	// first set and get needed to create a state and avoid that execution will be break by current state signature verification
	options := server.DefaultOptions().WithAuth(true).WithSigningKey("./../../test/signer/ec3.key")
	bs := servertest.NewBufconnServer(options)

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}

	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts).WithServerSigningPubKey("./../../test/signer/ec3.pub"))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	_, err = client.StreamVerifiedSet(ctx, []*stream.KeyValue{{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`key`))),
			Size:    len([]byte(`key`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`val`))),
			Size:    len([]byte(`val`)),
		},
	}})

	_, err = client.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{KeyRequest: &schema.KeyRequest{Key: []byte(`key`)}})

	client.Disconnect()

	ts = NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err = NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts).WithServerSigningPubKey("./../../test/signer/ec1.pub"))
	if err != nil {
		log.Fatal(err)
	}
	lr, err = client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md = metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{KeyRequest: &schema.KeyRequest{Key: []byte(`key`)}})

	require.Error(t, err)

	_, err = client.StreamVerifiedSet(ctx, []*stream.KeyValue{{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`key`))),
			Size:    len([]byte(`key`)),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(`val`))),
			Size:    len([]byte(`val`)),
		},
	}})
	require.Error(t, err)

	client.Disconnect()
}
