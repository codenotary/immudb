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
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/codenotary/immudb/pkg/client/tokenservice"

	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/errors"

	"fmt"
	"io"
	"log"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/codenotary/immudb/pkg/streamutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestImmuClient_SetGetStream(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
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

	hdr, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	entry, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile.Name())})
	require.NoError(t, err)
	require.NotNil(t, hdr)

	newSha := sha256.Sum256(entry.Value)

	client.Disconnect()

	require.Equal(t, oriSha, newSha[:])
}

func TestImmuClient_Set32MBStream(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", (32<<20)-1)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}

	hdr, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	_, err = client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile.Name())})
	require.NoError(t, err)
	require.NotNil(t, hdr)

	client.Disconnect()
}

func TestImmuClient_SetMaxValueExceeded(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", 32<<20)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}

	_, err = client.StreamSet(ctx, kvs)
	require.Equal(t, stream.ErrMaxValueLenExceeded, err.Error())
	require.Equal(t, errors.CodDataException, err.(errors.ImmuError).Code())
}

func TestImmuClient_SetMaxTxValuesExceeded(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	tmpFile1, err := streamtest.GenerateDummyFile("myFile1", 16<<20)
	require.NoError(t, err)
	defer tmpFile1.Close()
	defer os.Remove(tmpFile1.Name())

	tmpFile2, err := streamtest.GenerateDummyFile("tmpFile2", 16<<20)
	require.NoError(t, err)
	defer tmpFile2.Close()
	defer os.Remove(tmpFile2.Name())

	tmpFile3, err := streamtest.GenerateDummyFile("tmpFile3", 16<<20)
	require.NoError(t, err)
	defer tmpFile3.Close()
	defer os.Remove(tmpFile3.Name())

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile1.Name(), tmpFile2.Name(), tmpFile3.Name())
	if err != nil {
		t.Error(err)
	}

	_, err = client.StreamSet(ctx, kvs)
	require.Equal(t, stream.ErrMaxTxValuesLenExceeded, err.(errors.ImmuError).Error())
	require.Equal(t, errors.CodDataException, err.(errors.ImmuError).Code())
}

func TestImmuClient_SetGetSmallMessage(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
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

	hdr, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	entry, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile.Name())})
	require.NoError(t, err)
	require.NotNil(t, hdr)

	newSha := sha256.Sum256(entry.Value)

	client.Disconnect()

	require.Equal(t, oriSha, newSha[:])
}

func TestImmuClient_SetMultipleKeys(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
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
	hdr, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	entry1, err := client.StreamGet(ctx, &schema.KeyRequest{Key: key1})
	require.NoError(t, err)
	require.NotNil(t, hdr)
	require.Equal(t, val1, entry1.Value)

	require.Equal(t, sha256.Sum256(val1), sha256.Sum256(entry1.Value))

	entry2, err := client.StreamGet(ctx, &schema.KeyRequest{Key: key2})
	require.NoError(t, err)
	require.NotNil(t, hdr)
	require.Equal(t, val2, entry2.Value)

	client.Disconnect()
}

func TestImmuClient_SetMultipleLargeEntries(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
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

	hdr, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, hdr)

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

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
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

	hdr, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	for i := 1; i <= 100; i++ {
		_, err = client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(fmt.Sprintf("key-%d", i))})
		require.NoError(t, err)
		require.NotNil(t, hdr)
	}

	client.Disconnect()
}

func TestImmuClient_StreamScan(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
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

	hdr, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	scanResp, err := client.StreamScan(ctx, &schema.ScanRequest{
		Prefix:  []byte("key"),
		SinceTx: hdr.Id,
	})

	client.Disconnect()

	require.Len(t, scanResp.Entries, 100)
}

func TestImmuClient_SetEmptyReader(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
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
	hdr, err := client.StreamSet(ctx, kvs)
	require.Equal(t, stream.ErrReaderIsEmpty, err.Error())
	require.Equal(t, errors.CodInvalidParameterValue, err.(errors.ImmuError).Code())
	require.Nil(t, hdr)

	client.Disconnect()
}

func TestImmuClient_SetSizeTooLarge(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
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
	hdr, err := client.StreamSet(ctx, kvs)
	require.Equal(t, stream.ErrNotEnoughDataOnStream, err.Error())
	require.Nil(t, hdr)

	client.Disconnect()
}

func TestImmuClient_SetSizeTooLargeOnABigMessage(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
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

	hdr, err := client.StreamSet(ctx, kvs1)
	require.Equal(t, stream.ErrNotEnoughDataOnStream, err.Error())
	require.Nil(t, hdr)

	f1, _ := streamtest.GenerateDummyFile("myFile1", 10_000_000)
	defer f.Close()
	defer os.Remove(f.Name())
	f2, _ := streamtest.GenerateDummyFile("myFile2", 10_000_000)
	defer f.Close()
	defer os.Remove(f.Name())

	kvs2, err := streamutils.GetKeyValuesFromFiles(f1.Name(), f2.Name())
	kvs2[1].Value.Size = 12_000_000

	hdr, err = client.StreamSet(ctx, kvs2)
	require.Equal(t, stream.ErrNotEnoughDataOnStream, err.Error())
	require.Nil(t, hdr)

	client.Disconnect()
}

func TestImmuClient_ExecAll(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
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

	hdr, err := client.StreamExecAll(ctx, aOps)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	entry1, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(`exec-all-key`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`exec-all-val`), entry1.Value)

	entry2, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(`exec-all-key2`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`exec-all-val2`), entry2.Value)

	client.Disconnect()
}

func TestImmuClient_StreamWithSignature(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithSigningKey("./../../test/signer/ec1.key")
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer bs.Stop()

	ts := tokenservice.NewInmemoryTokenService()
	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithServerSigningPubKey("./../../test/signer/ec1.pub"))
	if err != nil {
		log.Fatal(err)
	}
	client.WithTokenService(ts)
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

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer bs.Stop()

	ts := tokenservice.NewInmemoryTokenService()
	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithServerSigningPubKey("./../../test/signer/ec3.pub"))
	if err != nil {
		log.Fatal(err)
	}
	client.WithTokenService(ts)
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

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer bs.Stop()

	ts := tokenservice.NewInmemoryTokenService()
	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithServerSigningPubKey("./../../test/signer/ec3.pub"))
	if err != nil {
		log.Fatal(err)
	}
	client.WithTokenService(ts)
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

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer bs.Stop()

	ts := tokenservice.NewInmemoryTokenService()
	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithServerSigningPubKey("./../../test/signer/ec3.pub"))
	if err != nil {
		log.Fatal(err)
	}
	client.WithTokenService(ts)
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

	ts = tokenservice.NewInmemoryTokenService()
	client, err = ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithServerSigningPubKey("./../../test/signer/ec1.pub"))
	if err != nil {
		log.Fatal(err)
	}
	client.WithTokenService(ts)
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

func TestImmuClient_StreamerServiceErrors(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.Remove(".state-")
	defer os.RemoveAll(options.Dir)

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer bs.Stop()

	sfm := DefaultServiceFactoryMock()

	sfm.NewMsgSenderF = func(str stream.ImmuServiceSender_Stream) stream.MsgSender {
		sm := streamtest.DefaultImmuServiceSenderStreamMock()
		s := streamtest.DefaultMsgSenderMock(sm, 4096)
		s.SendF = func(reader io.Reader, payloadSize int) (err error) {
			return errors.New("custom one")
		}
		return streamtest.DefaultMsgSenderMock(sm, 4096)
	}
	sfm.NewMsgReceiverF = func(str stream.ImmuServiceReceiver_Stream) stream.MsgReceiver {
		return stream.NewMsgReceiver(str)
	}
	sfm.NewKvStreamSenderF = func(str stream.MsgSender) stream.KvStreamSender {
		return stream.NewKvStreamSender(str)
	}
	sfm.NewKvStreamReceiverF = func(str stream.MsgReceiver) stream.KvStreamReceiver {
		me := []*streamtest.MsgError{
			{M: []byte{1, 1, 1}, E: errors.New("custom one")},
		}
		msr := streamtest.DefaultMsgReceiverMock(me)
		return stream.NewKvStreamReceiver(msr, 4096)
	}

	sfm.NewVEntryStreamReceiverF = func(str stream.MsgReceiver) stream.VEntryStreamReceiver {
		me := []*streamtest.MsgError{
			{M: []byte{1, 1, 1}, E: errors.New("custom one")},
		}
		msr := streamtest.DefaultMsgReceiverMock(me)
		return stream.NewVEntryStreamReceiver(msr, 4096)
	}

	sfm.NewExecAllStreamSenderF = func(str stream.MsgSender) stream.ExecAllStreamSender {
		return stream.NewExecAllStreamSender(str)
	}

	ts := tokenservice.NewInmemoryTokenService()
	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	if err != nil {
		log.Fatal(err)
	}
	client.WithStreamServiceFactory(sfm).WithTokenService(ts)

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

	key := []byte("key3")
	val := []byte("val3")

	kv := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(key)),
			Size:    len(key),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(val)),
			Size:    len(val),
		},
	}

	_, err = client.StreamSet(ctx, []*stream.KeyValue{kv})
	require.Error(t, err)

	_, err = client.StreamGet(ctx, &schema.KeyRequest{Key: key})
	require.Error(t, err)

	_, err = client.StreamExecAll(ctx, &stream.ExecAllRequest{
		Operations: []*stream.Op{
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
		},
	})
	require.Error(t, err)

	client.Disconnect()

}

func TestImmuClient_StreamerServiceHistoryErrors(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer bs.Stop()

	sfm := DefaultServiceFactoryMock()
	sfm.NewMsgReceiverF = func(str stream.ImmuServiceReceiver_Stream) stream.MsgReceiver {
		return stream.NewMsgReceiver(str)
	}
	sfm.NewKvStreamSenderF = func(str stream.MsgSender) stream.KvStreamSender {
		return stream.NewKvStreamSender(str)
	}
	sfm.NewKvStreamReceiverF = func(str stream.MsgReceiver) stream.KvStreamReceiver {
		me := []*streamtest.MsgError{
			{M: []byte{1, 1, 1}, E: errors.New("custom one")},
		}
		msr := streamtest.DefaultMsgReceiverMock(me)
		return stream.NewKvStreamReceiver(msr, 4096)
	}

	sfm.NewZStreamReceiverF = func(str stream.MsgReceiver) stream.ZStreamReceiver {
		me := []*streamtest.MsgError{
			{M: []byte{1, 1, 1}, E: errors.New("custom one")},
		}
		msr := streamtest.DefaultMsgReceiverMock(me)
		return stream.NewZStreamReceiver(msr, 4096)
	}

	ts := tokenservice.NewInmemoryTokenService()
	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	if err != nil {
		log.Fatal(err)
	}
	client.WithStreamServiceFactory(sfm).WithTokenService(ts)

	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.StreamZScan(ctx, &schema.ZScanRequest{Set: []byte(`key`)})
	require.Error(t, err)

	_, err = client.StreamHistory(ctx, &schema.HistoryRequest{Key: []byte(`key`)})
	require.Error(t, err)

	client.Disconnect()
}

func TestImmuClient_ChunkToChunkGetStream(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	file_size := 1_000_000
	tmpFile, err := streamtest.GenerateDummyFile("myFile1", file_size)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	tmpFile.Seek(0, io.SeekStart)

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}

	hdr, err := client.StreamSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	sc := client.GetServiceClient()
	gs, err := sc.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile.Name())})

	kvr := stream.NewKvStreamReceiver(stream.NewMsgReceiver(gs), stream.DefaultChunkSize)

	_, vr, err := kvr.Next()
	require.NoError(t, err)

	l := 0
	chunk := make([]byte, 4096)
	for {
		r, err := vr.Read(chunk)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		if err == io.EOF {
			break
		}
		l += r
	}

	client.Disconnect()

	require.Equal(t, file_size, l)
}

type ServiceFactoryMock struct {
	NewMsgSenderF   func(str stream.ImmuServiceSender_Stream) stream.MsgSender
	NewMsgReceiverF func(str stream.ImmuServiceReceiver_Stream) stream.MsgReceiver

	NewKvStreamReceiverF func(str stream.MsgReceiver) stream.KvStreamReceiver
	NewKvStreamSenderF   func(str stream.MsgSender) stream.KvStreamSender

	NewVEntryStreamReceiverF func(str stream.MsgReceiver) stream.VEntryStreamReceiver
	NewVEntryStreamSenderF   func(str stream.MsgSender) stream.VEntryStreamSender

	NewZStreamReceiverF func(str stream.MsgReceiver) stream.ZStreamReceiver
	NewZStreamSenderF   func(str stream.MsgSender) stream.ZStreamSender

	NewExecAllStreamReceiverF func(str stream.MsgReceiver) stream.ExecAllStreamReceiver
	NewExecAllStreamSenderF   func(str stream.MsgSender) stream.ExecAllStreamSender
}

func (sfm *ServiceFactoryMock) NewMsgReceiver(str stream.ImmuServiceReceiver_Stream) stream.MsgReceiver {
	return sfm.NewMsgReceiverF(str)
}

func (sfm *ServiceFactoryMock) NewMsgSender(str stream.ImmuServiceSender_Stream) stream.MsgSender {
	return sfm.NewMsgSenderF(str)
}

func (sfm *ServiceFactoryMock) NewKvStreamReceiver(str stream.MsgReceiver) stream.KvStreamReceiver {
	return sfm.NewKvStreamReceiverF(str)
}

func (sfm *ServiceFactoryMock) NewKvStreamSender(str stream.MsgSender) stream.KvStreamSender {
	return sfm.NewKvStreamSenderF(str)
}

func (sfm *ServiceFactoryMock) NewVEntryStreamReceiver(str stream.MsgReceiver) stream.VEntryStreamReceiver {
	return sfm.NewVEntryStreamReceiverF(str)
}

func (sfm *ServiceFactoryMock) NewVEntryStreamSender(str stream.MsgSender) stream.VEntryStreamSender {
	return sfm.NewVEntryStreamSenderF(str)
}

func (sfm *ServiceFactoryMock) NewZStreamReceiver(str stream.MsgReceiver) stream.ZStreamReceiver {
	return sfm.NewZStreamReceiverF(str)
}

func (sfm *ServiceFactoryMock) NewZStreamSender(str stream.MsgSender) stream.ZStreamSender {
	return sfm.NewZStreamSenderF(str)
}

func (sfm *ServiceFactoryMock) NewExecAllStreamSender(str stream.MsgSender) stream.ExecAllStreamSender {
	return sfm.NewExecAllStreamSenderF(str)
}

func (sfm *ServiceFactoryMock) NewExecAllStreamReceiver(str stream.MsgReceiver) stream.ExecAllStreamReceiver {
	return sfm.NewExecAllStreamReceiverF(str)
}

func DefaultServiceFactoryMock() *ServiceFactoryMock {
	return &ServiceFactoryMock{}
}

func TestImmuClient_SessionSetGetStream(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client := ic.NewClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

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

	hdr, err := client.StreamSet(context.TODO(), kvs)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	entry, err := client.StreamGet(context.TODO(), &schema.KeyRequest{Key: []byte(tmpFile.Name())})
	require.NoError(t, err)
	require.NotNil(t, hdr)

	newSha := sha256.Sum256(entry.Value)

	err = client.CloseSession(context.TODO())
	require.NoError(t, err)

	require.Equal(t, oriSha, newSha[:])
}
