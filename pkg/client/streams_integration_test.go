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
	"bufio"
	"bytes"
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"log"
	"os"
	"testing"
)

func TestImmuServer_Stream(t *testing.T) {

	cli, err := NewImmuClient(DefaultOptions())
	require.NoError(t, err)
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	s, err := cli.SetStream(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvs := stream.NewKvStreamSender(stream.NewMsgSender(s))

	filename := "/home/falce/vchain/immudb/src/test/Graph_Algorithms_Neo4j.pdf"
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}

	kv := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(filename))),
			Size:    len(filename),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(f),
			Size:    int(fi.Size()),
		},
	}

	err = kvs.Send(kv)
	require.NoError(t, err)

	filename2 := "/home/falce/vchain/immudb/src/test/digest_OK.mp4"
	f2, err := os.Open(filename2)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fi2, err := f2.Stat()
	if err != nil {
		log.Fatal(err)
	}

	kv2 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(filename2))),
			Size:    len(filename2),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(f2),
			Size:    int(fi2.Size()),
		},
	}

	err = kvs.Send(kv2)
	require.NoError(t, err)

	filename3 := "/home/falce/vchain/immudb/src/test/client_test.expected.bkp"
	f3, err := os.Open(filename3)
	if err != nil {
		log.Fatal(err)
	}
	defer f3.Close()
	fi3, err := f3.Stat()
	if err != nil {
		log.Fatal(err)
	}

	kv3 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(filename3))),
			Size:    len(filename3),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(f3),
			Size:    int(fi3.Size()),
		},
	}

	err = kvs.Send(kv3)
	require.NoError(t, err)

	err = s.CloseSend()
	require.NoError(t, err)

}

func TestImmuServer_SetGetStream(t *testing.T) {

	cli, err := NewImmuClient(DefaultOptions())
	require.NoError(t, err)
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	s, err := cli.SetStream(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvs := stream.NewKvStreamSender(stream.NewMsgSender(s))

	key := []byte("key1")
	val := []byte("val1")

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

	err = kvs.Send(kv)
	require.NoError(t, err)

	txMeta, err := s.CloseAndRecv()
	require.NoError(t, err)
	require.IsType(t, &schema.TxMetadata{}, txMeta)

	s2, err := cli.SetStream(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvs2 := stream.NewKvStreamSender(stream.NewMsgSender(s2))

	key2 := []byte("key2")
	val2 := []byte("val2")

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

	err = kvs2.Send(kv2)
	require.NoError(t, err)

	s3, err := cli.SetStream(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvs3 := stream.NewKvStreamSender(stream.NewMsgSender(s3))

	key3 := []byte("key3")
	val3 := []byte("val3")

	kv3 := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(key3)),
			Size:    len(key3),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer(val3)),
			Size:    len(val3),
		},
	}

	err = kvs3.Send(kv3)
	require.NoError(t, err)

	err = s2.CloseSend()
	require.NoError(t, err)

	// STREAM GET
	// 1 val
	kr := &schema.KeyRequest{
		Key: key,
	}

	gs, err := cli.GetStream(ctx, kr)
	require.NoError(t, err)

	kvr := stream.NewKvStreamReceiver(stream.NewMsgReceiver(gs))

	rkv, err := kvr.Recv()
	require.NoError(t, err)

	rk1 := make([]byte, rkv.Key.Size)
	_, err = rkv.Key.Content.Read(rk1)
	require.NoError(t, err)
	require.Equal(t, rk1, key)
	rv1 := make([]byte, rkv.Value.Size)
	_, err = rkv.Value.Content.Read(rv1)
	require.NoError(t, err)
	require.Equal(t, rv1, val)

	// 2 val
	kr2 := &schema.KeyRequest{
		Key: key2,
	}

	gs2, err := cli.GetStream(ctx, kr2)
	require.NoError(t, err)

	kvr2 := stream.NewKvStreamReceiver(stream.NewMsgReceiver(gs2))

	rkv2, err := kvr2.Recv()
	require.NoError(t, err)

	rk2 := make([]byte, rkv2.Key.Size)
	_, err = rkv2.Key.Content.Read(rk2)
	require.NoError(t, err)
	require.Equal(t, rk2, key2)
	rv2 := make([]byte, rkv2.Value.Size)
	_, err = rkv2.Value.Content.Read(rv2)
	require.NoError(t, err)
	require.Equal(t, rv2, val2)

	// 3 val
	kr3 := &schema.KeyRequest{
		Key: key3,
	}

	gs3, err := cli.GetStream(ctx, kr3)
	require.NoError(t, err)

	kvr3 := stream.NewKvStreamReceiver(stream.NewMsgReceiver(gs3))

	rkv3, err := kvr3.Recv()
	require.NoError(t, err)

	rk3 := make([]byte, rkv3.Key.Size)
	_, err = rkv3.Key.Content.Read(rk3)
	require.NoError(t, err)
	require.Equal(t, rk3, key3)
	rv3 := make([]byte, rkv3.Value.Size)
	_, err = rkv3.Value.Content.Read(rv3)
	require.NoError(t, err)
	require.Equal(t, rv3, val3)
}
