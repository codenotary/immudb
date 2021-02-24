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
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"io"
	"log"
	"net"
	"os"
	"testing"
	"time"
)

func TestImmuServer_Stream(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server at port %d to run this test.", DefaultOptions().Port))
	}

	cliIf, err := NewImmuClient(DefaultOptions())
	cli := cliIf.(*immuClient)
	require.NoError(t, err)
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

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

	txMeta, err := cli.StreamSet(ctx, []*stream.KeyValue{kv})
	require.NoError(t, err)
	require.NotNil(t, txMeta)

	filename2 := "/home/falce/vchain/immudb/src/test/digest_OK.mp4"
	f2, err := os.Open(filename2)
	if err != nil {
		log.Fatal(err)
	}
	defer f2.Close()
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

	txMeta, err = cli.StreamSet(ctx, []*stream.KeyValue{kv2})
	require.NoError(t, err)
	require.NotNil(t, txMeta)

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

	txMeta, err = cli.StreamSet(ctx, []*stream.KeyValue{kv3})
	require.NoError(t, err)
	require.NotNil(t, txMeta)

	fn := &schema.KeyRequest{
		Key: []byte(filename),
	}
	gs, err := cli.streamGet(ctx, fn)
	require.NoError(t, err)

	kvr := stream.NewKvStreamReceiver(stream.NewMsgReceiver(gs))

	k1, err := kvr.NextKey()
	require.NoError(t, err)
	require.Equal(t, []byte(filename), k1)

	received, err := os.Create(filename + "_received")
	defer received.Close()
	if err != nil {
		log.Fatal(err)
	}

	bw := bufio.NewWriter(received)
	vr, err := kvr.NextValueReader()
	if err != nil {
		log.Fatal(err)
	}

	vl := 0
	chunk := make([]byte, stream.ChunkSize)
	exit := true
	for exit {
		l, err := vr.Read(chunk)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		vl += l

		_, err = bw.Write(chunk[:l])
		require.NoError(t, err)

		if err == io.EOF || l == 0 {
			err = bw.Flush()
			require.NoError(t, err)
			exit = false
		}
	}
	log.Print(vl)

}

func TestImmuServer_SetGetStream(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server at port %d to run this test.", DefaultOptions().Port))
	}

	cliIf, err := NewImmuClient(DefaultOptions())
	require.NoError(t, err)
	cli := cliIf.(*immuClient)
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	s, err := cli.streamSet(ctx)
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

	s2, err := cli.streamSet(ctx)
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

	s3, err := cli.streamSet(ctx)
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

	entry, err := cli.StreamGet(ctx, kr)

	require.NoError(t, err)
	require.Equal(t, val, entry.Value)

	// 2 val
	kr2 := &schema.KeyRequest{
		Key: key2,
	}

	entry, err = cli.StreamGet(ctx, kr2)

	require.NoError(t, err)
	require.Equal(t, val2, entry.Value)

	// 3 val
	kr3 := &schema.KeyRequest{
		Key: key3,
	}

	entry, err = cli.StreamGet(ctx, kr3)

	require.NoError(t, err)
	require.Equal(t, val3, entry.Value)
}

func TestReader(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server at port %d to run this test.", DefaultOptions().Port))
	}

	filename := "/home/falce/vchain/immudb/src/test/Graph_Algorithms_Neo4j.pdf"
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	b := bytes.NewBuffer([]byte{})

	r := bufio.NewReader(f)
	chunk := make([]byte, 100000)
	totalRead := 0
	for {
		read, err := r.Read(chunk)
		totalRead += read
		b.Write(chunk)
		fmt.Printf("buffer size %d\n", b.Len())
		fmt.Printf("buffer cap %d\n", b.Cap())

		if err != nil {
			if err == io.EOF {
				fmt.Printf("EOF chunk lenght %d\n", len(chunk))
				fmt.Printf("EOF read %d\n", read)
				break
			}
		}
		fmt.Printf("chunk lenght %d\n", len(chunk))
		fmt.Printf("read %d\n", read)
	}
	fmt.Printf("total Read%d\n", totalRead)
}

func TestImmuServer_SimpleSetGetStream(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server at port %d to run this test.", DefaultOptions().Port))
	}

	cliIf, err := NewImmuClient(DefaultOptions())
	require.NoError(t, err)
	cli := cliIf.(*immuClient)
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{Databasename: "defaultdb"})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	filename := "/home/falce/vchain/immudb/src/test/simple.pdf"

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

	s, err := cli.streamSet(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvs := stream.NewKvStreamSender(stream.NewMsgSender(s))

	err = kvs.Send(kv)
	require.NoError(t, err)

	txMeta, err := s.CloseAndRecv()
	require.NoError(t, err)
	require.IsType(t, &schema.TxMetadata{}, txMeta)
}
