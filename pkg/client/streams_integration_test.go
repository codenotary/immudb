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
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"log"
	"net"
	"os"
	"testing"
	"time"
)

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

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", (1<<25)-1)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	kv := &stream.KeyValue{
		Key: &stream.ValueSize{
			Content: bufio.NewReader(bytes.NewBuffer([]byte(tmpFile.Name()))),
			Size:    len(tmpFile.Name()),
		},
		Value: &stream.ValueSize{
			Content: bufio.NewReader(tmpFile),
			Size:    (1 << 25) - 1,
		},
	}

	s, err := cli.streamSet(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvs := stream.NewKvStreamSender(stream.NewMsgSender(s, cli.Options.StreamChunkSize))

	err = kvs.Send(kv)
	require.NoError(t, err)

	txMeta, err := s.CloseAndRecv()
	require.NoError(t, err)
	require.IsType(t, &schema.TxMetadata{}, txMeta)
}

func TestImmuServer_SetGetManagedStream(t *testing.T) {
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

	s1, err := cli.streamSet(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvs := stream.NewKvStreamSender(stream.NewMsgSender(s1, cli.Options.StreamChunkSize))

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

	txMeta, err := s1.CloseAndRecv()
	require.NoError(t, err)
	require.IsType(t, &schema.TxMetadata{}, txMeta)

	s2, err := cli.streamSet(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvs2 := stream.NewKvStreamSender(stream.NewMsgSender(s2, cli.Options.StreamChunkSize))

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

	txMeta, err = s2.CloseAndRecv()
	require.NoError(t, err)
	require.IsType(t, &schema.TxMetadata{}, txMeta)

	s3, err := cli.streamSet(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvs3 := stream.NewKvStreamSender(stream.NewMsgSender(s3, cli.Options.StreamChunkSize))

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

	err = s3.CloseSend()
	require.NoError(t, err)
}
