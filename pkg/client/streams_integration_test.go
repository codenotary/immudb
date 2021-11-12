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
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/codenotary/immudb/pkg/streamutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestImmuServer_SimpleSetGetStream(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server at port %d to run this test.", DefaultOptions().Port))
	}

	cliIf, err := NewImmuClient(DefaultOptions())
	require.NoError(t, err)
	cli := cliIf
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{DatabaseName: "defaultdb"})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", (32<<20)-1)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}

	metaTx, err := cli.StreamSet(ctx, kvs)
	require.NoError(t, err)
	_, err = cli.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile.Name()), SinceTx: metaTx.Id})
	require.NoError(t, err)
}

func TestImmuServer_SimpleSetGetManagedStream(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server at port %d to run this test.", DefaultOptions().Port))
	}

	cliIf, err := NewImmuClient(DefaultOptions())
	require.NoError(t, err)
	cli := cliIf
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{DatabaseName: "defaultdb"})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", ur.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", (32<<20)-1)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}

	s, err := cli.streamSet(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvss := stream.NewKvStreamSender(stream.NewMsgSender(s, cli.Options.StreamChunkSize))

	err = kvss.Send(kvs[0])
	require.NoError(t, err)

	txhdr, err := s.CloseAndRecv()
	require.NoError(t, err)
	require.IsType(t, &schema.TxHeader{}, txhdr)
}

func TestImmuServer_MultiSetGetManagedStream(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server at port %d to run this test.", DefaultOptions().Port))
	}

	cliIf, err := NewImmuClient(DefaultOptions())
	require.NoError(t, err)
	cli := cliIf
	lr, err := cli.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	ur, err := cli.UseDatabase(ctx, &schema.Database{DatabaseName: "defaultdb"})
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

	txhdr, err := s1.CloseAndRecv()
	require.NoError(t, err)
	require.IsType(t, &schema.TxHeader{}, txhdr)

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

	txhdr, err = s2.CloseAndRecv()
	require.NoError(t, err)
	require.IsType(t, &schema.TxHeader{}, txhdr)

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
