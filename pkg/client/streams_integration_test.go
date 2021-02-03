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

	s, err := cli.Stream(ctx)
	if err != nil {
		log.Fatal(err)
	}

	kvs := NewKvStreamSender(stream.NewMsgSender(s))

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
