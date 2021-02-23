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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/codenotary/immudb/pkg/stream"
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

	meta, err := client.StreamSet(ctx, kv)
	require.NoError(t, err)
	require.NotNil(t, meta)

	entry, err := client.StreamGet(ctx, &schema.KeyRequest{Key: []byte(tmpFile.Name())})
	require.NoError(t, err)
	require.NotNil(t, meta)

	newSha := sha256.Sum256(entry.Value)

	client.Disconnect()

	require.Equal(t, oriSha, newSha[:])
}
