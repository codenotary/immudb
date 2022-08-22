/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/codenotary/immudb/pkg/streamutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestImmuClient_StreamVerifiedSetAndGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "integration_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	defer os.Remove(".state-")

	options := server.DefaultOptions().
		WithDir(dir).
		WithAuth(true)

	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	client, err := ic.NewImmuClient(ic.DefaultOptions().WithDialOptions(
		[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()},
	))
	require.NoError(t, err)

	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	defer client.Disconnect()

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	nbFiles := 3
	fileNames := make([]string, 0, nbFiles)
	hashes := make([][]byte, 0, nbFiles)

	for i := 1; i <= nbFiles; i++ {
		tmpFile, err := streamtest.GenerateDummyFile(
			fmt.Sprintf("TestImmuClient_StreamVerifiedSetAndGet_InputFile_%d", i),
			1<<(10+i))
		require.NoError(t, err)
		defer tmpFile.Close()
		defer os.Remove(tmpFile.Name())

		hash := sha256.New()
		_, err = io.Copy(hash, tmpFile)
		require.NoError(t, err)
		hashSum := hash.Sum(nil)

		fileNames = append(fileNames, tmpFile.Name())
		hashes = append(hashes, hashSum)
	}

	// split the KVs so that the last one is set and get separately, so that
	// StreamVerifiedSet gets called a second time (to catch also the case when
	// local state exists and the verification is actually run)
	fileNames1 := fileNames[:len(fileNames)-1]
	lastFileName := fileNames[len(fileNames)-1]
	kvs1, err := streamutils.GetKeyValuesFromFiles(fileNames1...)
	lastKv, err := streamutils.GetKeyValuesFromFiles(lastFileName)

	// set and get all but the last one
	hdr, err := client.StreamVerifiedSet(ctx, kvs1)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	for i, fileName := range fileNames1 {
		entry, err := client.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{
			KeyRequest: &schema.KeyRequest{Key: []byte(fileName)},
		})
		require.NoError(t, err)
		newSha1 := sha256.Sum256(entry.Value)
		require.Equal(t, hashes[i], newSha1[:])
	}

	// set and get the last one
	hdr, err = client.StreamVerifiedSet(ctx, lastKv)
	require.NoError(t, err)
	require.NotNil(t, hdr)

	entry, err := client.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{
		KeyRequest: &schema.KeyRequest{Key: []byte(lastFileName)},
	})
	require.NoError(t, err)
	newSha1 := sha256.Sum256(entry.Value)
	require.Equal(t, hashes[len(hashes)-1], newSha1[:])
}
