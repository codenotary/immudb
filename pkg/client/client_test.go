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
	"context"
	"errors"
	"fmt"
	immuErrors "github.com/codenotary/immudb/pkg/client/errors"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/fs"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/server/servertest"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

var lis *bufconn.Listener

var testData = struct {
	keys    [][]byte
	values  [][]byte
	refKeys [][]byte
	set     []byte
	scores  []float64
}{
	keys:    [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
	values:  [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")},
	refKeys: [][]byte{[]byte("refKey1"), []byte("refKey2"), []byte("refKey3")},
	set:     []byte("set1"),
	scores:  []float64{1.0, 2.0, 3.0},
}

var slog = logger.NewSimpleLoggerWithLevel("client_test", os.Stderr, logger.LogDebug)

func TestLogErr(t *testing.T) {
	logger := logger.NewSimpleLogger("client_test", os.Stderr)

	require.Nil(t, logErr(logger, "error: %v", nil))

	err := fmt.Errorf("expected error")
	require.Error(t, logErr(logger, "error: %v", err))
}

func testSafeSetAndSafeGet(ctx context.Context, t *testing.T, key []byte, value []byte, client ImmuClient) {
	_, err2 := client.VerifiedSet(ctx, key, value)
	require.NoError(t, err2)

	time.Sleep(10 * time.Millisecond)

	vi, err := client.VerifiedGet(ctx, key)

	require.NoError(t, err)
	require.NotNil(t, vi)
	require.Equal(t, key, vi.Key)
	require.Equal(t, value, vi.Value)
}

func testReference(ctx context.Context, t *testing.T, referenceKey []byte, key []byte, value []byte, client ImmuClient) {
	_, err2 := client.SetReference(ctx, referenceKey, key)
	require.NoError(t, err2)
	vi, err := client.VerifiedGet(ctx, referenceKey)
	require.NoError(t, err)
	require.NotNil(t, vi)
	require.Equal(t, key, vi.Key)
	require.Equal(t, value, vi.Value)
}

func testVerifiedReference(ctx context.Context, t *testing.T, key []byte, referencedKey []byte, value []byte, client ImmuClient) {
	md, err2 := client.VerifiedSetReference(ctx, key, referencedKey)
	require.NoError(t, err2)

	vi, err := client.VerifiedGetSince(ctx, key, md.Id)
	require.NoError(t, err)
	require.NotNil(t, vi)
	require.Equal(t, referencedKey, vi.Key)
	require.Equal(t, value, vi.Value)
}

func testVerifiedZAdd(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, client ImmuClient) {
	for i := 0; i < len(scores); i++ {
		_, err := client.VerifiedZAdd(ctx, set, scores[i], keys[i])
		require.NoError(t, err)
	}

	itemList, err := client.ZScan(ctx, &schema.ZScanRequest{
		Set:     set,
		SinceTx: uint64(len(scores)),
	})
	require.NoError(t, err)
	require.NotNil(t, itemList)
	require.Len(t, itemList.Entries, len(keys))

	for i := 0; i < len(keys); i++ {
		require.Equal(t, keys[i], itemList.Entries[i].Entry.Key)
		require.Equal(t, values[i], itemList.Entries[i].Entry.Value)
	}
}

func testZAdd(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, client ImmuClient) {
	var md *schema.TxMetadata
	var err error

	for i := 0; i < len(scores); i++ {
		md, err = client.ZAdd(ctx, set, scores[i], keys[i])
		require.NoError(t, err)
	}

	itemList, err := client.ZScan(ctx, &schema.ZScanRequest{
		Set:     set,
		SinceTx: md.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, itemList)
	require.Len(t, itemList.Entries, len(keys))

	for i := 0; i < len(keys); i++ {
		require.Equal(t, keys[i], itemList.Entries[i].Entry.Key)
		require.Equal(t, values[i], itemList.Entries[i].Entry.Value)
	}
}

func testZAddAt(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, at uint64, client ImmuClient) {
	var md *schema.TxMetadata
	var err error

	for i := 0; i < len(scores); i++ {
		md, err = client.ZAddAt(ctx, set, scores[i], keys[i], at)
		require.NoError(t, err)
	}

	itemList, err := client.ZScan(ctx, &schema.ZScanRequest{
		Set:     set,
		SinceTx: md.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, itemList)
	require.Len(t, itemList.Entries, len(keys))

	for i := 0; i < len(keys); i++ {
		require.Equal(t, keys[i], itemList.Entries[i].Entry.Key)
		require.Equal(t, values[i], itemList.Entries[i].Entry.Value)
	}
}

func testVerifiedZAddAt(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, at uint64, client ImmuClient) {
	for i := 0; i < len(scores); i++ {
		_, err := client.VerifiedZAddAt(ctx, set, scores[i], keys[i], at)
		require.NoError(t, err)
	}

	itemList, err := client.ZScan(ctx, &schema.ZScanRequest{
		Set:     set,
		SinceTx: uint64(len(scores)),
	})
	require.NoError(t, err)
	require.NotNil(t, itemList)
	require.Len(t, itemList.Entries, len(keys))

	for i := 0; i < len(keys); i++ {
		require.Equal(t, keys[i], itemList.Entries[i].Entry.Key)
		require.Equal(t, values[i], itemList.Entries[i].Entry.Value)
	}
}

func testGet(ctx context.Context, t *testing.T, client ImmuClient) {
	txmd, err := client.VerifiedSet(ctx, []byte("key-n11"), []byte("val-n11"))
	require.NoError(t, err)

	item, err := client.GetSince(ctx, []byte("key-n11"), txmd.Id)
	require.NoError(t, err)
	require.Equal(t, []byte("key-n11"), item.Key)

	item, err = client.GetAt(ctx, []byte("key-n11"), txmd.Id)
	require.NoError(t, err)
	require.Equal(t, []byte("key-n11"), item.Key)
}

func testGetTxByID(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, client ImmuClient) {
	vi1, err := client.VerifiedSet(ctx, []byte("key-n11"), []byte("val-n11"))
	require.NoError(t, err)

	item1, err3 := client.TxByID(ctx, vi1.Id)
	require.Equal(t, vi1.Ts, item1.Metadata.Ts)
	require.NoError(t, err3)
}

func testImmuClient_VerifiedTxByID(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, client ImmuClient) {
	vi1, err := client.VerifiedSet(ctx, []byte("key-n11"), []byte("val-n11"))
	require.NoError(t, err)

	item1, err3 := client.VerifiedTxByID(ctx, vi1.Id)
	require.Equal(t, vi1.Ts, item1.Metadata.Ts)
	require.NoError(t, err3)

	_, err = client.VerifiedSet(ctx, []byte("key-n12"), []byte("val-n12"))
	require.NoError(t, err)

	item1, err3 = client.VerifiedTxByID(ctx, vi1.Id)
	require.Equal(t, vi1.Ts, item1.Metadata.Ts)
	require.NoError(t, err3)
}

func TestImmuClient(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithSigningKey("./../../test/signer/ec1.key")
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	opts := DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts)
	client, err := NewImmuClient(opts.WithServerSigningPubKey("./../../test/signer/ec1.pub"))
	if err != nil {
		log.Fatal(err)
	}
	resp, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", resp.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	testSafeSetAndSafeGet(ctx, t, testData.keys[0], testData.values[0], client)
	testSafeSetAndSafeGet(ctx, t, testData.keys[1], testData.values[1], client)
	testSafeSetAndSafeGet(ctx, t, testData.keys[2], testData.values[2], client)

	testVerifiedReference(ctx, t, testData.refKeys[0], testData.keys[0], testData.values[0], client)
	testVerifiedReference(ctx, t, testData.refKeys[1], testData.keys[1], testData.values[1], client)
	testVerifiedReference(ctx, t, testData.refKeys[2], testData.keys[2], testData.values[2], client)

	testZAdd(ctx, t, testData.set, testData.scores, testData.keys, testData.values, client)
	testZAddAt(ctx, t, testData.set, testData.scores, testData.keys, testData.values, 0, client)

	testVerifiedZAdd(ctx, t, testData.set, testData.scores, testData.keys, testData.values, client)
	testVerifiedZAddAt(ctx, t, testData.set, testData.scores, testData.keys, testData.values, 0, client)

	testReference(ctx, t, testData.refKeys[0], testData.keys[0], testData.values[0], client)
	testGetTxByID(ctx, t, testData.set, testData.scores, testData.keys, testData.values, client)
	testImmuClient_VerifiedTxByID(ctx, t, testData.set, testData.scores, testData.keys, testData.values, client)

	testGet(ctx, t, client)
}

func TestImmuClientTampering(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithSigningKey("./../../test/signer/ec1.key")
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	opts := DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts)
	client, err := NewImmuClient(opts.WithServerSigningPubKey("./../../test/signer/ec1.pub"))
	if err != nil {
		log.Fatal(err)
	}
	resp, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", resp.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.Set(ctx, []byte{0}, []byte{0})
	require.NoError(t, err)

	bs.Server.PostSetFn = func(ctx context.Context,
		req *schema.SetRequest, res *schema.TxMetadata, err error) (*schema.TxMetadata, error) {

		if err != nil {
			return res, err
		}

		res.Nentries = 0

		return res, nil
	}

	_, err = client.Set(ctx, []byte{1}, []byte{1})
	require.Equal(t, store.ErrCorruptedData, err)

	_, err = client.SetAll(ctx, &schema.SetRequest{
		KVs: []*schema.KeyValue{{Key: []byte{1}, Value: []byte{1}}},
	})
	require.Equal(t, store.ErrCorruptedData, err)

	bs.Server.PostVerifiableSetFn = func(ctx context.Context,
		req *schema.VerifiableSetRequest, res *schema.VerifiableTx, err error) (*schema.VerifiableTx, error) {

		if err != nil {
			return res, err
		}

		res.Tx.Metadata.Nentries = 0

		return res, nil
	}

	_, err = client.VerifiedSet(ctx, []byte{1}, []byte{1})
	require.Equal(t, store.ErrCorruptedData, err)

	bs.Server.PostSetReferenceFn = func(ctx context.Context,
		req *schema.ReferenceRequest, res *schema.TxMetadata, err error) (*schema.TxMetadata, error) {

		if err != nil {
			return res, err
		}

		res.Nentries = 0

		return res, nil
	}

	_, err = client.SetReference(ctx, []byte{2}, []byte{1})
	require.Equal(t, store.ErrCorruptedData, err)

	bs.Server.PostVerifiableSetReferenceFn = func(ctx context.Context,
		req *schema.VerifiableReferenceRequest, res *schema.VerifiableTx, err error) (*schema.VerifiableTx, error) {

		if err != nil {
			return res, err
		}

		res.Tx.Metadata.Nentries = 0

		return res, nil
	}

	_, err = client.VerifiedSetReference(ctx, []byte{2}, []byte{1})
	require.Equal(t, store.ErrCorruptedData, err)

	bs.Server.PostZAddFn = func(ctx context.Context,
		req *schema.ZAddRequest, res *schema.TxMetadata, err error) (*schema.TxMetadata, error) {

		if err != nil {
			return res, err
		}

		res.Nentries = 0

		return res, nil
	}

	_, err = client.ZAdd(ctx, []byte{7}, 1, []byte{1})
	require.Equal(t, store.ErrCorruptedData, err)

	bs.Server.PostVerifiableZAddFn = func(ctx context.Context,
		req *schema.VerifiableZAddRequest, res *schema.VerifiableTx, err error) (*schema.VerifiableTx, error) {

		if err != nil {
			return res, err
		}

		res.Tx.Metadata.Nentries = 0

		return res, nil
	}

	_, err = client.VerifiedZAdd(ctx, []byte{7}, 1, []byte{1})
	require.Equal(t, store.ErrCorruptedData, err)

	bs.Server.PostExecAllFn = func(ctx context.Context,
		req *schema.ExecAllRequest, res *schema.TxMetadata, err error) (*schema.TxMetadata, error) {

		if err != nil {
			return res, err
		}

		res.Nentries = 0

		return res, nil
	}

	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
		},
	}

	_, err = client.ExecAll(ctx, aOps)
	require.Equal(t, store.ErrCorruptedData, err)
}

func TestReplica(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	err = client.CreateDatabase(ctx, &schema.DatabaseSettings{
		DatabaseName: "db1",
		Replica:      true,
	})
	require.NoError(t, err)

	resp, err := client.UseDatabase(ctx, &schema.Database{
		DatabaseName: "db1",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Token)

	err = client.UpdateDatabase(ctx, &schema.DatabaseSettings{
		DatabaseName: "db1",
		Replica:      true,
	})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", resp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.VerifiedSet(ctx, []byte(`db1-key1`), []byte(`db1-value1`))
	require.Error(t, err)
}

func TestDatabasesSwitching(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	err = client.CreateDatabase(ctx, &schema.DatabaseSettings{
		DatabaseName: "db1",
	})
	require.NoError(t, err)

	resp, err := client.UseDatabase(ctx, &schema.Database{
		DatabaseName: "db1",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Token)

	md = metadata.Pairs("authorization", resp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.VerifiedSet(ctx, []byte(`db1-my`), []byte(`item`))
	require.NoError(t, err)

	err = client.CreateDatabase(ctx, &schema.DatabaseSettings{
		DatabaseName: "db2",
	})
	require.NoError(t, err)

	resp2, err := client.UseDatabase(ctx, &schema.Database{
		DatabaseName: "db2",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp2.Token)

	md = metadata.Pairs("authorization", resp2.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.VerifiedSet(ctx, []byte(`db2-my`), []byte(`item`))
	require.NoError(t, err)

	vi, err := client.VerifiedGet(ctx, []byte(`db1-my`))
	require.Error(t, err)
	require.Nil(t, vi)
}

func TestImmuClientDisconnect(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithSigningKey("./../../test/signer/ec1.key")
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	opts := DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts)
	client, err := NewImmuClient(opts.WithServerSigningPubKey("./../../test/signer/ec1.pub"))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	err = client.Disconnect()
	require.Nil(t, err)

	require.False(t, client.IsConnected())

	require.True(t, errors.Is(client.CreateUser(ctx, []byte("user"), []byte("passwd"), 1, "db"), ErrNotConnected))
	require.True(t, errors.Is(client.ChangePassword(ctx, []byte("user"), []byte("oldPasswd"), []byte("newPasswd")), ErrNotConnected))
	require.True(t, errors.Is(client.UpdateAuthConfig(ctx, auth.KindPassword), ErrNotConnected))
	require.True(t, errors.Is(client.UpdateMTLSConfig(ctx, false), ErrNotConnected))
	require.True(t, errors.Is(client.CleanIndex(ctx, &emptypb.Empty{}), ErrNotConnected))

	_, err = client.Login(context.TODO(), []byte("user"), []byte("passwd"))
	require.True(t, errors.Is(err.(immuErrors.ImmuError), ErrNotConnected))

	require.True(t, errors.Is(client.Logout(context.TODO()), ErrNotConnected))

	_, err = client.Get(context.TODO(), []byte("key"))
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.CurrentState(context.TODO())
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.VerifiedGet(context.TODO(), []byte("key"))
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.GetAll(context.TODO(), [][]byte{[]byte(`aaa`), []byte(`bbb`)})
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.Scan(context.TODO(), &schema.ScanRequest{
		Prefix: []byte("key"),
	})
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.ZScan(context.TODO(), &schema.ZScanRequest{Set: []byte("key")})
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.Count(context.TODO(), []byte("key"))
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.CountAll(context.TODO())
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.Set(context.TODO(), []byte("key"), []byte("value"))
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.VerifiedSet(context.TODO(), []byte("key"), []byte("value"))
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.Set(context.TODO(), nil, nil)
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.TxByID(context.TODO(), 1)
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.VerifiedTxByID(context.TODO(), 1)
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.TxScan(context.TODO(), nil)
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.History(context.TODO(), &schema.HistoryRequest{
		Key: []byte("key"),
	})
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.SetReference(context.TODO(), []byte("ref"), []byte("key"))
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.VerifiedSetReference(context.TODO(), []byte("ref"), []byte("key"))
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.ZAdd(context.TODO(), []byte("set"), 1, []byte("key"))
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.VerifiedZAdd(context.TODO(), []byte("set"), 1, []byte("key"))
	require.True(t, errors.Is(err, ErrNotConnected))

	//_, err = client.Dump(context.TODO(), nil)
	//require.Equal(t, ErrNotConnected, err)

	_, err = client.GetSince(context.TODO(), []byte("key"), 0)
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.GetAt(context.TODO(), []byte("key"), 0)
	require.True(t, errors.Is(err, ErrNotConnected))

	require.True(t, errors.Is(client.HealthCheck(context.TODO()), ErrNotConnected))

	require.True(t, errors.Is(client.CreateDatabase(context.TODO(), nil), ErrNotConnected))

	_, err = client.UseDatabase(context.TODO(), nil)
	require.True(t, errors.Is(err, ErrNotConnected))

	err = client.ChangePermission(context.TODO(), schema.PermissionAction_REVOKE, "userName", "testDBName", auth.PermissionRW)
	require.True(t, errors.Is(err, ErrNotConnected))

	require.True(t, errors.Is(client.SetActiveUser(context.TODO(), nil), ErrNotConnected))

	_, err = client.DatabaseList(context.TODO())
	require.True(t, errors.Is(err, ErrNotConnected))
}

func TestImmuClientDisconnectNotConn(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}

	client.Disconnect()
	err = client.Disconnect()
	require.Error(t, err)
	require.Errorf(t, err, "not connected")
}

func TestWaitForHealthCheck(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	err = client.WaitForHealthCheck(context.TODO())
	require.Nil(t, err)
}

func TestWaitForHealthCheckFail(t *testing.T) {
	client := DefaultClient()
	err := client.WaitForHealthCheck(context.TODO())
	require.Error(t, err)
}

func TestSetupDialOptions(t *testing.T) {
	client := DefaultClient()

	ts := TokenServiceMock{}
	ts.GetTokenF = func() (string, error) {
		return "token", nil
	}
	client.WithTokenService(ts)

	dialOpts := client.SetupDialOptions(DefaultOptions().WithMTLs(true))
	require.NotNil(t, dialOpts)
}

func TestUserManagement(t *testing.T) {

	var (
		userName        = "test"
		userPassword    = "1Password!*"
		userNewPassword = "2Password!*"
		testDBName      = "test"
		testDB          = &schema.DatabaseSettings{DatabaseName: testDBName}
		err             error
		usrList         *schema.UserList
		immudbUser      *schema.User
		testUser        *schema.User
	)

	options := server.DefaultOptions().WithAuth(true).WithConfig("../../configs/immudb.toml")
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	err = client.CreateDatabase(ctx, testDB)
	require.NoError(t, err)

	err = client.UpdateAuthConfig(ctx, auth.KindPassword)
	require.NoError(t, err)

	err = client.UpdateMTLSConfig(ctx, false)
	require.Nil(t, err)

	err = client.CreateUser(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		auth.PermissionRW,
		testDBName,
	)
	require.Nil(t, err)

	err = client.ChangePermission(
		ctx,
		schema.PermissionAction_REVOKE,
		userName,
		testDBName,
		auth.PermissionRW,
	)
	require.Nil(t, err)

	err = client.SetActiveUser(
		ctx,
		&schema.SetActiveUserRequest{
			Active:   true,
			Username: userName,
		})
	require.Nil(t, err)

	err = client.ChangePassword(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		[]byte(userNewPassword),
	)
	require.Nil(t, err)

	usrList, err = client.ListUsers(ctx)
	require.NoError(t, err)
	require.NotNil(t, usrList)
	require.Len(t, usrList.Users, 2)

	for _, usr := range usrList.Users {
		switch string(usr.User) {
		case "immudb":
			immudbUser = usr
		case "test":
			testUser = usr
		}
	}
	require.NotNil(t, immudbUser)
	require.Equal(t, "immudb", string(immudbUser.User))
	require.Len(t, immudbUser.Permissions, 1)
	require.Equal(t, "*", immudbUser.Permissions[0].GetDatabase())
	require.Equal(t, uint32(auth.PermissionSysAdmin), immudbUser.Permissions[0].GetPermission())
	require.True(t, immudbUser.Active)

	require.NotNil(t, testUser)
	require.Equal(t, "test", string(testUser.User))
	require.Len(t, testUser.Permissions, 0)
	require.Equal(t, "immudb", testUser.Createdby)
	require.True(t, testUser.Active)

	client.Disconnect()
}

func TestDatabaseManagement(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	err1 := client.CreateDatabase(ctx, &schema.DatabaseSettings{DatabaseName: "test"})
	require.Nil(t, err1)

	resp2, err2 := client.DatabaseList(ctx)

	require.Nil(t, err2)
	require.IsType(t, &schema.DatabaseListResponse{}, resp2)
	require.Len(t, resp2.Databases, 2)
	client.Disconnect()
}

func TestImmuClient_History(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, _ = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	txmd, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val2`))
	if err != nil {
		log.Fatal(err)
	}

	sil, err := client.History(ctx, &schema.HistoryRequest{
		Key:     []byte(`key1`),
		SinceTx: txmd.Id,
	})

	require.Nil(t, err)
	require.Len(t, sil.Entries, 2)
	client.Disconnect()
}

func TestImmuClient_SetAll(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	options.StoreOptions.WithIndexOptions(options.StoreOptions.IndexOpts.WithCompactionThld(0))
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.SetAll(ctx, nil)
	require.Error(t, err)

	setRequest := &schema.SetRequest{KVs: []*schema.KeyValue{}}
	_, err = client.SetAll(ctx, setRequest)
	require.Error(t, err)

	setRequest = &schema.SetRequest{KVs: []*schema.KeyValue{
		{Key: []byte("1,2,3"), Value: []byte("3,2,1")},
		{Key: []byte("4,5,6"), Value: []byte("6,5,4")},
	}}

	_, err = client.SetAll(ctx, setRequest)
	require.NoError(t, err)

	time.Sleep(1 * time.Millisecond)

	err = client.CleanIndex(ctx, &emptypb.Empty{})
	require.NoError(t, err)

	for _, kv := range setRequest.KVs {
		i, err := client.Get(ctx, kv.Key)
		require.NoError(t, err)
		require.Equal(t, kv.Value, i.GetValue())
	}

	err = client.Disconnect()
	require.NoError(t, err)

	_, err = client.SetAll(ctx, setRequest)
	require.True(t, errors.Is(err, ErrNotConnected))
}

func TestImmuClient_GetAll(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.VerifiedSet(ctx, []byte(`aaa`), []byte(`val`))
	require.NoError(t, err)

	_, err = client.GetAll(ctx, [][]byte{[]byte(`aaa`), []byte(`bbb`)})
	require.Error(t, err)

	_, err = client.VerifiedSet(ctx, []byte(`bbb`), []byte(`val`))
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	entries, err := client.GetAll(ctx, [][]byte{[]byte(`aaa`), []byte(`bbb`)})
	require.NoError(t, err)
	require.Len(t, entries.Entries, 2)

	client.Disconnect()
}

func TestImmuClient_ExecAllOpsOptions(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
		},
	}

	idx, err := client.ExecAll(ctx, aOps)

	require.Nil(t, err)
	require.NotNil(t, idx)

	client.Disconnect()
}

func TestImmuClient_Scan(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, _ = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	_, _ = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val11`))
	_, _ = client.VerifiedSet(ctx, []byte(`key3`), []byte(`val3`))

	entries, err := client.Scan(ctx, &schema.ScanRequest{Prefix: []byte("key"), SinceTx: 3})

	require.IsType(t, &schema.Entries{}, entries)
	require.Nil(t, err)
	require.Len(t, entries.Entries, 2)
	client.Disconnect()
}

func TestImmuClient_TxScan(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect()

	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, _ = client.Set(ctx, []byte(`key1`), []byte(`val1`))
	_, _ = client.Set(ctx, []byte(`key1`), []byte(`val11`))
	_, _ = client.Set(ctx, []byte(`key3`), []byte(`val3`))

	txls, err := client.TxScan(ctx, &schema.TxScanRequest{
		InitialTx: 2,
	})
	require.IsType(t, &schema.TxList{}, txls)
	require.Nil(t, err)
	require.Len(t, txls.Txs, 3)

	txls, err = client.TxScan(ctx, &schema.TxScanRequest{
		InitialTx: 4,
		Limit:     3,
		Desc:      true,
	})
	require.IsType(t, &schema.TxList{}, txls)
	require.Nil(t, err)
	require.Len(t, txls.Txs, 3)

	txls, err = client.TxScan(ctx, &schema.TxScanRequest{
		InitialTx: 3,
		Limit:     1,
		Desc:      true,
	})
	require.IsType(t, &schema.TxList{}, txls)
	require.Nil(t, err)
	require.Len(t, txls.Txs, 1)
	require.Equal(t, database.TrimPrefix(txls.Txs[0].Entries[0].Key), []byte(`key1`))
}

func TestImmuClient_Logout(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts1 := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	ts2 := &TokenServiceMock{
		TokenService: ts1,
		GetTokenF:    ts1.GetToken,
		SetTokenF:    ts1.SetToken,
		DeleteTokenF: ts1.DeleteToken,
		IsTokenPresentF: func() (bool, error) {
			return false, errors.New("some IsTokenPresent error")
		},
	}
	ts3 := *ts2
	ts3.DeleteTokenF = func() error {
		return errors.New("some DeleteToken error")
	}
	ts3.IsTokenPresentF = func() (bool, error) {
		return true, nil
	}
	tokenServices := []TokenService{ts1, ts2, &ts3}
	expectations := []func(error){
		func(err error) { require.Nil(t, err) },
		func(err error) {
			require.NotNil(t, err)
			require.Contains(t, err.Error(), "some IsTokenPresent error")
		},
		func(err error) {
			require.NotNil(t, err)
			require.Contains(t, err.Error(), "some DeleteToken error")
		},
	}

	for i, expect := range expectations {
		clientOpts := DefaultOptions().
			WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).
			WithTokenService(tokenServices[i])
		client, err := NewImmuClient(clientOpts)
		if err != nil {
			log.Fatal(err)
		}

		lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
		if err != nil {
			log.Fatal(err)
		}
		md := metadata.Pairs("authorization", lr.Token)
		ctx := metadata.NewOutgoingContext(context.Background(), md)

		err = client.Logout(ctx)
		expect(err)
		client.Disconnect()
	}
}

func TestImmuClient_GetServiceClient(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}

	cli := client.GetServiceClient()
	require.Implements(t, (*schema.ImmuServiceClient)(nil), cli)
	client.Disconnect()
}

func TestImmuClient_GetOptions(t *testing.T) {
	client := DefaultClient()
	op := client.GetOptions()
	require.IsType(t, &Options{}, op)
}

func TestImmuClient_CurrentRoot(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, _ = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))

	r, err := client.CurrentState(ctx)

	require.IsType(t, &schema.ImmutableState{}, r)
	require.Nil(t, err)
	client.Disconnect()
}

func TestImmuClient_Count(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.Count(ctx, []byte(`key1`))
	require.Error(t, err)
}

func TestImmuClient_CountAll(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	_, err = client.CountAll(ctx)

	require.Error(t, err)
}

/*

func TestImmuClient_SetBatchConcurrent(t *testing.T) {
	setup()
	var wg sync.WaitGroup
	var ris = make(chan int, 5)
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			br := BatchRequest{
				Keys:   []io.Reader{strings.NewReader("key1"), strings.NewReader("key2"), strings.NewReader("key3")},
				Values: []io.Reader{strings.NewReader("val1"), strings.NewReader("val2"), strings.NewReader("val3")},
			}
			idx, err := client.SetBatch(context.TODO(), &br)
			require.NoError(t, err)
			ris <- int(idx.Index)
		}()
	}
	wg.Wait()
	close(ris)
	client.Disconnect()
	s := make([]int, 0)
	for i := range ris {
		s = append(s, i)
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	require.Equal(t, 2, s[0])
	require.Equal(t, 5, s[1])
	require.Equal(t, 8, s[2])
	require.Equal(t, 11, s[3])
	require.Equal(t, 14, s[4])
}

func TestImmuClient_GetBatchConcurrent(t *testing.T) {
	setup()
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			br := BatchRequest{
				Keys:   []io.Reader{strings.NewReader("key1"), strings.NewReader("key2"), strings.NewReader("key3")},
				Values: []io.Reader{strings.NewReader("val1"), strings.NewReader("val2"), strings.NewReader("val3")},
			}
			_, err := client.SetBatch(context.TODO(), &br)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	var wg1 sync.WaitGroup
	var sils = make(chan *schema.StructuredItemList, 2)
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		sil, err := client.GetBatch(context.TODO(), [][]byte{[]byte(`key1`), []byte(`key2`)})
		require.NoError(t, err)
		sils <- sil
	}()
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		sil, err := client.GetBatch(context.TODO(), [][]byte{[]byte(`key3`)})
		require.NoError(t, err)
		sils <- sil
	}()

	wg1.Wait()
	close(sils)

	values := BytesSlice{}
	for sil := range sils {
		for _, val := range sil.Items {
			values = append(values, val.Value.Payload)
		}
	}
	sort.Sort(values)
	require.Equal(t, []byte(`val1`), values[0])
	require.Equal(t, []byte(`val2`), values[1])
	require.Equal(t, []byte(`val3`), values[2])
	client.Disconnect()

}

type BytesSlice [][]byte

func (p BytesSlice) Len() int           { return len(p) }
func (p BytesSlice) Less(i, j int) bool { return bytes.Compare(p[i], p[j]) == -1 }
func (p BytesSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }



func TestImmuClient_GetReference(t *testing.T) {
	setup()
	idx, err := client.Set(context.TODO(), []byte(`key`), []byte(`value`))
	require.NoError(t, err)
	_, err = client.Reference(context.TODO(), []byte(`reference`), []byte(`key`), idx)
	require.NoError(t, err)
	op, err := client.GetReference(context.TODO(), &schema.Key{Key: []byte(`reference`)})
	require.IsType(t, &schema.StructuredItem{}, op)
	require.NoError(t, err)
	client.Disconnect()
}


*/

func TestEnforcedLogoutAfterPasswordChange(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	var (
		userName        = "test"
		userPassword    = "1Password!*"
		userNewPassword = "2Password!*"
		testDBName      = "test"
		testDB          = &schema.Database{DatabaseName: testDBName}
		testUserContext = context.TODO()
	)
	// step 1: create test database
	err = client.CreateDatabase(ctx, &schema.DatabaseSettings{DatabaseName: testDBName})
	require.Nil(t, err)

	// step 2: create test user with read write permissions to the test db
	err = client.CreateUser(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		auth.PermissionRW,
		testDBName,
	)
	require.Nil(t, err)

	// setp 3: create test client and context
	lr, err = client.Login(context.TODO(), []byte(userName), []byte(userPassword))
	if err != nil {
		log.Fatal(err)
	}

	md = metadata.Pairs("authorization", lr.Token)
	testUserContext = metadata.NewOutgoingContext(context.Background(), md)

	dbResp, err := client.UseDatabase(testUserContext, testDB)
	md = metadata.Pairs("authorization", dbResp.Token)
	testUserContext = metadata.NewOutgoingContext(context.Background(), md)

	// step 4: successfully access the test db using the test client
	_, err = client.Set(testUserContext, []byte("sampleKey"), []byte("sampleValue"))
	require.Nil(t, err)

	// step 5: using admin client change the test user password
	err = client.ChangePassword(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		[]byte(userNewPassword),
	)
	require.Nil(t, err)

	// step 6: access the test db again using the test client which should give an error
	_, err = client.Set(testUserContext, []byte("sampleKey"), []byte("sampleValue"))
	require.NotNil(t, err)

	client.Disconnect()
}

func TestImmuClient_CurrentStateVerifiedSignature(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithSigningKey("./../../test/signer/ec1.key")
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	err := bs.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer bs.Stop()

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
	item, err := client.CurrentState(ctx)

	require.IsType(t, &schema.ImmutableState{}, item)
	require.Nil(t, err)
}

func TestImmuClient_VerifiedGetAt(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithSigningKey("./../../test/signer/ec1.key")
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	opts := DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts)
	client, err := NewImmuClient(opts.WithServerSigningPubKey("./../../test/signer/ec1.pub"))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	txMeta0, err := client.Set(ctx, []byte(`key0`), []byte(`val0`))
	require.NoError(t, err)
	entry0, err := client.VerifiedGetAt(ctx, []byte(`key0`), txMeta0.Id)
	require.NoError(t, err)
	require.Equal(t, []byte(`key0`), entry0.Key)
	require.Equal(t, []byte(`val0`), entry0.Value)

	txMeta1, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	require.NoError(t, err)
	txMeta2, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val2`))
	require.NoError(t, err)
	entry, err := client.VerifiedGetAt(ctx, []byte(`key1`), txMeta1.Id)
	require.NoError(t, err)
	require.Equal(t, []byte(`key1`), entry.Key)
	require.Equal(t, []byte(`val1`), entry.Value)

	entry2, err := client.VerifiedGetAt(ctx, []byte(`key1`), txMeta2.Id)
	require.NoError(t, err)
	require.Equal(t, []byte(`key1`), entry2.Key)
	require.Equal(t, []byte(`val2`), entry2.Value)
	client.Disconnect()
}

func TestImmuClient_VerifiedGetSince(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	require.NoError(t, err)
	txMeta2, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val2`))
	require.NoError(t, err)

	entry2, err := client.VerifiedGetSince(ctx, []byte(`key1`), txMeta2.Id)
	require.NoError(t, err)
	require.Equal(t, []byte(`key1`), entry2.Key)
	require.Equal(t, []byte(`val2`), entry2.Value)
	client.Disconnect()
}

func TestImmuClient_BackupAndRestoreUX(t *testing.T) {
	stateFileDir := path.Join(os.TempDir(), "testStates")
	dir := path.Join(os.TempDir(), "data")
	dirAtTx3 := path.Join(os.TempDir(), "dataTx3")

	defer os.RemoveAll(dir)
	defer os.RemoveAll(dirAtTx3)
	defer os.RemoveAll(stateFileDir)

	os.RemoveAll(dir)
	os.RemoveAll(dirAtTx3)

	options := server.DefaultOptions().WithAuth(true).WithDir(dir)
	bs := servertest.NewBufconnServer(options)

	bs.Start()

	cliOpts := DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithDir(stateFileDir)
	cliOpts.CurrentDatabase = DefaultDB
	client, err := NewImmuClient(cliOpts)
	if err != nil {
		log.Fatal(err)
	}
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	if err != nil {
		log.Fatal(err)
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	_, err = client.VerifiedSet(ctx, []byte(`key2`), []byte(`val2`))
	_, err = client.VerifiedSet(ctx, []byte(`key3`), []byte(`val3`))
	require.NoError(t, err)
	_, err = client.VerifiedGet(ctx, []byte(`key3`))
	require.NoError(t, err)
	client.Disconnect()
	bs.Stop()

	copier := fs.NewStandardCopier()
	err = copier.CopyDir(dir, dirAtTx3)
	require.NoError(t, err)

	bs = servertest.NewBufconnServer(options)
	err = bs.Start()
	require.NoError(t, err)

	cliOpts = DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithDir(stateFileDir)
	cliOpts.CurrentDatabase = DefaultDB
	client, err = NewImmuClient(cliOpts)
	require.NoError(t, err)

	lr, err = client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md = metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	_, err = client.VerifiedSet(ctx, []byte(`key2`), []byte(`val2`))
	_, err = client.VerifiedSet(ctx, []byte(`key3`), []byte(`val3`))
	require.NoError(t, err)
	_, err = client.VerifiedGet(ctx, []byte(`key3`))
	client.Disconnect()
	bs.Stop()

	os.RemoveAll(dir)
	err = copier.CopyDir(dirAtTx3, dir)
	require.NoError(t, err)

	bs = servertest.NewBufconnServer(options)
	err = bs.Start()
	require.NoError(t, err)

	cliOpts = DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithDir(stateFileDir)
	cliOpts.CurrentDatabase = DefaultDB
	client, err = NewImmuClient(cliOpts)
	require.NoError(t, err)

	lr, err = client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md = metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.VerifiedGet(ctx, []byte(`key3`))
	require.Equal(t, ErrServerStateIsOlder, err)

	bs.Stop()
}

type HomedirServiceMock struct {
	HomedirService
	WriteFileToUserHomeDirF    func(content []byte, pathToFile string) error
	FileExistsInUserHomeDirF   func(pathToFile string) (bool, error)
	ReadFileFromUserHomeDirF   func(pathToFile string) (string, error)
	DeleteFileFromUserHomeDirF func(pathToFile string) error
}

// WriteFileToUserHomeDir ...
func (h *HomedirServiceMock) WriteFileToUserHomeDir(content []byte, pathToFile string) error {
	return h.WriteFileToUserHomeDirF(content, pathToFile)
}

// FileExistsInUserHomeDir ...
func (h *HomedirServiceMock) FileExistsInUserHomeDir(pathToFile string) (bool, error) {
	return h.FileExistsInUserHomeDirF(pathToFile)
}

// ReadFileFromUserHomeDir ...
func (h *HomedirServiceMock) ReadFileFromUserHomeDir(pathToFile string) (string, error) {
	return h.ReadFileFromUserHomeDirF(pathToFile)
}

// DeleteFileFromUserHomeDir ...
func (h *HomedirServiceMock) DeleteFileFromUserHomeDir(pathToFile string) error {
	return h.DeleteFileFromUserHomeDirF(pathToFile)
}

// DefaultHomedirServiceMock ...
func DefaultHomedirServiceMock() *HomedirServiceMock {
	return &HomedirServiceMock{
		WriteFileToUserHomeDirF: func(content []byte, pathToFile string) error {
			return nil
		},
		FileExistsInUserHomeDirF: func(pathToFile string) (bool, error) {
			return false, nil
		},
		ReadFileFromUserHomeDirF: func(pathToFile string) (string, error) {
			return "", nil
		},
		DeleteFileFromUserHomeDirF: func(pathToFile string) error {
			return nil
		},
	}
}

type TokenServiceMock struct {
	TokenService
	GetTokenF       func() (string, error)
	SetTokenF       func(database string, token string) error
	IsTokenPresentF func() (bool, error)
	DeleteTokenF    func() error
}

func (ts TokenServiceMock) GetToken() (string, error) {
	return ts.GetTokenF()
}

func (ts TokenServiceMock) SetToken(database string, token string) error {
	return ts.SetTokenF(database, token)
}

func (ts TokenServiceMock) DeleteToken() error {
	return ts.DeleteTokenF()
}

func (ts TokenServiceMock) IsTokenPresent() (bool, error) {
	return ts.IsTokenPresentF()
}

func (ts TokenServiceMock) GetDatabase() (string, error) {
	return "", nil
}

func (ts TokenServiceMock) WithHds(hds HomedirService) TokenService {
	return ts
}

func (ts TokenServiceMock) WithTokenFileName(tfn string) TokenService {
	return ts
}
