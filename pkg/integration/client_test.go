/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package integration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/client/homedir"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
	"github.com/rs/xid"

	ic "github.com/codenotary/immudb/pkg/client"
	immuErrors "github.com/codenotary/immudb/pkg/client/errors"

	"github.com/codenotary/immudb/pkg/fs"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/server/servertest"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

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

func setupTestServerAndClient(t *testing.T) (*servertest.BufconnServer, ic.ImmuClient, context.Context) {
	bs := servertest.NewBufconnServer(server.
		DefaultOptions().
		WithMetricsServer(true).
		WithWebServer(true).
		WithDir(filepath.Join(t.TempDir(), "data")).
		WithAuth(true).
		WithLogRequestMetadata(true).
		WithSigningKey("./../../test/signer/ec1.key"),
	)

	bs.Start()
	t.Cleanup(func() { bs.Stop() })

	client, err := bs.NewAuthenticatedClient(ic.
		DefaultOptions().
		WithDir(t.TempDir()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.CloseSession(context.Background()) })
	return bs, client, context.Background()
}

func setupTestServerAndClientWithToken(t *testing.T) (*servertest.BufconnServer, ic.ImmuClient, context.Context) {
	bs := servertest.NewBufconnServer(server.
		DefaultOptions().
		WithDir(t.TempDir()).
		WithAuth(true).
		WithSigningKey("./../../test/signer/ec1.key"),
	)

	bs.Start()
	t.Cleanup(func() { bs.Stop() })

	client, err := ic.NewImmuClient(ic.
		DefaultOptions().
		WithDir(t.TempDir()).
		WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}).
		WithServerSigningPubKey("./../../test/signer/ec1.pub"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Disconnect() })

	client.WithTokenService(tokenservice.NewInmemoryTokenService())
	require.NoError(t, err)

	resp, err := client.Login(context.Background(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", resp.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	return bs, client, ctx
}

func testSafeSetAndSafeGet(ctx context.Context, t *testing.T, key []byte, value []byte, client ic.ImmuClient) {
	_, err := client.VerifiedSet(ctx, key, value)
	require.NoError(t, err)

	vi, err := client.VerifiedGet(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, vi)
	require.Equal(t, key, vi.Key)
	require.Equal(t, value, vi.Value)
}

func testReference(ctx context.Context, t *testing.T, referenceKey []byte, key []byte, value []byte, client ic.ImmuClient) {
	_, err := client.SetReference(ctx, referenceKey, key)
	require.NoError(t, err)

	vi, err := client.VerifiedGet(ctx, referenceKey)
	require.NoError(t, err)
	require.NotNil(t, vi)
	require.Equal(t, key, vi.Key)
	require.Equal(t, value, vi.Value)
}

func testVerifiedReference(ctx context.Context, t *testing.T, key []byte, referencedKey []byte, value []byte, client ic.ImmuClient) {
	md, err := client.VerifiedSetReference(ctx, key, referencedKey)
	require.NoError(t, err)

	vi, err := client.VerifiedGetSince(ctx, key, md.Id)
	require.NoError(t, err)
	require.NotNil(t, vi)
	require.Equal(t, referencedKey, vi.Key)
	require.Equal(t, value, vi.Value)
}

func testVerifiedZAdd(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, client ic.ImmuClient) {
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

func testZAdd(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, client ic.ImmuClient) {
	var md *schema.TxHeader
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

func testZAddAt(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, at uint64, client ic.ImmuClient) {
	var md *schema.TxHeader
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

func testVerifiedZAddAt(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, at uint64, client ic.ImmuClient) {
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

func testGet(ctx context.Context, t *testing.T, client ic.ImmuClient) {
	hdr, err := client.VerifiedSet(ctx, []byte("key-n11"), []byte("val-n11"))
	require.NoError(t, err)

	item, err := client.GetSince(ctx, []byte("key-n11"), hdr.Id)
	require.NoError(t, err)
	require.Equal(t, []byte("key-n11"), item.Key)

	item, err = client.GetAt(ctx, []byte("key-n11"), hdr.Id)
	require.NoError(t, err)
	require.Equal(t, []byte("key-n11"), item.Key)
}

func testGetAtRevision(ctx context.Context, t *testing.T, client ic.ImmuClient) {
	key := []byte("key-atrev")

	_, err := client.Set(ctx, key, []byte("value1"))
	require.NoError(t, err)

	_, err = client.Set(ctx, key, []byte("value2"))
	require.NoError(t, err)

	_, err = client.Set(ctx, key, []byte("value3"))
	require.NoError(t, err)

	_, err = client.Set(ctx, key, []byte("value4"))
	require.NoError(t, err)

	item, err := client.GetAtRevision(ctx, key, 0)
	require.NoError(t, err)
	require.Equal(t, key, item.Key)
	require.Equal(t, []byte("value4"), item.Value)
	require.EqualValues(t, 4, item.Revision)

	vitem, err := client.VerifiedGetAtRevision(ctx, key, 0)
	require.NoError(t, err)
	require.Equal(t, key, vitem.Key)
	require.Equal(t, []byte("value4"), vitem.Value)
	require.EqualValues(t, 4, vitem.Revision)

	item, err = client.GetAtRevision(ctx, key, 1)
	require.NoError(t, err)
	require.Equal(t, key, item.Key)
	require.Equal(t, []byte("value1"), item.Value)
	require.EqualValues(t, 1, item.Revision)

	vitem, err = client.VerifiedGetAtRevision(ctx, key, 1)
	require.NoError(t, err)
	require.Equal(t, key, vitem.Key)
	require.Equal(t, []byte("value1"), vitem.Value)
	require.EqualValues(t, 1, vitem.Revision)

	item, err = client.GetAtRevision(ctx, key, -1)
	require.NoError(t, err)
	require.Equal(t, key, item.Key)
	require.Equal(t, []byte("value3"), item.Value)
	require.EqualValues(t, 3, item.Revision)

	vitem, err = client.VerifiedGetAtRevision(ctx, key, -1)
	require.NoError(t, err)
	require.Equal(t, key, vitem.Key)
	require.Equal(t, []byte("value3"), vitem.Value)
	require.EqualValues(t, 3, vitem.Revision)

	item, err = client.Get(ctx, key, ic.AtRevision(-1))
	require.NoError(t, err)
	require.Equal(t, key, item.Key)
	require.Equal(t, []byte("value3"), item.Value)
	require.EqualValues(t, 3, item.Revision)

	vitem, err = client.VerifiedGet(ctx, key, ic.AtRevision(-1))
	require.NoError(t, err)
	require.Equal(t, key, vitem.Key)
	require.Equal(t, []byte("value3"), vitem.Value)
	require.EqualValues(t, 3, vitem.Revision)
}

func testGetTxByID(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, client ic.ImmuClient) {
	vi1, err := client.VerifiedSet(ctx, []byte("key-n11"), []byte("val-n11"))
	require.NoError(t, err)

	item1, err := client.TxByID(ctx, vi1.Id)
	require.Equal(t, vi1.Ts, item1.Header.Ts)
	require.NoError(t, err)
}

func testImmuClient_VerifiedTxByID(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte, client ic.ImmuClient) {
	vi1, err := client.VerifiedSet(ctx, []byte("key-n11"), []byte("val-n11"))
	require.NoError(t, err)

	item1, err3 := client.VerifiedTxByID(ctx, vi1.Id)
	require.Equal(t, vi1.Ts, item1.Header.Ts)
	require.NoError(t, err3)

	_, err = client.VerifiedSet(ctx, []byte("key-n12"), []byte("val-n12"))
	require.NoError(t, err)

	item1, err3 = client.VerifiedTxByID(ctx, vi1.Id)
	require.Equal(t, vi1.Ts, item1.Header.Ts)
	require.NoError(t, err3)
}

func TestImmuClient(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

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
	testGetAtRevision(ctx, t, client)
}

func TestImmuClientTampering(t *testing.T) {
	bs, client, ctx := setupTestServerAndClient(t)

	_, err := client.Set(ctx, []byte{0}, []byte{0})
	require.NoError(t, err)

	bs.Server.PostSetFn = func(ctx context.Context,
		req *schema.SetRequest, res *schema.TxHeader, err error) (*schema.TxHeader, error) {

		if err != nil {
			return res, err
		}

		res.Nentries = 0

		return res, nil
	}

	_, err = client.Set(ctx, []byte{1}, []byte{1})
	require.ErrorIs(t, err, store.ErrCorruptedData)

	_, err = client.SetAll(ctx, &schema.SetRequest{
		KVs: []*schema.KeyValue{{Key: []byte{1}, Value: []byte{1}}},
	})
	require.ErrorIs(t, err, store.ErrCorruptedData)

	bs.Server.PostVerifiableSetFn = func(ctx context.Context,
		req *schema.VerifiableSetRequest, res *schema.VerifiableTx, err error) (*schema.VerifiableTx, error) {

		if err != nil {
			return res, err
		}

		res.Tx.Header.Nentries = 0

		return res, nil
	}

	_, err = client.VerifiedSet(ctx, []byte{1}, []byte{1})
	require.ErrorIs(t, err, store.ErrCorruptedData)

	bs.Server.PostSetReferenceFn = func(ctx context.Context,
		req *schema.ReferenceRequest, res *schema.TxHeader, err error) (*schema.TxHeader, error) {

		if err != nil {
			return res, err
		}

		res.Nentries = 0

		return res, nil
	}

	_, err = client.SetReference(ctx, []byte{2}, []byte{1})
	require.ErrorIs(t, err, store.ErrCorruptedData)

	bs.Server.PostVerifiableSetReferenceFn = func(ctx context.Context,
		req *schema.VerifiableReferenceRequest, res *schema.VerifiableTx, err error) (*schema.VerifiableTx, error) {

		if err != nil {
			return res, err
		}

		res.Tx.Header.Nentries = 0

		return res, nil
	}

	_, err = client.VerifiedSetReference(ctx, []byte{2}, []byte{1})
	require.ErrorIs(t, err, store.ErrCorruptedData)

	bs.Server.PostZAddFn = func(ctx context.Context,
		req *schema.ZAddRequest, res *schema.TxHeader, err error) (*schema.TxHeader, error) {

		if err != nil {
			return res, err
		}

		res.Nentries = 0

		return res, nil
	}

	_, err = client.ZAdd(ctx, []byte{7}, 1, []byte{1})
	require.ErrorIs(t, err, store.ErrCorruptedData)

	bs.Server.PostVerifiableZAddFn = func(ctx context.Context,
		req *schema.VerifiableZAddRequest, res *schema.VerifiableTx, err error) (*schema.VerifiableTx, error) {

		if err != nil {
			return res, err
		}

		res.Tx.Header.Nentries = 0

		return res, nil
	}

	_, err = client.VerifiedZAdd(ctx, []byte{7}, 1, []byte{1})
	require.ErrorIs(t, err, store.ErrCorruptedData)

	bs.Server.PostExecAllFn = func(ctx context.Context,
		req *schema.ExecAllRequest, res *schema.TxHeader, err error) (*schema.TxHeader, error) {

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
	require.ErrorIs(t, err, store.ErrCorruptedData)
}

func TestReplica(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	err := client.CreateDatabase(ctx, &schema.DatabaseSettings{
		DatabaseName:    "db1",
		Replica:         true,
		PrimaryDatabase: "defaultdb",
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

	md := metadata.Pairs("authorization", resp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.VerifiedSet(ctx, []byte(`db1-key1`), []byte(`db1-value1`))
	require.ErrorContains(t, err, database.ErrIsReplica.Error())
}

func TestDatabasesSwitching(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	err := client.CreateDatabase(ctx, &schema.DatabaseSettings{
		DatabaseName: "db1",
	})
	require.NoError(t, err)

	resp, err := client.UseDatabase(ctx, &schema.Database{
		DatabaseName: "db1",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Token)

	md := metadata.Pairs("authorization", resp.Token)
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
	require.ErrorContains(t, err, "key not found")
	require.Nil(t, vi)
}

func TestDatabasesSwitchingWithInMemoryToken(t *testing.T) {
	_, client, _ := setupTestServerAndClient(t)

	err := client.CreateDatabase(context.Background(), &schema.DatabaseSettings{
		DatabaseName: "db1",
	})
	require.NoError(t, err)

	resp, err := client.UseDatabase(context.Background(), &schema.Database{
		DatabaseName: "db1",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Token)

	_, err = client.VerifiedSet(context.Background(), []byte(`db1-my`), []byte(`item`))
	require.NoError(t, err)

	err = client.CreateDatabase(context.Background(), &schema.DatabaseSettings{
		DatabaseName: "db2",
	})
	require.NoError(t, err)

	resp2, err := client.UseDatabase(context.Background(), &schema.Database{
		DatabaseName: "db2",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp2.Token)

	_, err = client.VerifiedSet(context.Background(), []byte(`db2-my`), []byte(`item`))
	require.NoError(t, err)

	vi, err := client.VerifiedGet(context.Background(), []byte(`db1-my`))
	require.ErrorContains(t, err, "key not found")
	require.Nil(t, vi)
}

func TestImmuClientDisconnect(t *testing.T) {
	_, client, ctx := setupTestServerAndClientWithToken(t)

	err := client.Disconnect()
	require.NoError(t, err)

	require.False(t, client.IsConnected())

	err = client.CreateUser(ctx, []byte("user"), []byte("passwd"), 1, "db")
	require.ErrorIs(t, err, ic.ErrNotConnected)

	err = client.ChangePassword(ctx, []byte("user"), []byte("oldPasswd"), []byte("newPasswd"))
	require.ErrorIs(t, err, ic.ErrNotConnected)

	err = client.UpdateAuthConfig(ctx, auth.KindPassword)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	err = client.UpdateMTLSConfig(ctx, false)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	err = client.CompactIndex(ctx, &emptypb.Empty{})
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.FlushIndex(ctx, 100, true)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.Login(context.Background(), []byte("user"), []byte("passwd"))
	require.True(t, errors.Is(err.(immuErrors.ImmuError), ic.ErrNotConnected))

	require.True(t, errors.Is(client.Logout(context.Background()), ic.ErrNotConnected))

	_, err = client.Get(context.Background(), []byte("key"))
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.CurrentState(context.Background())
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.VerifiedGet(context.Background(), []byte("key"))
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.GetAll(context.Background(), [][]byte{[]byte(`aaa`), []byte(`bbb`)})
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.Scan(context.Background(), &schema.ScanRequest{
		Prefix: []byte("key"),
	})
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.ZScan(context.Background(), &schema.ZScanRequest{Set: []byte("key")})
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.Count(context.Background(), []byte("key"))
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.CountAll(context.Background())
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.Set(context.Background(), []byte("key"), []byte("value"))
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.VerifiedSet(context.Background(), []byte("key"), []byte("value"))
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.Set(context.Background(), nil, nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.Delete(context.Background(), nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.ExecAll(context.Background(), nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.TxByID(context.Background(), 1)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.VerifiedTxByID(context.Background(), 1)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.TxByIDWithSpec(context.Background(), nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.TxScan(context.Background(), nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.History(context.Background(), &schema.HistoryRequest{
		Key: []byte("key"),
	})
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.SetReference(context.Background(), []byte("ref"), []byte("key"))
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.VerifiedSetReference(context.Background(), []byte("ref"), []byte("key"))
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.ZAdd(context.Background(), []byte("set"), 1, []byte("key"))
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.VerifiedZAdd(context.Background(), []byte("set"), 1, []byte("key"))
	require.ErrorIs(t, err, ic.ErrNotConnected)

	//_, err = client.Dump(context.Background(), nil)
	//require.Equal(t, ic.ErrNotConnected, err)

	_, err = client.GetSince(context.Background(), []byte("key"), 0)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.GetAt(context.Background(), []byte("key"), 0)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.ServerInfo(context.Background(), nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	err = client.HealthCheck(context.Background())
	require.ErrorIs(t, err, ic.ErrNotConnected)

	err = client.CreateDatabase(context.Background(), nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.UseDatabase(context.Background(), nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	err = client.ChangePermission(context.Background(), schema.PermissionAction_REVOKE, "userName", "testDBName", auth.PermissionRW)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	err = client.SetActiveUser(context.Background(), nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.ListUsers(context.Background())
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.DatabaseList(context.Background())
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.DatabaseListV2(context.Background())
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.UpdateDatabaseV2(context.Background(), "defaultdb", nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.CurrentState(context.Background())
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.VerifiedSet(context.Background(), nil, nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.VerifiedGet(context.Background(), nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.VerifiedZAdd(context.Background(), nil, 0, nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.VerifiedSetReference(context.Background(), nil, nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)
}

func TestImmuClientDisconnectNotConn(t *testing.T) {
	_, client, _ := setupTestServerAndClientWithToken(t)

	client.Disconnect()
	err := client.Disconnect()
	require.ErrorIs(t, err, ic.ErrNotConnected)
}

func TestWaitForHealthCheck(t *testing.T) {
	_, client, _ := setupTestServerAndClient(t)

	err := client.WaitForHealthCheck(context.Background())
	require.NoError(t, err)
}

func TestWaitForHealthCheckFail(t *testing.T) {
	client := ic.NewClient()
	err := client.WaitForHealthCheck(context.Background())
	require.ErrorIs(t, err, ic.ErrNotConnected)
}

func TestSetupDialOptions(t *testing.T) {
	client := ic.NewClient()

	ts := TokenServiceMock{}
	ts.GetTokenF = func() (string, error) {
		return "token", nil
	}
	client.WithTokenService(ts)

	dialOpts := client.SetupDialOptions(ic.DefaultOptions().WithMTLs(true))
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

	_, client, ctx := setupTestServerAndClient(t)

	err = client.CreateDatabase(ctx, testDB)
	require.NoError(t, err)

	err = client.UpdateAuthConfig(ctx, auth.KindPassword)
	require.ErrorContains(t, err, server.ErrNotSupported.Error())

	err = client.UpdateMTLSConfig(ctx, false)
	require.ErrorContains(t, err, server.ErrNotSupported.Error())

	err = client.CreateUser(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		auth.PermissionRW,
		testDBName,
	)
	require.NoError(t, err)

	err = client.ChangePermission(
		ctx,
		schema.PermissionAction_REVOKE,
		userName,
		testDBName,
		auth.PermissionRW,
	)
	require.NoError(t, err)

	err = client.SetActiveUser(
		ctx,
		&schema.SetActiveUserRequest{
			Active:   true,
			Username: userName,
		})
	require.NoError(t, err)

	err = client.ChangePassword(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		[]byte(userNewPassword),
	)
	require.NoError(t, err)

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
}

func TestDatabaseManagement(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	err1 := client.CreateDatabase(ctx, &schema.DatabaseSettings{DatabaseName: "test"})
	require.NoError(t, err1)

	resp2, err2 := client.DatabaseList(ctx)
	require.NoError(t, err2)
	require.IsType(t, &schema.DatabaseListResponse{}, resp2)
	require.Len(t, resp2.Databases, 2)

	resp3, err3 := client.DatabaseListV2(ctx)
	require.NoError(t, err3)
	require.IsType(t, &schema.DatabaseListResponseV2{}, resp3)
	require.Len(t, resp3.Databases, 2)
}

func TestImmuClient_History(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	_, _ = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	hdr, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val2`))
	require.NoError(t, err)

	sil, err := client.History(ctx, &schema.HistoryRequest{
		Key:     []byte(`key1`),
		SinceTx: hdr.Id,
	})

	require.NoError(t, err)
	require.Len(t, sil.Entries, 2)
}

func TestImmuClient_SetAll(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	_, err := client.SetAll(ctx, nil)
	require.ErrorContains(t, err, "Marshal called with nil")

	setRequest := &schema.SetRequest{KVs: []*schema.KeyValue{}}
	_, err = client.SetAll(ctx, setRequest)
	require.ErrorContains(t, err, "no entries provided")

	setRequest = &schema.SetRequest{KVs: []*schema.KeyValue{
		{Key: []byte("1,2,3"), Value: []byte("3,2,1")},
		{Key: []byte("4,5,6"), Value: []byte("6,5,4"), Metadata: &schema.KVMetadata{NonIndexable: true}},
	}}

	_, err = client.SetAll(ctx, setRequest)
	require.NoError(t, err)

	_, err = client.FlushIndex(ctx, 1, false)
	require.NoError(t, err)

	for _, kv := range setRequest.KVs {
		i, err := client.Get(ctx, kv.Key)

		if kv.Metadata != nil && kv.Metadata.NonIndexable {
			require.Contains(t, err.Error(), "key not found")
		} else {
			require.NoError(t, err)
			require.Equal(t, kv.Value, i.GetValue())
		}
	}

	err = client.CloseSession(ctx)
	require.NoError(t, err)

	_, err = client.SetAll(ctx, setRequest)
	require.ErrorIs(t, err, ic.ErrNotConnected)
}

func TestImmuClient_GetAll(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	_, err := client.VerifiedSet(ctx, []byte(`aaa`), []byte(`val`))
	require.NoError(t, err)

	entries, err := client.GetAll(ctx, [][]byte{[]byte(`aaa`), []byte(`bbb`)})
	require.NoError(t, err)
	require.Len(t, entries.Entries, 1)

	_, err = client.FlushIndex(ctx, 10, true)
	require.NoError(t, err)

	_, err = client.VerifiedSet(ctx, []byte(`bbb`), []byte(`val`))
	require.NoError(t, err)

	_, err = client.FlushIndex(ctx, 10, true)
	require.NoError(t, err)

	err = client.CompactIndex(ctx, &emptypb.Empty{})
	require.NoError(t, err)

	entries, err = client.GetAll(ctx, [][]byte{[]byte(`aaa`), []byte(`bbb`)})
	require.NoError(t, err)
	require.Len(t, entries.Entries, 2)

	err = client.TruncateDatabase(ctx, "defaultdb", 1*time.Hour)
	require.ErrorContains(t, err, "database is reserved")
}

func TestImmuClient_Delete(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	_, err := client.Delete(ctx, nil)
	require.ErrorContains(t, err, "Marshal called with nil")

	deleteRequest := &schema.DeleteKeysRequest{}
	_, err = client.Delete(ctx, deleteRequest)
	require.ErrorContains(t, err, "no entries provided")

	_, err = client.Set(ctx, []byte("1,2,3"), []byte("3,2,1"))
	require.NoError(t, err)

	i, err := client.Get(ctx, []byte("1,2,3"))
	require.NoError(t, err)
	require.Equal(t, []byte("3,2,1"), i.GetValue())

	_, err = client.ExpirableSet(ctx, []byte("expirableKey"), []byte("expirableValue"), time.Now())
	require.NoError(t, err)

	_, err = client.Get(ctx, []byte("expirableKey"))
	require.ErrorContains(t, err, "key not found")

	deleteRequest.Keys = append(deleteRequest.Keys, []byte("1,2,3"))
	_, err = client.Delete(ctx, deleteRequest)
	require.NoError(t, err)

	_, err = client.Get(ctx, []byte("1,2,3"))
	require.ErrorContains(t, err, "key not found")

	_, err = client.Delete(ctx, deleteRequest)
	require.ErrorContains(t, err, "key not found")
}

func TestImmuClient_ExecAllOpsOptions(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

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

	require.NoError(t, err)
	require.NotNil(t, idx)
}

func TestImmuClient_Scan(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	_, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	require.NoError(t, err)
	_, err = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val11`))
	require.NoError(t, err)
	_, err = client.VerifiedSet(ctx, []byte(`key3`), []byte(`val3`))
	require.NoError(t, err)

	entries, err := client.Scan(ctx, &schema.ScanRequest{Prefix: []byte("key"), SinceTx: 3})
	require.NoError(t, err)
	require.IsType(t, &schema.Entries{}, entries)
	require.Len(t, entries.Entries, 2)
}

func TestImmuClient_TxScan(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	_, err := client.Set(ctx, []byte(`key1`), []byte(`val1`))
	require.NoError(t, err)
	_, err = client.Set(ctx, []byte(`key1`), []byte(`val11`))
	require.NoError(t, err)
	_, err = client.Set(ctx, []byte(`key3`), []byte(`val3`))
	require.NoError(t, err)

	txls, err := client.TxScan(ctx, &schema.TxScanRequest{
		InitialTx: 1,
	})
	require.IsType(t, &schema.TxList{}, txls)
	require.NoError(t, err)
	require.Len(t, txls.Txs, 3)

	txls, err = client.TxScan(ctx, &schema.TxScanRequest{
		InitialTx: 3,
		Limit:     3,
		Desc:      true,
	})
	require.IsType(t, &schema.TxList{}, txls)
	require.NoError(t, err)
	require.Len(t, txls.Txs, 3)

	txls, err = client.TxScan(ctx, &schema.TxScanRequest{
		InitialTx: 2,
		Limit:     1,
		Desc:      true,
	})
	require.IsType(t, &schema.TxList{}, txls)
	require.NoError(t, err)
	require.Len(t, txls.Txs, 1)
	require.Equal(t, database.TrimPrefix(txls.Txs[0].Entries[0].Key), []byte(`key1`))
}

func TestImmuClient_Logout(t *testing.T) {
	bs := servertest.NewBufconnServer(server.
		DefaultOptions().
		WithDir(t.TempDir()).
		WithAuth(true),
	)

	bs.Start()
	defer bs.Stop()

	ts1 := tokenservice.NewInmemoryTokenService()
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
	tokenServices := []tokenservice.TokenService{ts1, ts2, &ts3}
	expectations := []func(error){
		func(err error) {
			require.NoError(t, err)
		},
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
		client, err := ic.NewImmuClient(ic.
			DefaultOptions().
			WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}).
			WithDir(t.TempDir()),
		)
		if err != nil {
			expect(err)
			continue
		}
		client.WithTokenService(tokenServices[i])

		lr, err := client.Login(context.Background(), []byte(`immudb`), []byte(`immudb`))
		if err != nil {
			expect(err)
			continue
		}
		md := metadata.Pairs("authorization", lr.Token)
		ctx := metadata.NewOutgoingContext(context.Background(), md)

		err = client.Logout(ctx)
		expect(err)
		err = client.Disconnect()
		require.NoError(t, err)
	}
}

func TestImmuClient_GetServiceClient(t *testing.T) {
	_, client, _ := setupTestServerAndClient(t)

	cli := client.GetServiceClient()
	require.Implements(t, (*schema.ImmuServiceClient)(nil), cli)
}

func TestImmuClient_GetOptions(t *testing.T) {
	client := ic.NewClient()
	op := client.GetOptions()
	require.IsType(t, &ic.Options{}, op)
}

func TestImmuClient_ServerInfo(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	resp, err := client.ServerInfo(ctx, &schema.ServerInfoRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "", resp.Version)
}

func TestImmuClient_CurrentRoot(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	_, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	require.NoError(t, err)

	r, err := client.CurrentState(ctx)
	require.NoError(t, err)
	require.IsType(t, &schema.ImmutableState{}, r)

	healthRes, err := client.Health(ctx)
	require.NoError(t, err)
	require.NotNil(t, healthRes)
	require.Equal(t, uint32(0x0), healthRes.PendingRequests)
}

func TestImmuClient_Count(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	res, err := client.Count(ctx, []byte(`key1`))
	require.NoError(t, err)
	require.Zero(t, res.Count)
}

func TestImmuClient_CountAll(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	res, err := client.CountAll(ctx)
	require.NoError(t, err)
	require.Zero(t, res.Count)
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
			idx, err := client.SetBatch(context.Background(), &br)
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
			_, err := client.SetBatch(context.Background(), &br)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	var wg1 sync.WaitGroup
	var sils = make(chan *schema.StructuredItemList, 2)
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		sil, err := client.GetBatch(context.Background(), [][]byte{[]byte(`key1`), []byte(`key2`)})
		require.NoError(t, err)
		sils <- sil
	}()
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		sil, err := client.GetBatch(context.Background(), [][]byte{[]byte(`key3`)})
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
	idx, err := client.Set(context.Background(), []byte(`key`), []byte(`value`))
	require.NoError(t, err)
	_, err = client.Reference(context.Background(), []byte(`reference`), []byte(`key`), idx)
	require.NoError(t, err)
	op, err := client.GetReference(context.Background(), &schema.Key{Key: []byte(`reference`)})
	require.IsType(t, &schema.StructuredItem{}, op)
	require.NoError(t, err)
	client.Disconnect()
}


*/

func TestEnforcedLogoutAfterPasswordChangeWithToken(t *testing.T) {
	_, client, ctx := setupTestServerAndClientWithToken(t)

	var (
		userName        = "test"
		userPassword    = "1Password!*"
		userNewPassword = "2Password!*"
		testDBName      = "test"
		testDB          = &schema.Database{DatabaseName: testDBName}
		testUserContext = context.Background()
	)
	// step 1: create test database
	err := client.CreateDatabase(ctx, &schema.DatabaseSettings{DatabaseName: testDBName})
	require.NoError(t, err)

	// step 2: create test user with read write permissions to the test db
	err = client.CreateUser(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		auth.PermissionRW,
		testDBName,
	)
	require.NoError(t, err)

	// step 3: create test client and context
	lr, err := client.Login(context.Background(), []byte(userName), []byte(userPassword))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	testUserContext = metadata.NewOutgoingContext(context.Background(), md)

	dbResp, err := client.UseDatabase(testUserContext, testDB)
	md = metadata.Pairs("authorization", dbResp.Token)
	testUserContext = metadata.NewOutgoingContext(context.Background(), md)

	// step 4: successfully access the test db using the test client
	_, err = client.Set(testUserContext, []byte("sampleKey"), []byte("sampleValue"))
	require.NoError(t, err)

	// step 5: using admin client change the test user password
	err = client.ChangePassword(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		[]byte(userNewPassword),
	)
	require.NoError(t, err)

	// step 6: access the test db again using the test client which should give an error
	_, err = client.Set(testUserContext, []byte("sampleKey"), []byte("sampleValue"))
	require.ErrorContains(t, err, auth.ErrNotLoggedIn.Error())
}

func TestEnforcedLogoutAfterPasswordChangeWithSessions(t *testing.T) {
	t.SkipNow()
	bs, client, ctx := setupTestServerAndClient(t)

	var (
		userName        = "test"
		userPassword    = "1Password!*"
		userNewPassword = "2Password!*"
		testDBName      = "test"
		testUserContext = context.Background()
	)
	// step 1: create test database
	err := client.CreateDatabase(ctx, &schema.DatabaseSettings{DatabaseName: testDBName})
	require.NoError(t, err)

	// step 2: create test user with read write permissions to the test db
	err = client.CreateUser(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		auth.PermissionRW,
		testDBName,
	)
	require.NoError(t, err)

	// step 3: create test client and context
	testClient := bs.NewClient(ic.DefaultOptions().WithDir(t.TempDir()))

	err = testClient.OpenSession(context.Background(), []byte(userName), []byte(userPassword), testDBName)
	require.NoError(t, err)

	// step 4: successfully access the test db using the test client
	_, err = testClient.Set(testUserContext, []byte("sampleKey"), []byte("sampleValue"))
	require.NoError(t, err)

	// step 5: using admin client change the test user password
	err = client.ChangePassword(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		[]byte(userNewPassword),
	)
	require.NoError(t, err)

	// step 6: access the test db again using the test client which should give an error
	_, err = testClient.Set(testUserContext, []byte("sampleKey"), []byte("sampleValue"))
	require.Error(t, err)
}

func TestImmuClient_CurrentStateVerifiedSignature(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	item, err := client.CurrentState(ctx)
	require.IsType(t, &schema.ImmutableState{}, item)
	require.NoError(t, err)
}

func TestImmuClient_VerifiedGetAt(t *testing.T) {
	bs, client, ctx := setupTestServerAndClient(t)

	txHdr0, err := client.Set(ctx, []byte(`key0`), []byte(`val0`))
	require.NoError(t, err)

	entry0, err := client.VerifiedGetAt(ctx, []byte(`key0`), txHdr0.Id)
	require.NoError(t, err)
	require.Equal(t, []byte(`key0`), entry0.Key)
	require.Equal(t, []byte(`val0`), entry0.Value)

	txHdr1, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	require.NoError(t, err)

	txHdr2, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val2`))
	require.NoError(t, err)

	entry, err := client.VerifiedGetAt(ctx, []byte(`key1`), txHdr1.Id)
	require.NoError(t, err)
	require.Equal(t, []byte(`key1`), entry.Key)
	require.Equal(t, []byte(`val1`), entry.Value)

	entry2, err := client.VerifiedGetAt(ctx, []byte(`key1`), txHdr2.Id)
	require.NoError(t, err)
	require.Equal(t, []byte(`key1`), entry2.Key)
	require.Equal(t, []byte(`val2`), entry2.Value)

	bs.Server.PreVerifiableGetFn = func(ctx context.Context, req *schema.VerifiableGetRequest) {
		req.KeyRequest.AtTx = txHdr1.Id
	}
	_, err = client.VerifiedGetAt(ctx, []byte(`key1`), txHdr2.Id)
	require.ErrorIs(t, err, store.ErrCorruptedData)

	bs.Server.PreVerifiableSetFn = func(ctx context.Context, req *schema.VerifiableSetRequest) {
		req.SetRequest.KVs[0].Value = []byte(`val2`)
	}

	_, err = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val3`))
	require.ErrorIs(t, err, store.ErrCorruptedData)
}

func TestImmuClient_VerifiedGetSince(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	_, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
	require.NoError(t, err)
	txMeta2, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val2`))
	require.NoError(t, err)

	entry2, err := client.VerifiedGetSince(ctx, []byte(`key1`), txMeta2.Id)
	require.NoError(t, err)
	require.Equal(t, []byte(`key1`), entry2.Key)
	require.Equal(t, []byte(`val2`), entry2.Value)
}

func TestImmuClient_BackupAndRestoreUX(t *testing.T) {

	var (
		uuid         xid.ID
		serverOpts   *server.Options
		stateFileDir = t.TempDir()
		dirAtTx3     = filepath.Join(t.TempDir(), "data")
		copier       = fs.NewStandardCopier()
	)

	// Setup the initial test server outside t.Run to ensure the main data folder
	// is present during whole test
	bs, client, ctx := setupTestServerAndClient(t)

	t.Run("write initial 3 Txs", func(t *testing.T) {
		uuid = bs.GetUUID()
		serverOpts = bs.Options
		defer bs.Stop()
		defer client.CloseSession(context.Background())

		_, err := client.VerifiedSet(ctx, []byte(`key1`), []byte(`val1`))
		require.NoError(t, err)

		_, err = client.VerifiedSet(ctx, []byte(`key2`), []byte(`val2`))
		require.NoError(t, err)

		_, err = client.VerifiedSet(ctx, []byte(`key3`), []byte(`val3`))
		require.NoError(t, err)

		_, err = client.VerifiedGet(ctx, []byte(`key3`))
		require.NoError(t, err)

		err = client.CloseSession(context.Background())
		require.NoError(t, err)

		err = bs.Stop()
		require.NoError(t, err)
	})

	t.Run("preserve data at Tx 3", func(t *testing.T) {
		err := copier.CopyDir(serverOpts.Dir, dirAtTx3)
		require.NoError(t, err)
	})

	t.Run("add some more transactions to the database", func(t *testing.T) {
		bs := servertest.NewBufconnServer(serverOpts)
		bs.SetUUID(uuid)
		err := bs.Start()
		require.NoError(t, err)
		defer bs.Stop()

		client, err := bs.NewAuthenticatedClient(ic.DefaultOptions().WithDir(stateFileDir))
		require.NoError(t, err)
		defer client.CloseSession(context.Background())

		_, err = client.VerifiedSet(context.Background(), []byte(`key1`), []byte(`val1`))
		require.NoError(t, err)
		_, err = client.VerifiedSet(context.Background(), []byte(`key2`), []byte(`val2`))
		require.NoError(t, err)
		_, err = client.VerifiedSet(context.Background(), []byte(`key3`), []byte(`val3`))
		require.NoError(t, err)
		_, err = client.VerifiedGet(context.Background(), []byte(`key3`))
		require.NoError(t, err)
		err = bs.Stop()
		require.NoError(t, err)
	})

	t.Run("clients will fail after restoring older dataset", func(t *testing.T) {
		os.RemoveAll(serverOpts.Dir)
		err := copier.CopyDir(dirAtTx3, serverOpts.Dir)
		require.NoError(t, err)

		bs := servertest.NewBufconnServer(serverOpts)
		bs.SetUUID(uuid)
		err = bs.Start()
		require.NoError(t, err)
		defer bs.Stop()

		client, err := bs.NewAuthenticatedClient(ic.DefaultOptions().WithDir(stateFileDir))
		require.NoError(t, err)
		defer client.CloseSession(context.Background())

		_, err = client.VerifiedGet(context.Background(), []byte(`key3`))
		require.ErrorIs(t, err, ic.ErrServerStateIsOlder)
	})
}

type HomedirServiceMock struct {
	homedir.HomedirService
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
	tokenservice.TokenService
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

func (ts TokenServiceMock) WithHds(hds homedir.HomedirService) tokenservice.TokenService {
	return ts
}

func (ts TokenServiceMock) WithTokenFileName(tfn string) tokenservice.TokenService {
	return ts
}

func TestServerLogRequestMetadata(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	requireMetadataPresent := func(hdr *schema.TxHeader) {
		txmd := schema.Metadata{}
		err := txmd.Unmarshal(hdr.Metadata.Extra)
		require.NoError(t, err)

		require.Equal(t, schema.Metadata{schema.UserRequestMetadataKey: auth.SysAdminUsername, schema.IpRequestMetadataKey: "bufconn"}, txmd)
	}

	hdr, err := client.Set(ctx, []byte("test"), []byte("test"))
	require.NoError(t, err)

	requireMetadataPresent(hdr)

	hdr1, err := client.VerifiedSet(ctx, []byte("test"), []byte("test"))
	require.NoError(t, err)

	requireMetadataPresent(hdr1)
	require.NoError(t, err)

	_, err = client.SQLExec(ctx, "CREATE TABLE mytable (id INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	tx, err := client.TxByID(ctx, 3)
	require.NoError(t, err)

	requireMetadataPresent(tx.Header)
}
