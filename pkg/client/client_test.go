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
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/client/timestamp"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

const BkpFileName = "client_test.dump.bkp"
const ExpectedBkpFileName = "./../../test/client_test.expected.bkp"

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

type ntpMock struct {
	t time.Time
}

func NewNtpMock() (timestamp.TsGenerator, error) {
	i, err := strconv.ParseInt("1405544146", 10, 64)
	if err != nil {
		panic(err)
	}
	tm := time.Unix(i, 0)
	return &ntpMock{tm}, nil
}

func (n *ntpMock) Now() time.Time {
	return n.t
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

/*

func testSafeReference(ctx context.Context, t *testing.T, referenceKey []byte, key []byte, value []byte) {
	_, err2 := client.SafeReference(ctx, referenceKey, key, nil)
	require.NoError(t, err2)
	vi, err := client.SafeGet(ctx, referenceKey)
	require.NoError(t, err)
	require.NotNil(t, vi)
	require.Equal(t, key, vi.Key)
	require.Equal(t, value, vi.Value)
	require.Equal(t, uint64(1405544146), vi.Time)
	require.True(t, vi.Verified)
}

func testSafeZAdd(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte) {
	for i := 0; i < len(scores); i++ {
		_, err := client.SafeZAdd(ctx, set, scores[i], keys[i], nil)
		require.NoError(t, err)
	}
	itemList, err := client.ZScan(ctx, &schema.ZScanOptions{
		Set: set,
	})
	require.NoError(t, err)
	require.NotNil(t, itemList)
	require.Len(t, itemList.Items, len(keys))

	for i := 0; i < len(keys); i++ {
		require.Equal(t, keys[i], itemList.Items[i].Item.Key)
		require.Equal(t, values[i], itemList.Items[i].Item.Value.Payload)
		require.Equal(t, uint64(1405544146), itemList.Items[i].Item.Value.Timestamp)
	}
}

func testGetByRawIndexOnSafeZAdd(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte) {

	vi1, err1 := client.RawSafeSet(ctx, []byte("key-n1"), []byte("val-n1"))
	require.True(t, vi1.Verified)
	require.NoError(t, err1)
	vi2, err2 := client.SafeZAdd(ctx, []byte("set-n1"), 98.5, []byte("key-n1"), nil)
	require.True(t, vi2.Verified)
	require.NoError(t, err2)

	item1, err3 := client.RawBySafeIndex(ctx, 9)
	require.True(t, item1.Verified)
	require.Equal(t, []byte("val-n1"), item1.Value)
	require.NoError(t, err3)
	item2, err2 := client.RawBySafeIndex(ctx, 1)
	require.True(t, item2.Verified)
	require.NoError(t, err2)
	item3, err3 := client.RawBySafeIndex(ctx, 2)
	require.True(t, item3.Verified)
	require.NoError(t, err3)
}

func testGetByRawIndexOnZAdd(ctx context.Context, t *testing.T, set []byte, scores []float64, keys [][]byte, values [][]byte) {
	vi1, err1 := client.RawSafeSet(ctx, []byte("key-n11"), []byte("val-n11"))
	require.True(t, vi1.Verified)
	require.NoError(t, err1)
	index, err2 := client.ZAdd(ctx, []byte("set-n11"), 98.5, []byte("key-n11"), nil)
	require.NoError(t, err2)

	item1, err3 := client.RawBySafeIndex(ctx, index.Index)
	require.True(t, item1.Verified)
	key, _, _ := store.UnwrapZIndexReference(item1.Value)
	require.Equal(t, []byte("key-n11"), key, nil)
	require.NoError(t, err3)
}
*/
func testGet(ctx context.Context, t *testing.T, client ImmuClient) {
	txmd, err := client.VerifiedSet(ctx, []byte("key-n11"), []byte("val-n11"))
	require.NoError(t, err)

	item, err := client.GetSince(ctx, []byte("key-n11"), txmd.Id)
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

// func testDump(ctx context.Context, t *testing.T) {
// 	bkpFile, err := os.Create(BkpFileName)
// 	require.NoError(t, err)
// 	n, err := client.Dump(ctx, bkpFile)

// 	require.NoError(t, err)
// 	require.Equal(t, int64(41), n)

// 	bkpBytesActual, err := ioutil.ReadFile(BkpFileName)
// 	require.NoError(t, err)
// 	require.NotEmpty(t, bkpBytesActual)
// 	bkpBytesExpected, err := ioutil.ReadFile(ExpectedBkpFileName)
// 	require.NoError(t, err)
// 	require.NotEmpty(t, bkpBytesExpected)
// 	require.Equal(t, bkpBytesExpected, bkpBytesActual)
// }

func TestImmuClient(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
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

	//testSafeReference(ctx, t, testData.refKeys[0], testData.keys[0], testData.values[0])
	//testSafeReference(ctx, t, testData.refKeys[1], testData.keys[1], testData.values[1])
	//testSafeReference(ctx, t, testData.refKeys[2], testData.keys[2], testData.values[2])
	//testSafeZAdd(ctx, t, testData.set, testData.scores, testData.keys, testData.values)
	//testGetByRawIndexOnSafeZAdd(ctx, t, testData.set, testData.scores, testData.keys, testData.values)
	//testGetByRawIndexOnZAdd(ctx, t, testData.set, testData.scores, testData.keys, testData.values)

	testReference(ctx, t, testData.refKeys[0], testData.keys[0], testData.values[0], client)
	testGetTxByID(ctx, t, testData.set, testData.scores, testData.keys, testData.values, client)

	testGet(ctx, t, client)

	//dump comparison will not work because at start user immu is automatically created and a time stamp of creation is used which will always make dumps different
	//userdata.CreatedAt = time.Now()
	//testDump(ctx, t)

}

func TestDatabasesSwitching(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

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

	err = client.CreateDatabase(ctx, &schema.Database{
		Databasename: "db1",
	})
	require.NoError(t, err)

	resp, err := client.UseDatabase(ctx, &schema.Database{
		Databasename: "db1",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Token)

	_, err = client.VerifiedSet(ctx, []byte(`db1-my`), []byte(`item`))
	assert.NoError(t, err)

	err = client.CreateDatabase(ctx, &schema.Database{
		Databasename: "db2",
	})
	assert.NoError(t, err)

	resp2, err := client.UseDatabase(ctx, &schema.Database{
		Databasename: "db2",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp2.Token)

	md = metadata.Pairs("authorization", resp2.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.VerifiedSet(ctx, []byte(`db2-my`), []byte(`item`))
	require.NoError(t, err)

	vi, err := client.VerifiedGet(ctx, []byte(`db1-my`))
	require.Error(t, store.ErrKeyNotFound, err)
	require.Nil(t, vi)
}

// todo(joe-dz): Enable restore when the feature is required again.
//func TestRestore(t *testing.T) {
//	cleanup()
//	defer cleanup()
//
//	ctx := context.Background()
//
//	// this only succeeds if only this test function is run, otherwise the key may
//	// be present from other test function that run before this:
//	// r1, err := client.Connected(ctx, func() (interface{}, error) {
//	// 	return client.SafeGet(ctx, testData.keys[1])
//	// })
//	// require.Error(t, err)
//	// require.Nil(t, r1)
//
//	bkpFileForRead, err := os.Open(ExpectedBkpFileName)
//	require.NoError(t, err)
//	r2, err := client.Connected(ctx, func() (interface{}, error) {
//		return client.Restore(ctx, bkpFileForRead, 20)
//	})
//	require.NoError(t, err)
//	n2 := r2.(int64)
//	require.Equal(t, int64(26), n2)
//
//	r3, err := client.Connected(ctx, func() (interface{}, error) {
//		return client.SafeGet(ctx, testData.keys[1])
//	})
//	require.NoError(t, err)
//	require.NotNil(t, r3)
//	vi := r3.(*VerifiedItem)
//	require.Equal(t, testData.keys[1], vi.CurrentOffset)
//	require.Equal(t, testData.values[1], vi.Value)
//	require.True(t, vi.Verified)
//
//	r4, err := client.Connected(ctx, func() (interface{}, error) {
//		return client.SafeGet(ctx, testData.refKeys[2])
//	})
//	require.NoError(t, err)
//	require.NotNil(t, r4)
//	viFromRef := r4.(*VerifiedItem)
//	require.Equal(t, testData.keys[2], viFromRef.CurrentOffset)
//	require.Equal(t, testData.values[2], viFromRef.Value)
//	require.True(t, viFromRef.Verified)
//
//	r5, err := client.Connected(ctx, func() (interface{}, error) {
//		return client.ZScan(ctx, testData.set)
//	})
//	require.NoError(t, err)
//	require.NotNil(t, r5)
//	itemList := r5.(*schema.StructuredItemList)
//	require.Len(t, itemList.Items, len(testData.keys))
//
//	for i := 0; i < len(testData.keys); i++ {
//		require.Equal(t, testData.keys[i], itemList.Items[i].CurrentOffset)
//		require.Equal(t, testData.values[i], itemList.Items[i].Value.Payload)
//	}
//}
func TestImmuClientDisconnect(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

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

	err = client.Disconnect()
	require.Nil(t, err)

	require.False(t, client.IsConnected())

	require.Error(t, ErrNotConnected, client.CreateUser(ctx, []byte("user"), []byte("passwd"), 1, "db"))
	require.Error(t, ErrNotConnected, client.ChangePassword(ctx, []byte("user"), []byte("oldPasswd"), []byte("newPasswd")))
	require.Error(t, ErrNotConnected, client.UpdateAuthConfig(ctx, auth.KindPassword))
	require.Error(t, ErrNotConnected, client.UpdateMTLSConfig(ctx, false))

	_, err = client.PrintTree(ctx)
	require.Error(t, ErrNotConnected, err)

	_, err = client.Login(context.TODO(), []byte("user"), []byte("passwd"))
	require.Error(t, ErrNotConnected, err)

	require.Error(t, ErrNotConnected, client.Logout(context.TODO()))

	_, err = client.Get(context.TODO(), []byte("key"))
	require.Error(t, ErrNotConnected, err)

	_, err = client.CurrentState(context.TODO())
	require.Error(t, ErrNotConnected, err)

	_, err = client.VerifiedGet(context.TODO(), []byte("key"))
	require.Error(t, ErrNotConnected, err)

	_, err = client.Scan(context.TODO(), &schema.ScanRequest{
		Prefix: []byte("key"),
	})

	require.Error(t, ErrNotConnected, err)

	_, err = client.ZScan(context.TODO(), &schema.ZScanRequest{Set: []byte("key")})
	require.Error(t, ErrNotConnected, err)

	_, err = client.Count(context.TODO(), []byte("key"))
	require.Error(t, ErrNotConnected, err)

	_, err = client.CountAll(context.TODO())
	require.Error(t, ErrNotConnected, err)

	_, err = client.Set(context.TODO(), []byte("key"), []byte("value"))
	require.Error(t, ErrNotConnected, err)

	_, err = client.VerifiedSet(context.TODO(), []byte("key"), []byte("value"))
	require.Error(t, ErrNotConnected, err)

	_, err = client.Set(context.TODO(), nil, nil)
	require.Error(t, ErrNotConnected, err)

	_, err = client.TxByID(context.TODO(), 1)
	require.Error(t, ErrNotConnected, err)

	_, err = client.VerifiedTxByID(context.TODO(), 1)
	require.Error(t, ErrNotConnected, err)

	/*_, err = client.History(context.TODO(), &schema.HistoryRequest{
		Key: []byte("key"),
	})
	require.Error(t, ErrNotConnected, err)*/

	_, err = client.SetReference(context.TODO(), []byte("ref"), []byte("key"))
	require.Error(t, ErrNotConnected, err)

	_, err = client.VerifiedSetReference(context.TODO(), []byte("ref"), []byte("key"))
	require.Error(t, ErrNotConnected, err)

	_, err = client.ZAdd(context.TODO(), []byte("set"), 1, []byte("key"))
	require.Error(t, ErrNotConnected, err)

	_, err = client.VerifiedZAdd(context.TODO(), []byte("set"), 1, []byte("key"))
	require.Error(t, ErrNotConnected, err)

	_, err = client.Dump(context.TODO(), nil)
	require.Error(t, ErrNotConnected, err)

	require.Error(t, ErrNotConnected, client.HealthCheck(context.TODO()))

	require.Error(t, ErrNotConnected, client.CreateDatabase(context.TODO(), nil))

	_, err = client.UseDatabase(context.TODO(), nil)
	require.Error(t, ErrNotConnected, err)

	err = client.ChangePermission(context.TODO(), schema.PermissionAction_REVOKE, "userName", "testDBName", auth.PermissionRW)
	require.Error(t, ErrNotConnected, err)

	require.Error(t, ErrNotConnected, client.SetActiveUser(context.TODO(), nil))

	_, err = client.DatabaseList(context.TODO())
	require.Error(t, ErrNotConnected, err)
}

func TestImmuClientDisconnectNotConn(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}

	client.Disconnect()
	err = client.Disconnect()
	assert.Error(t, err)
	assert.Errorf(t, err, "not connected")
}

func TestWaitForHealthCheck(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	ts := NewTokenService().WithTokenFileName("testTokenFile").WithHds(DefaultHomedirServiceMock())
	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).WithTokenService(ts))
	if err != nil {
		log.Fatal(err)
	}
	err = client.WaitForHealthCheck(context.TODO())
	assert.Nil(t, err)
}

func TestWaitForHealthCheckFail(t *testing.T) {
	client := DefaultClient()
	err := client.WaitForHealthCheck(context.TODO())
	assert.Error(t, err)
}

/*
func TestDump(t *testing.T) {
	setup()
	_, _ = client.VerifiedSet(context.TODO(), []byte(`key`), []byte(`val`))
	f, _ := os.Create("tmp")
	i, err := client.Dump(context.TODO(), f)
	_ = os.Remove("tmp")
	assert.Nil(t, err)
	assert.True(t, i > 0)
	client.Disconnect()
}
*/

func TestSetupDialOptions(t *testing.T) {
	client := DefaultClient()
	dialOpts := client.SetupDialOptions(DefaultOptions().WithMTLs(true))
	require.NotNil(t, dialOpts)
}

func TestUserManagement(t *testing.T) {

	var (
		userName        = "test"
		userPassword    = "1Password!*"
		userNewPassword = "2Password!*"
		testDBName      = "test"
		testDB          = &schema.Database{Databasename: testDBName}
		err             error
		usrList         *schema.UserList
		immudbUser      *schema.User
		testUser        *schema.User
	)

	options := server.DefaultOptions().WithAuth(true).WithConfig("../../configs/immudb.toml")
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

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
	assert.Nil(t, err)

	err = client.CreateUser(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		auth.PermissionRW,
		testDBName,
	)
	assert.Nil(t, err)

	err = client.ChangePermission(
		ctx,
		schema.PermissionAction_REVOKE,
		userName,
		testDBName,
		auth.PermissionRW,
	)
	assert.Nil(t, err)

	err = client.SetActiveUser(
		ctx,
		&schema.SetActiveUserRequest{
			Active:   true,
			Username: userName,
		})
	assert.Nil(t, err)

	err = client.ChangePassword(
		ctx,
		[]byte(userName),
		[]byte(userPassword),
		[]byte(userNewPassword),
	)
	assert.Nil(t, err)

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

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

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

	err1 := client.CreateDatabase(ctx, &schema.Database{Databasename: "test"})
	assert.Nil(t, err1)

	resp2, err2 := client.DatabaseList(ctx)

	assert.Nil(t, err2)
	assert.IsType(t, &schema.DatabaseListResponse{}, resp2)
	assert.Len(t, resp2.Databases, 2)
	client.Disconnect()
}

/*
func TestImmuClient_Inclusion(t *testing.T) {
	setup()
	index, _ := client.SafeSet(context.TODO(), []byte(`key`), []byte(`val`))

	proof, err := client.Inclusion(context.TODO(), index.Index)

	assert.IsType(t, &schema.InclusionProof{}, proof)
	assert.Nil(t, err)
	client.Disconnect()
}

func TestImmuClient_Consistency(t *testing.T) {
	setup()
	index, _ := client.SafeSet(context.TODO(), []byte(`key`), []byte(`val`))

	proof, err := client.Consistency(context.TODO(), index.Index)

	assert.IsType(t, &schema.ConsistencyProof{}, proof)
	assert.Nil(t, err)
	client.Disconnect()
}
*/
func TestImmuClient_History(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

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
	_, _ = client.VerifiedSet(ctx, []byte(`key1`), []byte(`val2`))

	sil, err := client.History(ctx, &schema.HistoryRequest{
		Key: []byte(`key1`),
	})

	assert.Nil(t, err)
	assert.Len(t, sil.Entries, 2)
	client.Disconnect()
}

/*
func TestImmuClient_SetAll(t *testing.T) {
	setup()

	_, err := client.SetAll(context.TODO(), nil)
	require.Error(t, ErrIllegalArguments, err)

	kvList := &schema.KVList{KVs: []*schema.KeyValue{}}
	_, err = client.SetAll(context.TODO(), kvList)
	require.Error(t, schema.ErrEmptySet, err)

	kvList = &schema.KVList{KVs: []*schema.KeyValue{
		{Key: []byte("1,2,3"), Value: []byte("3,2,1")},
		{Key: []byte("4,5,6"), Value: []byte("6,5,4")},
	}}

	_, err = client.SetAll(context.TODO(), kvList)
	require.NoError(t, err)

	for _, kv := range kvList.KVs {
		i, err := client.Get(context.TODO(), kv.Key)
		require.NoError(t, err)
		require.Equal(t, kv.Value, i.Value.GetPayload())
	}

	err = client.Disconnect()
	require.NoError(t, err)

	_, err = client.SetAll(context.TODO(), kvList)
	require.Error(t, ErrNotConnected, err)
}

func TestImmuClient_GetBatch(t *testing.T) {
	setup()
	_, _ = client.SafeSet(context.TODO(), []byte(`aaa`), []byte(`val`))

	sil, err := client.GetBatch(context.TODO(), [][]byte{[]byte(`aaa`), []byte(`bbb`)})

	assert.IsType(t, &schema.StructuredItemList{}, sil)
	assert.Nil(t, err)
	client.Disconnect()
}

func TestImmuClient_SetBatch(t *testing.T) {
	setup()
	br := BatchRequest{
		Keys:   []io.Reader{strings.NewReader("key1"), strings.NewReader("key2"), strings.NewReader("key3")},
		Values: []io.Reader{strings.NewReader("val1"), strings.NewReader("val2"), strings.NewReader("val3")},
	}
	ris, err := client.SetBatch(context.TODO(), &br)
	assert.Equal(t, uint64(2), ris.Index)
	assert.Nil(t, err)
	client.Disconnect()
}

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
			assert.NoError(t, err)
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
	assert.Equal(t, 2, s[0])
	assert.Equal(t, 5, s[1])
	assert.Equal(t, 8, s[2])
	assert.Equal(t, 11, s[3])
	assert.Equal(t, 14, s[4])
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
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	var wg1 sync.WaitGroup
	var sils = make(chan *schema.StructuredItemList, 2)
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		sil, err := client.GetBatch(context.TODO(), [][]byte{[]byte(`key1`), []byte(`key2`)})
		assert.NoError(t, err)
		sils <- sil
	}()
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		sil, err := client.GetBatch(context.TODO(), [][]byte{[]byte(`key3`)})
		assert.NoError(t, err)
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
	assert.Equal(t, []byte(`val1`), values[0])
	assert.Equal(t, []byte(`val2`), values[1])
	assert.Equal(t, []byte(`val3`), values[2])
	client.Disconnect()

}

type BytesSlice [][]byte

func (p BytesSlice) Len() int           { return len(p) }
func (p BytesSlice) Less(i, j int) bool { return bytes.Compare(p[i], p[j]) == -1 }
func (p BytesSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func TestImmuClient_Count(t *testing.T) {
	setup()
	_, _ = client.SafeSet(context.TODO(), []byte(`key1`), []byte(`val1`))
	_, _ = client.SafeSet(context.TODO(), []byte(`key1`), []byte(`val2`))
	_, _ = client.SafeSet(context.TODO(), []byte(`key1`), []byte(`val3`))

	ic, err := client.Count(context.TODO(), []byte(`key1`))

	assert.Nil(t, err)
	assert.IsType(t, &schema.ItemsCount{}, ic)
	assert.True(t, ic.Count == 3)
	client.Disconnect()
}

func TestImmuClient_CountAll(t *testing.T) {
	setup()
	_, _ = client.SafeSet(context.TODO(), []byte(`key1`), []byte(`val11`))
	_, _ = client.SafeSet(context.TODO(), []byte(`key2`), []byte(`val21`))
	_, _ = client.SafeSet(context.TODO(), []byte(`key2`), []byte(`val22`))
	_, _ = client.SafeSet(context.TODO(), []byte(`key3`), []byte(`val31`))

	ic, err := client.CountAll(context.TODO())

	assert.Nil(t, err)
	assert.IsType(t, &schema.ItemsCount{}, ic)
	// for each key there's an extra entry in the db:
	// 4 entries (2 of them have the same key) + 3 extra = 7 entries in total
	assert.Equal(t, 7, int(ic.Count))
	client.Disconnect()
}

func TestImmuClient_Scan(t *testing.T) {
	setup()
	_, _ = client.SafeSet(context.TODO(), []byte(`key1`), []byte(`val1`))
	_, _ = client.SafeSet(context.TODO(), []byte(`key1`), []byte(`val11`))
	_, _ = client.SafeSet(context.TODO(), []byte(`key3`), []byte(`val3`))

	sil, err := client.Scan(context.TODO(), &schema.ScanOptions{Prefix: []byte("key")})

	assert.IsType(t, &schema.StructuredItemList{}, sil)
	assert.Nil(t, err)
	assert.Len(t, sil.Items, 2)
	client.Disconnect()
}

func TestImmuClient_IScan(t *testing.T) {
	setup()
	_, _ = client.SafeSet(context.TODO(), []byte(`key1`), []byte(`val1`))
	_, _ = client.SafeSet(context.TODO(), []byte(`key1`), []byte(`val11`))
	_, _ = client.SafeSet(context.TODO(), []byte(`key3`), []byte(`val3`))

	p, err := client.IScan(context.TODO(), 1, 2)

	assert.IsType(t, &schema.SPage{}, p)
	assert.Nil(t, err)
	assert.Len(t, p.Items, 2)
	client.Disconnect()
}

func TestImmuClient_CurrentRoot(t *testing.T) {
	setup()
	_, _ = client.SafeSet(context.TODO(), []byte(`key1`), []byte(`val1`))

	r, err := client.CurrentRoot(context.TODO())

	assert.IsType(t, &schema.Root{}, r)
	assert.Nil(t, err)
	client.Disconnect()
}

func TestImmuClient_Logout(t *testing.T) {
	setup()
	// @todo need to be fixed
	_ = client.Logout(context.TODO())
	//assert.Nil(t, err)
	client.Disconnect()
}

func TestImmuClient_PrintTree(t *testing.T) {
	setup()
	_, _ = client.SafeSet(context.TODO(), []byte(`key1`), []byte(`val1`))

	tree, err := client.PrintTree(context.TODO())

	assert.Nil(t, err)
	assert.IsType(t, &schema.Tree{}, tree)

	client.Disconnect()
}

func TestImmuClient_GetServiceClient(t *testing.T) {
	setup()
	cli := client.GetServiceClient()
	assert.Implements(t, (*schema.ImmuServiceClient)(nil), *cli)
	client.Disconnect()
}

func TestImmuClient_GetReference(t *testing.T) {
	setup()
	idx, err := client.Set(context.TODO(), []byte(`key`), []byte(`value`))
	require.NoError(t, err)
	_, err = client.Reference(context.TODO(), []byte(`reference`), []byte(`key`), idx)
	require.NoError(t, err)
	op, err := client.GetReference(context.TODO(), &schema.Key{Key: []byte(`reference`)})
	assert.IsType(t, &schema.StructuredItem{}, op)
	require.NoError(t, err)
	client.Disconnect()
}

func TestImmuClient_GetOptions(t *testing.T) {
	setup()
	op := client.GetOptions()
	assert.IsType(t, &Options{}, op)
	client.Disconnect()
}

func TestImmuClient_ExecAllOpsOptions(t *testing.T) {
	setup()

	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
		},
	}

	idx, err := client.ExecAllOps(context.TODO(), aOps)

	assert.Nil(t, err)
	assert.NotNil(t, idx)

	client.Disconnect()
}

func TestEnforcedLogoutAfterPasswordChange(t *testing.T) {
	setup()
	var (
		userName        = "test"
		userPassword    = "1Password!*"
		userNewPassword = "2Password!*"
		testDBName      = "test"
		testDB          = &schema.Database{Databasename: testDBName}
		err             error
		testUserClient  ImmuClient
		testUserContext = context.TODO()
	)
	// step 1: create test database
	err = client.CreateDatabase(context.TODO(), testDB)
	assert.Nil(t, err)

	// step 2: create test user with read write permissions to the test db
	err = client.CreateUser(
		context.TODO(),
		[]byte(userName),
		[]byte(userPassword),
		auth.PermissionRW,
		testDBName,
	)
	assert.Nil(t, err)

	// setp 3: create test client and context
	nm, _ := NewNtpMock()
	tss := NewTimestampService(nm)
	testUserClient = newClient(false, "").WithTimestampService(tss)

	// step 4: test user login using the test context and test client
	resp, err := testUserClient.Login(
		testUserContext,
		[]byte(userName),
		[]byte(userPassword),
	)
	assert.Nil(t, err)
	testUserClient = newClient(true, resp.Token).WithTimestampService(tss)
	dbResp, err := testUserClient.UseDatabase(testUserContext, testDB)
	assert.Nil(t, err)
	testUserClient = newClient(true, dbResp.Token).WithTimestampService(tss)

	// step 5: successfully access the test db using the test client
	_, err = testUserClient.Set(testUserContext, []byte("sampleKey"), []byte("sampleValue"))
	assert.Nil(t, err)

	// step 6: using admin client change the test user password
	err = client.ChangePassword(
		context.TODO(),
		[]byte(userName),
		[]byte(userPassword),
		[]byte(userNewPassword),
	)
	assert.Nil(t, err)

	// step 7: access the test db again using the test client which should give an error
	_, err = testUserClient.Set(testUserContext, []byte("sampleKey"), []byte("sampleValue"))
	assert.NotNil(t, err)

	// step 8: repeat 4 and 5
	resp, err = testUserClient.Login(
		testUserContext,
		[]byte(userName),
		[]byte(userNewPassword),
	)
	assert.Nil(t, err)
	testUserClient = newClient(true, resp.Token).WithTimestampService(tss)
	dbResp, err = testUserClient.UseDatabase(testUserContext, testDB)
	assert.Nil(t, err)
	testUserClient = newClient(true, dbResp.Token).WithTimestampService(tss)

	_, err = testUserClient.Set(testUserContext, []byte("sampleKey"), []byte("sampleValue"))
	assert.Nil(t, err)

	client.Disconnect()
	testUserClient.Disconnect()
}
*/

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
