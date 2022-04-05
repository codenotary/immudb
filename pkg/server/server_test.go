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
package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/fs"
	"github.com/codenotary/immudb/pkg/stream"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var testDatabase = "lisbon"
var testUsername = []byte("Sagrada")
var testPassword = []byte("Familia@2")
var testKey = []byte("Antoni")
var testValue = []byte("Gaudí")

var kvs = []*schema.KeyValue{
	{
		Key:   []byte("Alberto"),
		Value: []byte("Tomba"),
	},
	{
		Key:   []byte("Jean-Claude"),
		Value: []byte("Killy"),
	},
	{
		Key:   []byte("Franz"),
		Value: []byte("Clamer"),
	},
}

func testServer(opts *Options) (*ImmuServer, func()) {
	s := DefaultServer().WithOptions(opts).(*ImmuServer)
	return s, func() {
		if s.Listener != nil {
			s.Listener.Close()
		}
		os.RemoveAll(s.Options.Dir)
	}
}

func TestLogErr(t *testing.T) {
	logger := logger.NewSimpleLogger("immudb ", os.Stderr)

	require.Nil(t, logErr(logger, "error: %v", nil))

	err := fmt.Errorf("expected error")
	require.Error(t, logErr(logger, "error: %v", err))
}

func TestServerDefaultDatabaseLoad(t *testing.T) {
	options := database.DefaultOption()
	dbRootpath := options.GetDBRootPath()

	s, closer := testServer(DefaultOptions())
	defer closer()

	err := s.loadDefaultDatabase(dbRootpath, nil)
	require.NoError(t, err)
	require.DirExists(t, path.Join(options.GetDBRootPath(), DefaultDBName))
}

func TestServerReOpen(t *testing.T) {
	serverOptions := DefaultOptions().WithDir("reopen")
	options := database.DefaultOption().WithDBRootPath(serverOptions.Dir)
	dbRootpath := options.GetDBRootPath()

	s, closer := testServer(serverOptions)
	defer closer()

	err := s.loadSystemDatabase(dbRootpath, nil, s.Options.AdminPassword)
	require.NoError(t, err)

	err = s.loadDefaultDatabase(dbRootpath, nil)
	require.NoError(t, err)

	err = s.CloseDatabases()
	require.NoError(t, err)

	s, closer = testServer(serverOptions)
	defer closer()

	err = s.loadSystemDatabase(dbRootpath, nil, s.Options.AdminPassword)
	require.NoError(t, err)

	err = s.loadDefaultDatabase(dbRootpath, nil)
	require.NoError(t, err)

	require.DirExists(t, path.Join(options.GetDBRootPath(), DefaultOptions().GetSystemAdminDBName()))
}

func TestServerSystemDatabaseLoad(t *testing.T) {
	serverOptions := DefaultOptions().WithDir("Nice")
	options := database.DefaultOption().WithDBRootPath(serverOptions.Dir)
	dbRootpath := options.GetDBRootPath()

	s, closer := testServer(serverOptions)
	defer closer()

	err := s.loadSystemDatabase(dbRootpath, nil, s.Options.AdminPassword)
	require.NoError(t, err)

	err = s.loadDefaultDatabase(dbRootpath, nil)
	require.NoError(t, err)

	require.DirExists(t, path.Join(options.GetDBRootPath(), DefaultOptions().GetSystemAdminDBName()))
}

func TestServerWithEmptyAdminPassword(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword("")
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	assert.ErrorIs(t, err, ErrEmptyAdminPassword)
}

func TestServerWithInvalidAdminPassword(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword("enc:*")
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	assert.Error(t, err)
}

func TestServerErrChunkSizeTooSmall(t *testing.T) {
	serverOptions := DefaultOptions().WithStreamChunkSize(4095)
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	assert.Equal(t, stream.ErrChunkTooSmall, err.Error())
}

func TestServerCreateDatabase(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	_, err = s.CreateDatabase(ctx, nil)
	require.Equal(t, ErrIllegalArguments, err)

	dbSettings := &schema.DatabaseSettings{
		DatabaseName:   "lisbon",
		Replica:        false,
		MasterDatabase: "masterdb",
	}
	_, err = s.CreateDatabaseWith(ctx, dbSettings)
	require.ErrorIs(t, err, ErrIllegalArguments)

	newdb := &schema.Database{
		DatabaseName: "lisbon",
	}
	_, err = s.CreateDatabase(ctx, newdb)
	require.NoError(t, err)
}

func TestServerCreateDatabaseCaseError(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	newdb := &schema.DatabaseSettings{
		DatabaseName: "MyDatabase",
	}
	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	_, err = s.CreateDatabaseWith(ctx, newdb)
	assert.Equal(t, err.Error(), "provide a lowercase database name")
}

func TestServerCreateMultipleDatabases(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	for i := 0; i < 64; i++ {
		dbname := fmt.Sprintf("db%d", i)

		db := &schema.DatabaseSettings{
			DatabaseName: dbname,
		}
		_, err = s.CreateDatabaseWith(ctx, db)
		require.NoError(t, err)

		uR, err := s.UseDatabase(ctx, &schema.Database{DatabaseName: dbname})
		require.NoError(t, err)

		md := metadata.Pairs("authorization", uR.Token)
		ctx := metadata.NewIncomingContext(context.Background(), md)

		_, err = s.Set(ctx, &schema.SetRequest{
			KVs: []*schema.KeyValue{
				{
					Key:   testKey,
					Value: testValue,
				},
			},
		})
		require.NoError(t, err)
	}

	err = s.CloseDatabases()
	require.NoError(t, err)
}

func TestServerUpdateDatabase(t *testing.T) {
	ctx := context.Background()

	serverOptions := DefaultOptions().
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithAuth(false)

	t.Run("auth disabled", func(t *testing.T) {

		s, closer := testServer(serverOptions)
		defer closer()

		err := s.Initialize()
		require.NoError(t, err)

		_, err = s.UpdateDatabase(ctx, &schema.DatabaseSettings{})
		require.Equal(t, ErrAuthMustBeEnabled, err)
	})

	t.Run("auth enabled", func(t *testing.T) {

		s, closer := testServer(serverOptions.WithAuth(true))
		defer closer()

		err := s.Initialize()
		require.NoError(t, err)

		r := &schema.LoginRequest{
			User:     []byte(auth.SysAdminUsername),
			Password: []byte(auth.SysAdminPassword),
		}

		lr, err := s.Login(ctx, r)
		require.NoError(t, err)

		md := metadata.Pairs("authorization", lr.Token)
		ctx = metadata.NewIncomingContext(context.Background(), md)

		_, err = s.UpdateDatabase(ctx, nil)
		require.Equal(t, ErrIllegalArguments, err)

		dbSettings := &schema.DatabaseSettings{
			DatabaseName: serverOptions.defaultDBName,
		}
		_, err = s.UpdateDatabase(ctx, dbSettings)
		require.Equal(t, ErrReservedDatabase, err)

		dbSettings = &schema.DatabaseSettings{
			DatabaseName: fmt.Sprintf("nodb%v", time.Now()),
		}
		_, err = s.UpdateDatabase(ctx, dbSettings)
		require.Equal(t, database.ErrDatabaseNotExists, err)

		newdb := &schema.DatabaseSettings{
			DatabaseName:   "lisbon",
			Replica:        true,
			MasterDatabase: "defaultdb",
		}
		_, err = s.CreateDatabaseWith(ctx, newdb)
		require.NoError(t, err)

		newdb.Replica = false
		newdb.MasterDatabase = ""
		_, err = s.UpdateDatabase(ctx, newdb)
		require.NoError(t, err)

		dbOpts, err := s.loadDBOptions("lisbon", false)
		require.NoError(t, err)
		require.Equal(t, false, dbOpts.Replica)
	})
}

func TestServerUpdateDatabaseV2(t *testing.T) {
	ctx := context.Background()

	serverOptions := DefaultOptions().
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithAuth(false)

	t.Run("auth disabled", func(t *testing.T) {

		s, closer := testServer(serverOptions)
		defer closer()

		err := s.Initialize()
		require.NoError(t, err)

		_, err = s.UpdateDatabaseV2(ctx, &schema.UpdateDatabaseRequest{})
		require.ErrorIs(t, err, ErrAuthMustBeEnabled)
	})

	t.Run("auth enabled", func(t *testing.T) {

		s, closer := testServer(serverOptions.WithAuth(true))
		defer closer()

		err := s.Initialize()
		require.NoError(t, err)

		r := &schema.LoginRequest{
			User:     []byte(auth.SysAdminUsername),
			Password: []byte(auth.SysAdminPassword),
		}

		lr, err := s.Login(ctx, r)
		require.NoError(t, err)

		md := metadata.Pairs("authorization", lr.Token)
		ctx = metadata.NewIncomingContext(context.Background(), md)

		_, err = s.UpdateDatabaseV2(ctx, nil)
		require.Equal(t, ErrIllegalArguments, err)

		dbSettings := &schema.UpdateDatabaseRequest{
			Database: serverOptions.defaultDBName,
		}
		_, err = s.UpdateDatabaseV2(ctx, dbSettings)
		require.Equal(t, ErrReservedDatabase, err)

		dbSettings = &schema.UpdateDatabaseRequest{
			Database: fmt.Sprintf("nodb%v", time.Now()),
		}
		_, err = s.UpdateDatabaseV2(ctx, dbSettings)
		require.Equal(t, database.ErrDatabaseNotExists, err)

		newdb := &schema.CreateDatabaseRequest{
			Name: "lisbon",
			Settings: &schema.DatabaseNullableSettings{
				ReplicationSettings: &schema.ReplicationNullableSettings{
					Replica:        &schema.NullableBool{Value: true},
					MasterDatabase: &schema.NullableString{Value: "defaultdb"},
				},
			},
		}
		_, err = s.CreateDatabaseV2(ctx, newdb)
		require.NoError(t, err)

		dbSettings = &schema.UpdateDatabaseRequest{
			Database: "lisbon",
			Settings: &schema.DatabaseNullableSettings{
				ReplicationSettings: &schema.ReplicationNullableSettings{
					Replica:        &schema.NullableBool{Value: false},
					MasterDatabase: &schema.NullableString{Value: ""},
				},
			},
		}
		_, err = s.UpdateDatabaseV2(ctx, dbSettings)
		require.NoError(t, err)

		dbOpts, err := s.loadDBOptions("lisbon", false)
		require.NoError(t, err)
		require.Equal(t, false, dbOpts.Replica)

		dbSettings = &schema.UpdateDatabaseRequest{
			Database: "lisbon",
			Settings: &schema.DatabaseNullableSettings{
				WriteTxHeaderVersion: &schema.NullableUint32{Value: 99999},
			},
		}
		_, err = s.UpdateDatabaseV2(ctx, dbSettings)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})
}

func TestServerLoaduserDatabase(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	newdb := &schema.DatabaseSettings{
		DatabaseName: testDatabase,
	}
	_, err = s.CreateDatabaseWith(ctx, newdb)
	require.NoError(t, err)

	err = s.CloseDatabases()
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	if s.dbList.Length() != 2 {
		t.Fatalf("LoadUserDatabase error %d", s.dbList.Length())
	}
}

func TestServerLoadUserDatabases(t *testing.T) {
	copier := fs.NewStandardCopier()
	require.NoError(t, copier.CopyDir("../../test/data_v1.1.0", "data_v1.1.0"))

	serverOptions := DefaultOptions().WithMetricsServer(false).WithDir("./data_v1.1.0")
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	err = s.loadUserDatabases(s.Options.Dir, nil)
	require.NoError(t, err)
}

func testServerSetGet(ctx context.Context, s *ImmuServer, t *testing.T) {
	txhdr, err := s.Set(ctx, &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   testKey,
				Value: testValue,
			},
		},
	})
	require.NoError(t, err)

	time.Sleep(1 * time.Millisecond)

	it, err := s.Get(ctx, &schema.KeyRequest{
		Key:     testKey,
		SinceTx: txhdr.Id,
	})
	require.NoError(t, err)
	if it.Tx != txhdr.Id {
		t.Fatalf("set.get tx missmatch expected %v got %v", txhdr.Id, it.Tx)
	}
	if !bytes.Equal(it.Key, testKey) {
		t.Fatalf("get key missmatch expected %v got %v", string(testKey), string(it.Key))
	}
	if !bytes.Equal(it.Value, testValue) {
		t.Fatalf("get key missmatch expected %v got %v", string(testValue), string(it.Value))
	}
}

func testServerSetGetError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Set(context.Background(), &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   testKey,
				Value: testValue,
			},
		},
	})
	require.Error(t, err)

	_, err = s.Get(context.Background(), &schema.KeyRequest{
		Key: testKey,
	})
	require.Error(t, err)
}

func testServerSafeSetGet(ctx context.Context, s *ImmuServer, t *testing.T) {
	state, err := s.CurrentState(ctx, &emptypb.Empty{})
	require.NoError(t, err)

	kv := []*schema.VerifiableSetRequest{
		{
			SetRequest: &schema.SetRequest{
				KVs: []*schema.KeyValue{
					{
						Key:   []byte("Alberto"),
						Value: []byte("Tomba"),
					},
				},
			},
			ProveSinceTx: state.TxId,
		},
		{
			SetRequest: &schema.SetRequest{
				KVs: []*schema.KeyValue{
					{
						Key:   []byte("Jean-Claude"),
						Value: []byte("Killy"),
					},
				},
			},
			ProveSinceTx: state.TxId,
		},
		{
			SetRequest: &schema.SetRequest{
				KVs: []*schema.KeyValue{
					{
						Key:   []byte("Franz"),
						Value: []byte("Clamer"),
					},
				},
			},
			ProveSinceTx: state.TxId,
		},
	}

	for _, val := range kv {
		vTx, err := s.VerifiableSet(ctx, val)
		require.NoError(t, err)

		if vTx == nil {
			t.Fatalf("Nil proof after SafeSet")
		}

		_, err = s.VerifiableGet(ctx, &schema.VerifiableGetRequest{
			KeyRequest: &schema.KeyRequest{
				Key:     val.SetRequest.KVs[0].Key,
				SinceTx: vTx.Tx.Header.Id,
			},
		})
		require.NoError(t, err)

		//if it.GetItem().GetIndex() != proof.Index {
		//	t.Fatalf("SafeGet index error, expected %d, got %d", proof.Index, it.GetItem().GetIndex())
		//}
	}
}

func testServerCurrentRoot(ctx context.Context, s *ImmuServer, t *testing.T) {
	for _, val := range kvs {
		_, err := s.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)

		_, err = s.CurrentState(ctx, &emptypb.Empty{})
		require.NoError(t, err)

	}
}

func testServerCurrentRootError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.CurrentState(context.Background(), &emptypb.Empty{})
	require.Error(t, err)
}

func testServerSetGetBatch(ctx context.Context, s *ImmuServer, t *testing.T) {
	kvs := []*schema.KeyValue{
		{
			Key:   []byte("Alberto"),
			Value: []byte("Tomba"),
		},
		{
			Key:   []byte("Jean-Claude"),
			Value: []byte("Killy"),
		},
		{
			Key:   []byte("Franz"),
			Value: []byte("Clamer"),
		},
	}

	ind, err := s.Set(ctx, &schema.SetRequest{KVs: kvs})
	require.NoError(t, err)

	if ind == nil {
		t.Fatalf("Nil index after Setbatch")
	}

	_, err = s.CompactIndex(ctx, nil)
	require.NoError(t, err)

	_, err = s.CompactIndex(ctx, &emptypb.Empty{})
	require.ErrorIs(t, err, tbtree.ErrCompactionThresholdNotReached)

	_, err = s.GetAll(ctx, nil)
	require.Equal(t, store.ErrIllegalArguments, err)

	itList, err := s.GetAll(ctx, &schema.KeyListRequest{
		Keys: [][]byte{
			[]byte("Alberto"),
			[]byte("Jean-Claude"),
			[]byte("Franz"),
		},
	})
	require.NoError(t, err)

	for ind, val := range itList.Entries {
		if !bytes.Equal(val.Value, kvs[ind].Value) {
			t.Fatalf("BatchSet value not equal to BatchGet value, expected %s, got %s", string(kvs[ind].Value), string(val.Value))
		}
	}
}

func testServerSetGetBatchError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.ExecAll(context.Background(), &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte("Alberto"),
						Value: []byte("Tomba"),
					},
				},
			},
		},
	})
	require.Error(t, err)

	_, err = s.GetAll(context.Background(), &schema.KeyListRequest{
		Keys: [][]byte{
			[]byte("Alberto"),
		},
	})
	require.Error(t, err)
}

func testServerByIndex(ctx context.Context, s *ImmuServer, t *testing.T) {
	ind := uint64(1)

	for _, val := range kvs {
		txhdr, err := s.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{val}})
		require.NoError(t, err)

		ind = txhdr.Id
	}

	s.VerifiableSet(ctx, &schema.VerifiableSetRequest{
		SetRequest: &schema.SetRequest{
			KVs: []*schema.KeyValue{
				{
					Key:   testKey,
					Value: testValue,
				},
			},
		},
	})

	_, err := s.TxById(ctx, &schema.TxRequest{Tx: ind})
	require.NoError(t, err)

	//if !bytes.Equal(inc.Value, kvs[len(kv)-1].Value) {
	//	t.Fatalf("ByIndex, expected %s, got %d", kvs[ind].Value, inc.Value)
	//}
}

func testServerByIndexError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.TxById(context.Background(), &schema.TxRequest{Tx: 0})
	require.Error(t, err)
}

func testServerBySafeIndex(ctx context.Context, s *ImmuServer, t *testing.T) {
	for _, val := range kvs {
		_, err := s.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{val}})
		require.NoError(t, err)

	}
	s.VerifiableSet(ctx, &schema.VerifiableSetRequest{
		SetRequest: &schema.SetRequest{KVs: []*schema.KeyValue{{
			Key:   testKey,
			Value: testValue,
		}}},
	})

	ind := uint64(1)
	_, err := s.VerifiableTxById(ctx, &schema.VerifiableTxRequest{Tx: ind})
	require.NoError(t, err)

	//if inc.Item.Index != ind {
	//	t.Fatalf("ByIndexSV, expected %d, got %d", ind, inc.Item.Index)
	//}
}

func testServerBySafeIndexError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.VerifiableTxById(context.Background(), &schema.VerifiableTxRequest{Tx: 0})
	require.Error(t, err)

}

func testServerHistory(ctx context.Context, s *ImmuServer, t *testing.T) {
	inc, err := s.History(ctx, &schema.HistoryRequest{
		Key: testKey,
	})
	require.NoError(t, err)

	for _, val := range inc.Entries {
		if !bytes.Equal(val.Value, testValue) {
			t.Fatalf("History, expected %s, got %s", val.Value, testValue)
		}
	}
}

func testServerHistoryError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.History(context.Background(), &schema.HistoryRequest{
		Key: testKey,
	})
	require.Error(t, err)

}

func testServerHealth(ctx context.Context, s *ImmuServer, t *testing.T) {
	h, err := s.Health(ctx, &emptypb.Empty{})
	require.NoError(t, err)

	if !h.GetStatus() {
		t.Fatalf("Health, expected %v, got %v", true, h.GetStatus())
	}
}

func testServerHealthError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Health(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)

}

func testServerReference(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{kvs[0]}})
	require.NoError(t, err)

	meta, err := s.SetReference(ctx, &schema.ReferenceRequest{
		Key:           []byte(`tag`),
		ReferencedKey: kvs[0].Key,
	})
	require.NoError(t, err)

	item, err := s.Get(ctx, &schema.KeyRequest{Key: []byte(`tag`), SinceTx: meta.Id})
	require.NoError(t, err)
	require.Equal(t, kvs[0].Value, item.Value)
}

func testServerGetReference(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{kvs[0]}})
	require.NoError(t, err)

	_, err = s.SetReference(ctx, &schema.ReferenceRequest{
		Key:           []byte(`tag`),
		ReferencedKey: kvs[0].Key,
	})
	require.NoError(t, err)

	item, err := s.Get(ctx, &schema.KeyRequest{
		Key: []byte(`tag`),
	})
	require.NoError(t, err)

	if !bytes.Equal(item.Value, kvs[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(item.Value), string(kvs[0].Value))
	}
}

func testServerReferenceError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.SetReference(ctx, &schema.ReferenceRequest{
		Key:           []byte(`tag`),
		ReferencedKey: kvs[0].Key,
	})
	require.NoError(t, err)

}

func testServerZAdd(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{kvs[0]}})
	require.NoError(t, err)

	meta, err := s.ZAdd(ctx, &schema.ZAddRequest{
		Key:   kvs[0].Key,
		Score: 1,
		Set:   kvs[0].Value,
	})
	require.NoError(t, err)

	item, err := s.ZScan(ctx, &schema.ZScanRequest{
		Set:     kvs[0].Value,
		SeekKey: []byte(""),
		Limit:   3,
		Desc:    false,
		SinceTx: meta.Id,
	})
	require.NoError(t, err)
	if !bytes.Equal(item.Entries[0].Entry.Value, kvs[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(kvs[0].Value), string(item.Entries[0].Entry.Value))
	}
}

func testServerZAddError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.ZAdd(context.Background(), &schema.ZAddRequest{
		Key:   kvs[0].Key,
		Score: 1,
		Set:   kvs[0].Value,
	})
	require.Error(t, err)

	_, err = s.ZScan(context.Background(), &schema.ZScanRequest{
		Set:     kvs[0].Value,
		SeekKey: []byte(""),
		Limit:   3,
		Desc:    false,
	})
	require.Error(t, err)

}

func testServerScan(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{kvs[0]}})
	require.NoError(t, err)

	_, err = s.ZAdd(ctx, &schema.ZAddRequest{
		Key:   kvs[0].Key,
		Score: 3,
		Set:   kvs[0].Value,
	})
	require.NoError(t, err)

	meta, err := s.VerifiableZAdd(ctx, &schema.VerifiableZAddRequest{
		ZAddRequest: &schema.ZAddRequest{
			Key:   kvs[0].Key,
			Score: 0,
			Set:   kvs[0].Value,
		},
		ProveSinceTx: 0,
	})
	require.NoError(t, err)

	item, err := s.Scan(ctx, &schema.ScanRequest{
		SeekKey: nil,
		Limit:   1,
		Prefix:  kvs[0].Key,
		SinceTx: meta.Tx.Header.Id,
	})
	require.NoError(t, err)

	if !bytes.Equal(item.Entries[0].Key, kvs[0].Key) {
		t.Fatalf("Reference, expected %v, got %v", string(kvs[0].Key), string(item.Entries[0].Key))
	}
}

func testServerScanError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Scan(context.Background(), &schema.ScanRequest{
		SeekKey: nil,
		Limit:   1,
		Prefix:  kvs[0].Key,
	})
	require.Error(t, err)

}

func testServerTxScan(ctx context.Context, s *ImmuServer, t *testing.T) {
	txmd, err := s.Set(ctx, &schema.SetRequest{KVs: []*schema.KeyValue{kvs[0]}})
	require.NoError(t, err)

	_, err = s.ZAdd(ctx, &schema.ZAddRequest{
		Key:   kvs[0].Key,
		Score: 3,
		Set:   kvs[0].Value,
	})
	require.NoError(t, err)

	_, err = s.VerifiableZAdd(ctx, &schema.VerifiableZAddRequest{
		ZAddRequest: &schema.ZAddRequest{
			Key:   kvs[0].Key,
			Score: 0,
			Set:   kvs[0].Value,
		},
		ProveSinceTx: 0,
	})
	require.NoError(t, err)

	txls, err := s.TxScan(ctx, &schema.TxScanRequest{
		InitialTx: txmd.Id,
	})
	require.NoError(t, err)
	require.Len(t, txls.Txs, 3)
	require.Equal(t, database.TrimPrefix(txls.Txs[0].Entries[0].Key), kvs[0].Key)
}

func testServerSafeReference(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.VerifiableSet(ctx, &schema.VerifiableSetRequest{
		SetRequest: &schema.SetRequest{
			KVs: []*schema.KeyValue{kvs[0]},
		},
	})
	require.NoError(t, err)

	vtx, err := s.VerifiableSetReference(ctx, &schema.VerifiableReferenceRequest{
		ReferenceRequest: &schema.ReferenceRequest{
			Key:           []byte("refKey1"),
			ReferencedKey: kvs[0].Key,
		},
		ProveSinceTx: 1,
	})
	require.NoError(t, err)

	ref, err := s.Get(ctx, &schema.KeyRequest{
		Key:     []byte("refKey1"),
		SinceTx: vtx.Tx.Header.Id,
	})
	require.NoError(t, err)
	require.Equal(t, kvs[0].Value, ref.Value)
}

func testServerSafeReferenceError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.VerifiableSetReference(context.Background(), &schema.VerifiableReferenceRequest{
		ReferenceRequest: &schema.ReferenceRequest{
			Key:           []byte("refKey1"),
			ReferencedKey: kvs[0].Key,
		},
		ProveSinceTx: 0,
	})
	require.Error(t, err)
}

func testServerCount(ctx context.Context, s *ImmuServer, t *testing.T) {
	// Count
	c, err := s.Count(ctx, &schema.KeyPrefix{
		Prefix: kvs[0].Key,
	})
	require.NoError(t, err)

	if c.Count == 0 {
		t.Fatalf("Count error >0 got %d", c.Count)
	}
	// CountAll
	countAll, err := s.CountAll(ctx, new(empty.Empty))
	require.NoError(t, err)

	if countAll.Count != 43 {
		t.Fatalf("CountAll error: expected %d, got %d", 43, countAll.Count)
	}
}

func testServerCountError(ctx context.Context, s *ImmuServer, t *testing.T) {
	_, err := s.Count(context.Background(), &schema.KeyPrefix{
		Prefix: kvs[0].Key,
	})
	require.NoError(t, err)

}

func TestServerDbOperations(t *testing.T) {
	serverOptions := DefaultOptions().
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithSigningKey("./../../test/signer/ec1.key")

	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	newdb := &schema.DatabaseSettings{
		DatabaseName: testDatabase,
		FileSize:     1 << 20,
	}
	_, err = s.CreateDatabaseWith(ctx, newdb)
	require.NoError(t, err)

	_, err = s.Count(ctx, nil)
	require.Equal(t, ErrNotSupported, err)

	_, err = s.CountAll(ctx, nil)
	require.Equal(t, ErrNotSupported, err)

	testServerSetGet(ctx, s, t)
	testServerSetGetError(ctx, s, t)
	testServerCurrentRoot(ctx, s, t)
	testServerCurrentRootError(ctx, s, t)
	testServerSafeSetGet(ctx, s, t)
	testServerSetGetBatch(ctx, s, t)
	testServerSetGetBatchError(ctx, s, t)
	testServerByIndex(ctx, s, t)
	testServerByIndexError(ctx, s, t)
	testServerHistory(ctx, s, t)
	testServerHistoryError(ctx, s, t)
	testServerBySafeIndex(ctx, s, t)
	testServerBySafeIndexError(ctx, s, t)
	testServerHealth(ctx, s, t)
	testServerHealthError(ctx, s, t)
	testServerReference(ctx, s, t)
	testServerReferenceError(ctx, s, t)
	testServerZAdd(ctx, s, t)
	testServerZAddError(ctx, s, t)
	testServerScan(ctx, s, t)
	testServerScanError(ctx, s, t)
	testServerTxScan(ctx, s, t)
	testServerSafeReference(ctx, s, t)
	testServerSafeReferenceError(ctx, s, t)
	//testServerCount(ctx, s, t)
	//testServerCountError(ctx, s, t)
}

func TestServerUpdateConfigItem(t *testing.T) {
	dataDir := "test-server-update-config-item-config"
	configFile := fmt.Sprintf("%s.toml", dataDir)
	s, closer := testServer(DefaultOptions().
		WithAuth(false).
		WithMaintenance(false).
		WithDir(dataDir))
	defer func() {
		closer()
		os.Remove(configFile)
	}()

	// Config file path empty
	s.Options.Config = ""
	err := s.updateConfigItem("key", "key = value", func(string) bool { return false })
	require.Error(t, err)
	require.Equal(t, errors.New("config file does not exist"), err)
	s.Options.Config = configFile

	// ReadFile error
	immuOS := s.OS.(*immuos.StandardOS)
	readFileOK := immuOS.ReadFileF
	errReadFile := "ReadFile error"
	immuOS.ReadFileF = func(filename string) ([]byte, error) {
		return nil, errors.New(errReadFile)
	}
	expectedErr :=
		fmt.Errorf("error reading config file '%s'. Reason: %s", configFile, errReadFile)
	err = s.updateConfigItem("key", "key = value", func(string) bool { return false })
	require.Equal(t, expectedErr, err)
	immuOS.ReadFileF = readFileOK

	// Config already having the specified item
	ioutil.WriteFile(configFile, []byte("key = value"), 0644)
	err = s.updateConfigItem("key", "key = value", func(string) bool { return true })
	require.Equal(t, errors.New("Server config already has 'key = value'"), err)

	// Add new config item
	err = s.updateConfigItem("key2", "key2 = value2", func(string) bool { return false })
	require.NoError(t, err)

	// WriteFile error
	errWriteFile := errors.New("WriteFile error")
	immuOS.WriteFileF = func(filename string, data []byte, perm os.FileMode) error {
		return errWriteFile
	}
	err = s.updateConfigItem("key3", "key3 = value3", func(string) bool { return false })
	require.Equal(t, err, errWriteFile)
}

func TestServerPID(t *testing.T) {
	op := DefaultOptions().
		WithAuth(false).
		WithMaintenance(false).WithPidfile("pidfile")
	s, closer := testServer(op)
	defer closer()
	defer os.Remove("pidfile")
	err := s.setupPidFile()
	require.NoError(t, err)
}

func TestServerErrors(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	adminCtx := context.Background()
	lr, err := s.Login(adminCtx, &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	})
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	adminCtx = metadata.NewIncomingContext(context.Background(), md)

	// insertNewUser errors
	_, _, err = s.insertNewUser([]byte("%"), nil, 1, DefaultDBName, true, auth.SysAdminUsername)
	require.Error(t, err)
	require.Contains(t, err.Error(), "username can only contain letters, digits and underscores")

	username := "someusername"
	usernameBytes := []byte(username)
	password := "$omePassword1"
	passwordBytes := []byte(password)
	_, _, err = s.insertNewUser(usernameBytes, []byte("a"), 1, DefaultDBName, true, auth.SysAdminUsername)
	require.Error(t, err)
	require.Contains(
		t,
		err.Error(),
		"password must have between 8 and 32 letters, digits and special characters "+
			"of which at least 1 uppercase letter, 1 digit and 1 special character")

	_, _, err = s.insertNewUser(usernameBytes, passwordBytes, 99, DefaultDBName, false, auth.SysAdminUsername)
	require.Equal(t, errors.New("unknown permission"), err)

	// getLoggedInUserDataFromUsername errors
	userdata := s.userdata.Userdata[username]
	delete(s.userdata.Userdata, username)
	_, err = s.getLoggedInUserDataFromUsername(username)
	require.Equal(t, ErrNotLoggedIn, err)
	s.userdata.Userdata[username] = userdata

	// getDBFromCtx errors
	adminUserdata := s.userdata.Userdata[auth.SysAdminUsername]
	delete(s.userdata.Userdata, auth.SysAdminUsername)
	s.Options.maintenance = true
	_, err = s.getDBFromCtx(adminCtx, "ListUsers")
	require.Equal(t, ErrNotLoggedIn, err)
	s.userdata.Userdata[auth.SysAdminUsername] = adminUserdata
	s.Options.maintenance = false

	// SetActiveUser errors
	_, err = s.SetActiveUser(adminCtx, &schema.SetActiveUserRequest{Username: "", Active: false})
	require.Equal(t, errors.New("username can not be empty"), err)

	s.Options.auth = false
	_, err = s.SetActiveUser(adminCtx, &schema.SetActiveUserRequest{Username: username, Active: false})
	require.Equal(t, errors.New("this command is available only with authentication on"), err)
	s.Options.auth = true

	delete(s.userdata.Userdata, auth.SysAdminUsername)
	_, err = s.SetActiveUser(adminCtx, &schema.SetActiveUserRequest{Username: username, Active: false})
	require.Equal(t, ErrNotLoggedIn, err)
	s.userdata.Userdata[auth.SysAdminUsername] = adminUserdata

	_, err = s.SetActiveUser(adminCtx, &schema.SetActiveUserRequest{Username: auth.SysAdminUsername, Active: false})
	require.Equal(t, errors.New("changing your own status is not allowed"), err)

	_, err = s.CreateUser(adminCtx, &schema.CreateUserRequest{
		User:       usernameBytes,
		Password:   passwordBytes,
		Permission: 1,
		Database:   DefaultDBName,
	})
	require.NoError(t, err)

	userCtx := context.Background()

	lr, err = s.Login(userCtx, &schema.LoginRequest{User: usernameBytes, Password: passwordBytes})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", lr.Token)
	userCtx = metadata.NewIncomingContext(context.Background(), md)

	_, err = s.SetActiveUser(userCtx, &schema.SetActiveUserRequest{Username: auth.SysAdminUsername, Active: false})
	require.Equal(t, errors.New("user is not system admin nor admin in any of the databases"), err)

	_, err = s.SetActiveUser(adminCtx, &schema.SetActiveUserRequest{Username: "nonexistentuser", Active: false})
	require.Equal(t, errors.New("user nonexistentuser not found"), err)

	// ChangePermission errors
	cpr := &schema.ChangePermissionRequest{
		Action:     schema.PermissionAction_GRANT,
		Username:   username,
		Database:   SystemDBName,
		Permission: 2,
	}
	_, err = s.ChangePermission(adminCtx, cpr)
	require.Equal(t, ErrPermissionDenied, err)

	_, err = s.Logout(userCtx, &emptypb.Empty{})
	require.NoError(t, err)

	cpr.Database = DefaultDBName
	s.Options.auth = false
	_, err = s.ChangePermission(userCtx, cpr)
	require.Equal(t, ErrNotLoggedIn.Message(), err.Error())
	s.Options.auth = true

	delete(s.userdata.Userdata, auth.SysAdminUsername)
	_, err = s.ChangePermission(userCtx, cpr)
	require.Equal(t, ErrNotLoggedIn, err)
	s.userdata.Userdata[auth.SysAdminUsername] = adminUserdata

	cpr.Username = ""
	_, err = s.ChangePermission(userCtx, cpr)
	require.Contains(t, err.Error(), "username can not be empty")

	cpr.Username = username
	cpr.Database = ""
	_, err = s.ChangePermission(userCtx, cpr)
	errStatus, _ := status.FromError(err)
	require.Equal(t, codes.InvalidArgument, errStatus.Code())
	require.Equal(t, "database can not be empty", errStatus.Message())

	cpr.Database = DefaultDBName
	cpr.Action = 99
	_, err = s.ChangePermission(userCtx, cpr)
	errStatus, _ = status.FromError(err)
	require.Equal(t, codes.InvalidArgument, errStatus.Code())
	require.Equal(t, "action not recognized", errStatus.Message())
	cpr.Action = schema.PermissionAction_GRANT

	cpr.Permission = 99
	_, err = s.ChangePermission(userCtx, cpr)
	errStatus, _ = status.FromError(err)
	require.Equal(t, codes.InvalidArgument, errStatus.Code())
	require.Equal(t, "unrecognized permission", errStatus.Message())

	cpr.Permission = auth.PermissionRW

	userCtx = context.Background()
	lr, err = s.Login(userCtx, &schema.LoginRequest{
		User:     []byte(username),
		Password: []byte(password),
	})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", lr.Token)
	userCtx = metadata.NewIncomingContext(context.Background(), md)

	cpr.Username = username
	_, err = s.ChangePermission(userCtx, cpr)
	errStatus, _ = status.FromError(err)
	require.Equal(t, "changing your own permissions is not allowed", errStatus.Message())

	cpr.Username = "nonexistentuser"
	_, err = s.ChangePermission(userCtx, cpr)
	errStatus, _ = status.FromError(err)
	require.Equal(t, codes.NotFound, errStatus.Code())
	require.Equal(t, fmt.Sprintf("user %s not found", cpr.Username), errStatus.Message())

	cpr.Username = auth.SysAdminUsername
	_, err = s.ChangePermission(userCtx, cpr)
	errStatus, _ = status.FromError(err)
	require.Equal(t, "changing sysadmin permisions is not allowed", errStatus.Message())

	cpr.Username = username

	cpr.Action = schema.PermissionAction_REVOKE
	_, err = s.ChangePermission(adminCtx, cpr)
	require.NoError(t, err)

	cpr.Action = schema.PermissionAction_GRANT

	_, err = s.SetActiveUser(adminCtx, &schema.SetActiveUserRequest{Active: false, Username: username})
	require.NoError(t, err)

	_, err = s.ChangePermission(adminCtx, cpr)
	errStatus, _ = status.FromError(err)
	require.Equal(t, codes.FailedPrecondition, errStatus.Code())
	require.Equal(t, fmt.Sprintf("user %s is not active", username), errStatus.Message())

	_, err = s.SetActiveUser(adminCtx, &schema.SetActiveUserRequest{Active: true, Username: username})
	require.NoError(t, err)

	// UseDatabase errors
	s.Options.auth = false
	_, err = s.UseDatabase(adminCtx, &schema.Database{DatabaseName: DefaultDBName})
	require.Equal(t, errors.New("this command is available only with authentication on"), err)
	s.Options.auth = true

	_, err = s.UseDatabase(userCtx, &schema.Database{DatabaseName: DefaultDBName})
	errStatus, _ = status.FromError(err)
	require.Equal(t, codes.Unauthenticated, errStatus.Code())
	require.Equal(t, "Please login", errStatus.Message())

	_, err = s.UseDatabase(adminCtx, &schema.Database{DatabaseName: SystemDBName})
	require.NoError(t, err)

	lr, err = s.Login(userCtx, &schema.LoginRequest{User: usernameBytes, Password: passwordBytes})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", lr.Token)
	userCtx = metadata.NewIncomingContext(context.Background(), md)

	_, err = s.UseDatabase(userCtx, &schema.Database{DatabaseName: SystemDBName})
	errStatus, _ = status.FromError(err)
	require.Equal(t, codes.PermissionDenied, errStatus.Code())

	someDb1 := "somedatabase1"
	_, err = s.CreateDatabaseWith(adminCtx, &schema.DatabaseSettings{DatabaseName: someDb1})
	require.NoError(t, err)

	_, err = s.UseDatabase(userCtx, &schema.Database{DatabaseName: someDb1})
	errStatus, _ = status.FromError(err)
	require.Equal(t, codes.PermissionDenied, errStatus.Code())

	s.Options.maintenance = true
	_, err = s.UseDatabase(userCtx, &schema.Database{DatabaseName: DefaultDBName})
	errStatus, _ = status.FromError(err)
	require.Equal(t, codes.PermissionDenied, errStatus.Code())

	s.Options.maintenance = false

	_, err = s.UseDatabase(userCtx, &schema.Database{DatabaseName: "nonexistentdb"})
	errStatus, _ = status.FromError(err)
	require.Equal(t, codes.NotFound, errStatus.Code())
	require.Equal(t, "'nonexistentdb' does not exist", errStatus.Message())

	// DatabaseList errors
	s.Options.auth = false
	_, err = s.DatabaseList(userCtx, new(emptypb.Empty))
	require.Equal(t, errors.New("this command is available only with authentication on"), err)
	s.Options.auth = true

	_, err = s.DatabaseList(context.Background(), new(emptypb.Empty))
	require.Equal(t, errors.New("please login"), err)

	cpr = &schema.ChangePermissionRequest{
		Action:     schema.PermissionAction_GRANT,
		Username:   username,
		Database:   DefaultDBName,
		Permission: 2,
	}
	_, err = s.ChangePermission(adminCtx, cpr)
	require.NoError(t, err)

	lr, err = s.Login(userCtx, &schema.LoginRequest{User: usernameBytes, Password: passwordBytes})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", lr.Token)
	userCtx = metadata.NewIncomingContext(context.Background(), md)

	require.NoError(t, err)
	_, err = s.DatabaseList(userCtx, new(emptypb.Empty))
	require.NoError(t, err)

	// ListUsers errors
	s.Options.auth = false
	_, err = s.ListUsers(userCtx, new(emptypb.Empty))
	require.Equal(t, errors.New("this command is available only with authentication on"), err)
	s.Options.auth = true

	_, err = s.ListUsers(context.Background(), new(emptypb.Empty))
	require.Equal(t, ErrNotLoggedIn.Message(), err.Error())

	// CreateUser errors
	username2 := "someusername2"
	username2Bytes := []byte(username2)
	password2 := "$omePassword2"
	password2Bytes := []byte(password2)
	createUser2Req := &schema.CreateUserRequest{
		User:       nil,
		Password:   password2Bytes,
		Permission: auth.PermissionRW,
		Database:   someDb1,
	}

	s.Options.auth = false
	_, err = s.CreateUser(adminCtx, createUser2Req)
	require.Equal(t, errors.New("this command is available only with authentication on"), err)
	s.Options.auth = true

	_, err = s.CreateUser(context.Background(), createUser2Req)
	require.Equal(t, ErrNotLoggedIn.Message(), err.Error())

	_, err = s.CreateUser(adminCtx, createUser2Req)
	require.Equal(t, errors.New("username can not be empty"), err)

	createUser2Req.User = username2Bytes
	createUser2Req.Database = ""
	_, err = s.CreateUser(adminCtx, createUser2Req)
	require.Equal(t, errors.New("database name can not be empty when there are multiple databases"), err)

	createUser2Req.Database = "nonexistentdb"
	_, err = s.CreateUser(adminCtx, createUser2Req)
	require.Equal(t, errors.New("database nonexistentdb does not exist"), err)

	createUser2Req.Database = someDb1
	createUser2Req.Permission = auth.PermissionNone
	_, err = s.CreateUser(adminCtx, createUser2Req)
	require.Equal(t, errors.New("unrecognized permission"), err)

	createUser2Req.Permission = auth.PermissionRW
	_, err = s.CreateUser(userCtx, createUser2Req)
	require.Equal(t, errors.New("you do not have permission on this database"), err)

	createUser2Req.Permission = auth.PermissionSysAdmin
	_, err = s.CreateUser(adminCtx, createUser2Req)
	require.Equal(t, errors.New("can not create another system admin"), err)

	createUser2Req.Permission = auth.PermissionRW
	createUser2Req.User = usernameBytes
	_, err = s.CreateUser(adminCtx, createUser2Req)
	require.Equal(t, errors.New("user already exists"), err)

	// CreateDatabase errors
	someDb2 := "somedatabase2"
	createDbReq := &schema.DatabaseSettings{DatabaseName: someDb2}
	s.Options.auth = false
	_, err = s.CreateDatabaseWith(adminCtx, createDbReq)
	require.Equal(t, errors.New("this command is available only with authentication on"), err)
	s.Options.auth = true

	_, err = s.CreateDatabaseWith(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = s.CreateDatabaseWith(context.Background(), createDbReq)
	require.Equal(t, errors.New("could not get loggedin user data"), err)

	_, err = s.CreateDatabaseWith(userCtx, createDbReq)
	require.Equal(t, errors.New("logged In user does not have permissions for this operation"), err)

	createDbReq.DatabaseName = SystemDBName
	_, err = s.CreateDatabaseWith(adminCtx, createDbReq)
	require.Equal(t, errors.New("this database name is reserved"), err)
	createDbReq.DatabaseName = someDb2

	createDbReq.DatabaseName = ""
	_, err = s.CreateDatabaseWith(adminCtx, createDbReq)
	require.Equal(t, errors.New("database name length outside of limits"), err)

	createDbReq.DatabaseName = someDb1
	_, err = s.CreateDatabaseWith(adminCtx, createDbReq)
	require.Equal(t, fmt.Errorf("database '%s' already exists", someDb1), err)

	// ChangePassword errors
	s.Options.auth = false
	changePassReq := &schema.ChangePasswordRequest{
		User:        usernameBytes,
		OldPassword: passwordBytes,
		NewPassword: password2Bytes,
	}
	_, err = s.ChangePassword(adminCtx, changePassReq)
	require.Equal(t, errors.New("this command is available only with authentication on"), err)
	s.Options.auth = true

	_, err = s.ChangePassword(context.Background(), changePassReq)
	require.Equal(t, ErrNotLoggedIn.Message(), err.Error())

	changePassReq.User = []byte(auth.SysAdminUsername)
	changePassReq.OldPassword = []byte("incorrect")
	_, err = s.ChangePassword(adminCtx, changePassReq)
	require.Error(t, err)
	require.Contains(t, err.Error(), "old password is incorrect")

	changePassReq.User = usernameBytes
	changePassReq.OldPassword = passwordBytes
	_, err = s.ChangePassword(userCtx, changePassReq)
	require.Equal(t, errors.New("user is not system admin nor admin in any of the databases"), err)

	changePassReq.User = nil
	_, err = s.ChangePassword(adminCtx, changePassReq)
	require.Equal(t, errors.New("username can not be empty"), err)

	changePassReq.User = []byte("nonexistent")
	_, err = s.ChangePassword(adminCtx, changePassReq)
	require.Equal(t, fmt.Errorf("user %s was not found or it was not created by you", changePassReq.User), err)

	_, err = s.ChangePermission(adminCtx, &schema.ChangePermissionRequest{
		Action:     schema.PermissionAction_GRANT,
		Username:   username,
		Database:   someDb1,
		Permission: auth.PermissionAdmin,
	})
	require.NoError(t, err)

	lr, err = s.Login(userCtx, &schema.LoginRequest{User: usernameBytes, Password: passwordBytes})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", lr.Token)
	userCtx = metadata.NewIncomingContext(context.Background(), md)

	require.NoError(t, err)
	createUser2Req = &schema.CreateUserRequest{
		User:       username2Bytes,
		Password:   password2Bytes,
		Permission: auth.PermissionAdmin,
		Database:   someDb1,
	}
	_, err = s.CreateUser(adminCtx, createUser2Req)
	require.NoError(t, err)

	changePassReq.User = username2Bytes
	changePassReq.OldPassword = password2Bytes
	password2New := []byte("$omePassword2New")
	password2NewBytes := []byte(password2New)
	changePassReq.NewPassword = password2NewBytes
	_, err = s.ChangePassword(userCtx, changePassReq)
	require.Equal(t, fmt.Errorf("user %s was not found or it was not created by you", changePassReq.User), err)

	// Not logged in errors on DB operations
	emptyCtx := context.Background()
	_, err = s.VerifiableZAdd(emptyCtx, &schema.VerifiableZAddRequest{})
	require.Equal(t, ErrNotLoggedIn.Message(), err.Error())
	_, err = s.SetReference(emptyCtx, &schema.ReferenceRequest{})
	require.Equal(t, ErrNotLoggedIn.Message(), err.Error())
	_, err = s.UpdateMTLSConfig(emptyCtx, &schema.MTLSConfig{})
	require.Equal(t, ErrNotSupported.Error(), err.Error())
	_, err = s.UpdateAuthConfig(emptyCtx, &schema.AuthConfig{})
	require.Equal(t, ErrNotSupported.Error(), err.Error())

	// Login errors
	s.Options.auth = false
	_, err = s.Login(emptyCtx, &schema.LoginRequest{})
	require.Equal(t, "server is running with authentication disabled, please enable authentication to login", err.Error())
	s.Options.auth = true

	_, err = s.Login(emptyCtx, &schema.LoginRequest{User: []byte("nonexistent")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid user name or password")

	_, err = s.SetActiveUser(adminCtx, &schema.SetActiveUserRequest{Active: false, Username: username})
	require.NoError(t, err)

	_, err = s.Login(emptyCtx, &schema.LoginRequest{User: usernameBytes, Password: passwordBytes})
	require.Equal(t, "user is not active", err.Error())

	_, err = s.SetActiveUser(adminCtx, &schema.SetActiveUserRequest{Active: true, Username: username})
	require.NoError(t, err)

	lr, err = s.Login(userCtx, &schema.LoginRequest{User: usernameBytes, Password: passwordBytes})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", lr.Token)
	userCtx = metadata.NewIncomingContext(context.Background(), md)

	// setup PID
	OS := s.OS.(*immuos.StandardOS)
	baseFOK := OS.BaseF
	OS.BaseF = func(path string) string {
		return "."
	}
	s.Options.Pidfile = "pidfile"
	defer os.Remove(s.Options.Pidfile)
	require.Equal(t, fmt.Errorf("Pid filename is invalid: %s", s.Options.Pidfile), s.setupPidFile())
	OS.BaseF = baseFOK

	// print usage call-to-action
	s.Options.Logfile = "TestUserAndDatabaseOperations.log"
	s.printUsageCallToAction()
}

func TestServerGetUserAndUserExists(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)
	require.NoError(t, err)
	username := "someuser"
	_, err = s.CreateUser(ctx, &schema.CreateUserRequest{
		User:       []byte(username),
		Password:   []byte("Somepass1$"),
		Permission: 1,
		Database:   DefaultDBName})
	require.NoError(t, err)
	require.NoError(t, err)

	_, err = s.getUser([]byte(username), true)
	require.NoError(t, err)

	_, err = s.getUser([]byte(username), false)
	require.Equal(t, errors.New("user not found"), err)

	_, err = s.getValidatedUser([]byte(username), []byte("wrongpass"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "crypto/bcrypt: hashedPassword is not the hash of the given password")

	_, err = s.getValidatedUser([]byte(username), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "crypto/bcrypt: hashedPassword is not the hash of the given password")

	_, err = s.getValidatedUser([]byte(username), []byte{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "crypto/bcrypt: hashedPassword is not the hash of the given password")
}

func TestServerIsValidDBName(t *testing.T) {
	err := isValidDBName("")
	require.Equal(t, errors.New("database name length outside of limits"), err)

	err = isValidDBName(strings.Repeat("a", 129))
	require.Equal(t, errors.New("database name length outside of limits"), err)

	err = isValidDBName(" ")
	require.Equal(t, errors.New("unrecognized character in database name"), err)

	err = isValidDBName("-")
	require.Equal(t, errors.New("punctuation marks and symbols are not allowed in database name"), err)

	err = isValidDBName(strings.Repeat("a", 32))
	require.NoError(t, err)
}

func TestServerMandatoryAuth(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(auth.SysAdminPassword),
	}
	ctx := context.Background()
	lr, err := s.Login(ctx, r)
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	_, err = s.CreateUser(ctx, &schema.CreateUserRequest{
		User:       []byte("someuser"),
		Password:   []byte("Somepass1$"),
		Permission: 1,
		Database:   DefaultDBName,
	})
	require.NoError(t, err)
	s.dbList.Append(s.dbList.GetByIndex(0))
	require.True(t, s.mandatoryAuth())

	s.sysDB = nil
	require.True(t, s.mandatoryAuth())
}

func TestServerLoginAttempWithEmptyPassword(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithAdminPassword(auth.SysAdminPassword)
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	r := &schema.LoginRequest{
		User:     []byte(auth.SysAdminUsername),
		Password: []byte(``),
	}
	ctx := context.Background()
	_, err = s.Login(ctx, r)

	require.Contains(t, err.Error(), "invalid user name or password")
}

func TestServerMaintenanceMode(t *testing.T) {
	serverOptions := DefaultOptions().WithMetricsServer(false).WithMaintenance(true).WithAuth(false)
	s, closer := testServer(serverOptions)
	defer closer()

	err := s.Initialize()
	require.NoError(t, err)

	_, err = s.CreateUser(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.ChangePassword(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.ChangePermission(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.SetActiveUser(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.CreateDatabaseWith(context.Background(), &schema.DatabaseSettings{})
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.UpdateDatabase(context.Background(), &schema.DatabaseSettings{})
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.Set(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.VerifiableSet(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.SetReference(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.VerifiableSetReference(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.ZAdd(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.VerifiableZAdd(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.Delete(context.Background(), nil)
	require.Contains(t, err.Error(), store.ErrIllegalArguments.Error())

	_, err = s.Delete(context.Background(), &schema.DeleteKeysRequest{})
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.ExecAll(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	_, err = s.SQLExec(context.Background(), nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	err = s.StreamSet(nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	err = s.StreamVerifiableSet(nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())

	err = s.StreamExecAll(nil)
	require.Contains(t, err.Error(), ErrNotAllowedInMaintenanceMode.Error())
}
