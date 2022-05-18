/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
package database

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/fs"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
)

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

func makeDb() (*db, func()) {
	rootPath := "data_" + strconv.FormatInt(time.Now().UnixNano(), 10)

	options := DefaultOption().WithDBRootPath(rootPath).WithCorruptionChecker(false)
	options.storeOpts.WithIndexOptions(options.storeOpts.IndexOpts.WithCompactionThld(2))

	return makeDbWith("db", options)
}

func makeDbWith(dbName string, opts *Options) (*db, func()) {
	d, err := NewDB(dbName, &dummyMultidbHandler{}, opts, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		log.Fatalf("Error creating Db instance %s", err)
	}

	return d.(*db), func() {
		if err := d.Close(); err != nil {
			log.Fatal(err)
		}

		if err := os.RemoveAll(d.GetOptions().dbRootPath); err != nil {
			log.Fatal(err)
		}
	}
}

type dummyMultidbHandler struct {
}

func (h *dummyMultidbHandler) ListDatabases(ctx context.Context) ([]string, error) {
	return nil, sql.ErrNoSupported
}

func (h *dummyMultidbHandler) CreateDatabase(ctx context.Context, db string, ifNotExists bool) error {
	return sql.ErrNoSupported
}

func (h *dummyMultidbHandler) UseDatabase(ctx context.Context, db string) error {
	return sql.ErrNoSupported
}

func (h *dummyMultidbHandler) ExecPreparedStmts(ctx context.Context, stmts []sql.SQLStmt, params map[string]interface{}) (ntx *sql.SQLTx, committedTxs []*sql.SQLTx, err error) {
	return nil, nil, sql.ErrNoSupported
}

func TestDefaultDbCreation(t *testing.T) {
	options := DefaultOption()
	db, err := NewDB("mydb", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Fatalf("Error creating Db instance %s", err)
	}

	require.Equal(t, options, db.GetOptions())

	defer func() {
		db.Close()
		time.Sleep(1 * time.Second)
		os.RemoveAll(options.GetDBRootPath())
	}()

	n, err := db.Size()
	require.NoError(t, err)
	require.Equal(t, uint64(1), n)

	_, err = db.Count(nil)
	require.Error(t, err)

	_, err = db.CountAll()
	require.Error(t, err)

	dbPath := path.Join(options.GetDBRootPath(), db.GetName())
	if _, err = os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Db dir not created")
	}

	_, err = os.Stat(path.Join(options.GetDBRootPath()))
	if os.IsNotExist(err) {
		t.Fatalf("Data dir not created")
	}
}

func TestDbCreationInAlreadyExistentDirectories(t *testing.T) {
	options := DefaultOption().WithDBRootPath("Paris")
	defer os.RemoveAll(options.GetDBRootPath())

	err := os.MkdirAll(options.GetDBRootPath(), os.ModePerm)
	require.NoError(t, err)

	err = os.MkdirAll(filepath.Join(options.GetDBRootPath(), "EdithPiaf"), os.ModePerm)
	require.NoError(t, err)

	_, err = NewDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.Error(t, err)
}

func TestDbCreationInInvalidDirectory(t *testing.T) {
	options := DefaultOption().WithDBRootPath("/?")
	defer os.RemoveAll(options.GetDBRootPath())

	_, err := NewDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.Error(t, err)
}

func TestDbCreation(t *testing.T) {
	options := DefaultOption().WithDBRootPath("Paris")
	db, err := NewDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Fatalf("Error creating Db instance %s", err)
	}

	defer func() {
		db.Close()
		time.Sleep(1 * time.Second)
		os.RemoveAll(options.GetDBRootPath())
	}()

	dbPath := path.Join(options.GetDBRootPath(), db.GetName())
	if _, err = os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Db dir not created")
	}

	_, err = os.Stat(options.GetDBRootPath())
	if os.IsNotExist(err) {
		t.Fatalf("Data dir not created")
	}
}

func TestOpenWithMissingDBDirectories(t *testing.T) {
	options := DefaultOption().WithDBRootPath("Paris")
	_, err := OpenDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.Error(t, err)
}

func TestOpenWithIllegalDBName(t *testing.T) {
	options := DefaultOption().WithDBRootPath("Paris")
	_, err := OpenDB("", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestOpenDB(t *testing.T) {
	options := DefaultOption().WithDBRootPath("Paris")
	db, err := NewDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Fatalf("Error creating Db instance %s", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Error closing store %s", err)
	}

	db, err = OpenDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Fatalf("Error opening database %s", err)
	}

	db.Close()
	time.Sleep(1 * time.Second)
	os.RemoveAll(options.GetDBRootPath())
}

func TestOpenV1_0_1_DB(t *testing.T) {
	copier := fs.NewStandardCopier()
	require.NoError(t, copier.CopyDir("../../test/data_v1.1.0", "data_v1.1.0"))

	defer os.RemoveAll("data_v1.1.0")

	sysOpts := DefaultOption().WithDBRootPath("./data_v1.1.0")
	sysDB, err := OpenDB("systemdb", nil, sysOpts, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	dbOpts := DefaultOption().WithDBRootPath("./data_v1.1.0")
	db, err := OpenDB("defaultdb", nil, dbOpts, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	err = sysDB.Close()
	require.NoError(t, err)
}

func TestDbSynchronousSet(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for _, kv := range kvs {
		_, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{kv}})
		require.NoError(t, err)

		item, err := db.Get(&schema.KeyRequest{Key: kv.Key})
		require.NoError(t, err)
		require.Equal(t, kv.Key, item.Key)
		require.Equal(t, kv.Value, item.Value)
	}
}

func TestDbSetGet(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	var trustedAlh [sha256.Size]byte
	var trustedIndex uint64

	_, err := db.Set(nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.VerifiableGet(nil)
	require.Equal(t, ErrIllegalArguments, err)

	for i, kv := range kvs[:1] {
		txhdr, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{kv}})
		require.NoError(t, err)
		require.Equal(t, uint64(i+2), txhdr.Id)

		if i == 0 {
			alh := schema.TxHeaderFromProto(txhdr).Alh()
			copy(trustedAlh[:], alh[:])
			trustedIndex = 2
		}

		keyReq := &schema.KeyRequest{Key: kv.Key, SinceTx: txhdr.Id}

		item, err := db.Get(keyReq)
		require.NoError(t, err)
		require.Equal(t, kv.Key, item.Key)
		require.Equal(t, kv.Value, item.Value)

		_, err = db.Get(&schema.KeyRequest{Key: kv.Key, SinceTx: txhdr.Id, AtTx: txhdr.Id})
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, err = db.Get(&schema.KeyRequest{Key: kv.Key, SinceTx: txhdr.Id + 1})
		require.ErrorIs(t, err, ErrIllegalArguments)

		vitem, err := db.VerifiableGet(&schema.VerifiableGetRequest{
			KeyRequest:   keyReq,
			ProveSinceTx: trustedIndex,
		})
		require.NoError(t, err)
		require.Equal(t, kv.Key, vitem.Entry.Key)
		require.Equal(t, kv.Value, vitem.Entry.Value)

		inclusionProof := schema.InclusionProofFromProto(vitem.InclusionProof)
		dualProof := schema.DualProofFromProto(vitem.VerifiableTx.DualProof)

		var eh [sha256.Size]byte
		var sourceID, targetID uint64
		var sourceAlh, targetAlh [sha256.Size]byte

		if trustedIndex <= vitem.Entry.Tx {
			copy(eh[:], dualProof.TargetTxHeader.Eh[:])
			sourceID = trustedIndex
			sourceAlh = trustedAlh
			targetID = vitem.Entry.Tx
			targetAlh = dualProof.TargetTxHeader.Alh()
		} else {
			copy(eh[:], dualProof.SourceTxHeader.Eh[:])
			sourceID = vitem.Entry.Tx
			sourceAlh = dualProof.SourceTxHeader.Alh()
			targetID = trustedIndex
			targetAlh = trustedAlh
		}

		entrySpec := EncodeEntrySpec(vitem.Entry.Key, schema.KVMetadataFromProto(vitem.Entry.Metadata), vitem.Entry.Value)

		entrySpecDigest, err := store.EntrySpecDigestFor(int(txhdr.Version))
		require.NoError(t, err)
		require.NotNil(t, entrySpecDigest)

		verifies := store.VerifyInclusion(
			inclusionProof,
			entrySpecDigest(entrySpec),
			eh,
		)
		require.True(t, verifies)

		verifies = store.VerifyDualProof(
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		require.True(t, verifies)
	}

	_, err = db.Get(&schema.KeyRequest{Key: []byte{}})
	require.Error(t, err)
}

func TestDelete(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.Delete(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   nil,
				Value: []byte("value1"),
			},
		},
	})
	require.ErrorIs(t, err, ErrIllegalArguments)

	hdr, err := db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte("key1"),
				Value: []byte("value1"),
			},
		},
	})
	require.NoError(t, err)

	t.Run("deletion with invalid indexing spec should return an error", func(t *testing.T) {
		_, err = db.Delete(&schema.DeleteKeysRequest{
			Keys: [][]byte{
				[]byte("key1"),
			},
			SinceTx: hdr.Id + 1,
		})
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	_, err = db.Get(&schema.KeyRequest{
		Key: []byte("key1"),
	})
	require.NoError(t, err)

	hdr, err = db.Delete(&schema.DeleteKeysRequest{
		Keys: [][]byte{
			[]byte("key1"),
		},
	})
	require.NoError(t, err)

	_, err = db.Get(&schema.KeyRequest{
		Key: []byte("key1"),
	})
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	_, err = db.VerifiableGet(&schema.VerifiableGetRequest{
		KeyRequest: &schema.KeyRequest{
			Key:  []byte("key1"),
			AtTx: hdr.Id,
		},
	})
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	tx, err := db.TxByID(&schema.TxRequest{
		Tx: hdr.Id,
		EntriesSpec: &schema.EntriesSpec{
			KvEntriesSpec: &schema.EntryTypeSpec{
				Action: schema.EntryTypeAction_RESOLVE,
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, tx)
	require.Empty(t, tx.KvEntries)
}

func TestCurrentState(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range kvs {
		txhdr, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
		require.Equal(t, uint64(ind+2), txhdr.Id)

		time.Sleep(1 * time.Second)

		state, err := db.CurrentState()
		require.NoError(t, err)
		require.Equal(t, uint64(ind+2), state.TxId)
	}
}

func TestSafeSetGet(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	state, err := db.CurrentState()
	require.NoError(t, err)

	_, err = db.VerifiableSet(nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.VerifiableSet(&schema.VerifiableSetRequest{
		SetRequest: &schema.SetRequest{
			KVs: []*schema.KeyValue{
				{
					Key:   []byte("Alberto"),
					Value: []byte("Tomba"),
				},
			},
		},
		ProveSinceTx: 2,
	})
	require.Equal(t, ErrIllegalState, err)

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

	for ind, val := range kv {
		vtx, err := db.VerifiableSet(val)
		require.NoError(t, err)
		require.NotNil(t, vtx)

		vit, err := db.VerifiableGet(&schema.VerifiableGetRequest{
			KeyRequest: &schema.KeyRequest{
				Key:     val.SetRequest.KVs[0].Key,
				SinceTx: vtx.Tx.Header.Id,
			},
		})
		require.NoError(t, err)
		require.Equal(t, uint64(ind+2), vit.Entry.Tx)
	}
}

func TestSetGetAll(t *testing.T) {
	db, closer := makeDb()
	defer closer()

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

	txhdr, err := db.Set(&schema.SetRequest{KVs: kvs})
	require.NoError(t, err)
	require.Equal(t, uint64(2), txhdr.Id)

	err = db.CompactIndex()
	require.NoError(t, err)

	itList, err := db.GetAll(&schema.KeyListRequest{
		Keys: [][]byte{
			[]byte("Alberto"),
			[]byte("Jean-Claude"),
			[]byte("Franz"),
		},
		SinceTx: txhdr.Id,
	})
	require.NoError(t, err)

	for ind, val := range itList.Entries {
		require.Equal(t, kvs[ind].Value, val.Value)
	}
}

func TestTxByID(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.TxByID(nil)
	require.Error(t, ErrIllegalArguments, err)

	txhdr1, err := db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{
			{Key: []byte("key0"), Value: []byte("value0")},
		},
	})
	require.NoError(t, err)

	txhdr2, err := db.ExecAll(&schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Ref{
					Ref: &schema.ReferenceRequest{
						Key:           []byte("ref1"),
						ReferencedKey: []byte("key0"),
					},
				},
			},
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
				},
			},
			{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:   []byte("set1"),
						Score: 10,
						Key:   []byte("key1"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	_, _, err = db.SQLExec(&schema.SQLExecRequest{Sql: "CREATE TABLE mytable(id INTEGER AUTO_INCREMENT, PRIMARY KEY id)"}, nil)
	require.NoError(t, err)

	_, ctx1, err := db.SQLExec(&schema.SQLExecRequest{Sql: "INSERT INTO mytable() VALUES ()"}, nil)
	require.NoError(t, err)
	require.Len(t, ctx1, 1)

	txhdr3 := ctx1[0].TxHeader()

	t.Run("values should not be resolved but digests returned in entries field", func(t *testing.T) {
		tx, err := db.TxByID(&schema.TxRequest{Tx: txhdr1.Id})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 1)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			require.Len(t, e.Value, 0)
		}

		tx, err = db.TxByID(&schema.TxRequest{Tx: txhdr2.Id})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 3)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			require.Len(t, e.Value, 0)
		}

		tx, err = db.TxByID(&schema.TxRequest{Tx: txhdr3.ID})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 1)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			require.Len(t, e.Value, 0)
		}
	})

	t.Run("values should not be resolved but digests returned in entries field", func(t *testing.T) {
		tx, err := db.TxByID(&schema.TxRequest{Tx: txhdr1.Id, EntriesSpec: &schema.EntriesSpec{
			KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_ONLY_DIGEST},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 1)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			require.Len(t, e.Value, 0)
		}

		tx, err = db.TxByID(&schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{
			KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_ONLY_DIGEST},
			ZEntriesSpec:  &schema.EntryTypeSpec{Action: schema.EntryTypeAction_ONLY_DIGEST},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 3)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			require.Len(t, e.Value, 0)
		}

		tx, err = db.TxByID(&schema.TxRequest{Tx: txhdr3.ID, EntriesSpec: &schema.EntriesSpec{
			SqlEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_ONLY_DIGEST},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 1)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			require.Len(t, e.Value, 0)
		}
	})

	t.Run("no entries should be returned if not explicitly included", func(t *testing.T) {
		tx, err := db.TxByID(&schema.TxRequest{Tx: txhdr1.Id, EntriesSpec: &schema.EntriesSpec{}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		tx, err = db.TxByID(&schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		tx, err = db.TxByID(&schema.TxRequest{Tx: txhdr3.ID, EntriesSpec: &schema.EntriesSpec{}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)
	})

	t.Run("no entries should be returned if explicitly excluded", func(t *testing.T) {
		tx, err := db.TxByID(&schema.TxRequest{Tx: txhdr1.Id, EntriesSpec: &schema.EntriesSpec{
			KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		tx, err = db.TxByID(&schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{
			KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
			ZEntriesSpec:  &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		tx, err = db.TxByID(&schema.TxRequest{Tx: txhdr3.ID, EntriesSpec: &schema.EntriesSpec{
			SqlEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)
	})

	t.Run("raw entries should be returned", func(t *testing.T) {
		tx, err := db.TxByID(&schema.TxRequest{Tx: txhdr1.Id, EntriesSpec: &schema.EntriesSpec{
			KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_RAW_VALUE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 1)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			hval := sha256.Sum256(e.Value)
			require.Equal(t, e.HValue, hval[:])
		}

		tx, err = db.TxByID(&schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{
			ZEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_RAW_VALUE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 1)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			hval := sha256.Sum256(e.Value)
			require.Equal(t, e.HValue, hval[:])
		}

		tx, err = db.TxByID(&schema.TxRequest{Tx: txhdr3.ID, EntriesSpec: &schema.EntriesSpec{
			SqlEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_RAW_VALUE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 1)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			hval := sha256.Sum256(e.Value)
			require.Equal(t, e.HValue, hval[:])
		}
	})

	t.Run("only kv entries should be resolved", func(t *testing.T) {
		tx, err := db.TxByID(&schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{
			KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_RESOLVE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Len(t, tx.KvEntries, 2)
		require.Empty(t, tx.ZEntries)

		for i, e := range tx.KvEntries {
			require.Equal(t, []byte(fmt.Sprintf("key%d", i)), e.Key)
			require.Equal(t, []byte(fmt.Sprintf("value%d", i)), e.Value)
		}
	})

	t.Run("only kv entries should be resolved (but not references)", func(t *testing.T) {
		tx, err := db.TxByID(&schema.TxRequest{
			Tx: txhdr2.Id,
			EntriesSpec: &schema.EntriesSpec{
				KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_RESOLVE},
			},
			KeepReferencesUnresolved: true,
		})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Len(t, tx.KvEntries, 2)
		require.Empty(t, tx.ZEntries)

		for i, e := range tx.KvEntries {
			require.Equal(t, []byte(fmt.Sprintf("key%d", i)), e.Key)

			if e.ReferencedBy == nil {
				require.Equal(t, []byte(fmt.Sprintf("value%d", i)), e.Value)
			} else {
				require.Empty(t, e.Value)
			}
		}
	})

	t.Run("only zentries should be resolved", func(t *testing.T) {
		tx, err := db.TxByID(&schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{
			ZEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_RESOLVE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Len(t, tx.ZEntries, 1)

		require.Equal(t, []byte("set1"), tx.ZEntries[0].Set)
		require.Equal(t, []byte("key1"), tx.ZEntries[0].Key)
		require.Equal(t, float64(10), tx.ZEntries[0].Score)
		require.NotNil(t, tx.ZEntries[0].Entry)
	})

	t.Run("only zentries should be resolved (but not including entries)", func(t *testing.T) {
		tx, err := db.TxByID(&schema.TxRequest{
			Tx: txhdr2.Id,
			EntriesSpec: &schema.EntriesSpec{
				ZEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_RESOLVE},
			},
			KeepReferencesUnresolved: true,
		})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Len(t, tx.ZEntries, 1)

		require.Equal(t, []byte("set1"), tx.ZEntries[0].Set)
		require.Equal(t, []byte("key1"), tx.ZEntries[0].Key)
		require.Equal(t, float64(10), tx.ZEntries[0].Score)
		require.Nil(t, tx.ZEntries[0].Entry)
	})

	t.Run("sql entries can not be resolved", func(t *testing.T) {
		_, err := db.TxByID(&schema.TxRequest{Tx: txhdr3.ID, EntriesSpec: &schema.EntriesSpec{
			SqlEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_RESOLVE},
		}})
		require.ErrorIs(t, err, ErrIllegalArguments)
	})
}

func TestVerifiableTxByID(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.VerifiableTxByID(nil)
	require.Error(t, ErrIllegalArguments, err)

	var txhdr *schema.TxHeader

	for _, val := range kvs {
		txhdr, err = db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
	}

	t.Run("values should be returned", func(t *testing.T) {
		vtx, err := db.VerifiableTxByID(&schema.VerifiableTxRequest{
			Tx:           txhdr.Id,
			ProveSinceTx: 0,
			EntriesSpec: &schema.EntriesSpec{
				KvEntriesSpec: &schema.EntryTypeSpec{
					Action: schema.EntryTypeAction_RAW_VALUE,
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, vtx)
		require.Len(t, vtx.Tx.Entries, 1)

		hval := sha256.Sum256(vtx.Tx.Entries[0].Value)
		require.Equal(t, vtx.Tx.Entries[0].HValue, hval[:])
	})

	t.Run("values should not be returned", func(t *testing.T) {
		vtx, err := db.VerifiableTxByID(&schema.VerifiableTxRequest{
			Tx:           txhdr.Id,
			ProveSinceTx: 0,
		})
		require.NoError(t, err)
		require.NotNil(t, vtx)
		require.Len(t, vtx.Tx.Entries, 1)
		require.Len(t, vtx.Tx.Entries[0].Value, 0)
	})
}

func TestTxScan(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	db.maxResultSize = len(kvs)

	initialState, err := db.CurrentState()
	require.NoError(t, err)

	for _, val := range kvs {
		_, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
	}

	_, err = db.TxScan(nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.TxScan(&schema.TxScanRequest{
		InitialTx: 0,
	})
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.TxScan(&schema.TxScanRequest{
		InitialTx: 1,
		Limit:     uint32(db.MaxResultSize() + 1),
	})
	require.ErrorIs(t, err, ErrResultSizeLimitExceeded)

	t.Run("values should be returned reaching result size limit", func(t *testing.T) {
		txList, err := db.TxScan(&schema.TxScanRequest{
			InitialTx: initialState.TxId + 1,
			EntriesSpec: &schema.EntriesSpec{
				KvEntriesSpec: &schema.EntryTypeSpec{
					Action: schema.EntryTypeAction_RAW_VALUE,
				},
			},
		})
		require.ErrorIs(t, err, ErrResultSizeLimitReached)
		require.Len(t, txList.Txs, len(kvs))

		for i := 0; i < len(kvs); i++ {
			require.Equal(t, kvs[i].Key, TrimPrefix(txList.Txs[i].Entries[0].Key))

			hval := sha256.Sum256(txList.Txs[i].Entries[0].Value)
			require.Equal(t, txList.Txs[i].Entries[0].HValue, hval[:])
		}
	})

	t.Run("values should be returned without reaching result size limit", func(t *testing.T) {
		limit := db.MaxResultSize() - 1

		txList, err := db.TxScan(&schema.TxScanRequest{
			InitialTx: initialState.TxId + 1,
			Limit:     uint32(limit),
			EntriesSpec: &schema.EntriesSpec{
				KvEntriesSpec: &schema.EntryTypeSpec{
					Action: schema.EntryTypeAction_RAW_VALUE,
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, txList.Txs, limit)

		for i := 0; i < limit; i++ {
			require.Equal(t, kvs[i].Key, TrimPrefix(txList.Txs[i].Entries[0].Key))

			hval := sha256.Sum256(txList.Txs[i].Entries[0].Value)
			require.Equal(t, txList.Txs[i].Entries[0].HValue, hval[:])
		}
	})

	t.Run("values should not be returned", func(t *testing.T) {
		txList, err := db.TxScan(&schema.TxScanRequest{
			InitialTx: initialState.TxId + 1,
		})
		require.ErrorIs(t, err, ErrResultSizeLimitReached)
		require.Len(t, txList.Txs, len(kvs))

		for i := 0; i < len(kvs); i++ {
			require.Equal(t, kvs[i].Key, TrimPrefix(txList.Txs[i].Entries[0].Key))
			require.Len(t, txList.Txs[i].Entries[0].Value, 0)
		}
	})
}

func TestHistory(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	db.maxResultSize = 2

	var lastTx uint64

	for _, val := range kvs {
		_, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
	}

	err := db.FlushIndex(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = db.FlushIndex(&schema.FlushIndexRequest{CleanupPercentage: 100, Synced: true})
	require.NoError(t, err)

	_, err = db.Delete(&schema.DeleteKeysRequest{Keys: [][]byte{kvs[0].Key}})
	require.NoError(t, err)

	meta, err := db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   kvs[0].Key,
			Value: kvs[0].Value,
		}},
	})
	require.NoError(t, err)
	lastTx = meta.Id

	time.Sleep(1 * time.Millisecond)

	_, err = db.History(nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.History(&schema.HistoryRequest{
		Key:     kvs[0].Key,
		SinceTx: lastTx,
		Limit:   int32(db.MaxResultSize() + 1),
	})
	require.ErrorIs(t, err, ErrResultSizeLimitExceeded)

	inc, err := db.History(&schema.HistoryRequest{
		Key:     kvs[0].Key,
		SinceTx: lastTx,
	})
	require.ErrorIs(t, err, ErrResultSizeLimitReached)

	for i, val := range inc.Entries {
		require.Equal(t, kvs[0].Key, val.Key)
		if val.GetMetadata().GetDeleted() {
			require.Empty(t, val.Value)
		} else {
			require.Equal(t, kvs[0].Value, val.Value)
		}
		require.EqualValues(t, i+1, val.Revision)
	}

	dec, err := db.History(&schema.HistoryRequest{
		Key:     kvs[0].Key,
		SinceTx: lastTx,
		Desc:    true,
	})
	require.ErrorIs(t, err, ErrResultSizeLimitReached)

	for i, val := range dec.Entries {
		require.Equal(t, kvs[0].Key, val.Key)
		if val.GetMetadata().GetDeleted() {
			require.Empty(t, val.Value)
		} else {
			require.Equal(t, kvs[0].Value, val.Value)
		}
		require.EqualValues(t, 3-i, val.Revision)
	}

	inc, err = db.History(&schema.HistoryRequest{
		Key:     kvs[0].Key,
		Offset:  uint64(len(kvs) + 1),
		SinceTx: lastTx,
	})
	require.NoError(t, err)
	require.Empty(t, inc.Entries)
}

func TestPreconditionedSet(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key-no-preconditions"),
			Value: []byte("value"),
		}},
	})
	require.NoError(t, err, "could not set a value without preconditions")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustExist([]byte("key")),
		},
	})
	require.ErrorIs(t, err, store.ErrPreconditionFailed,
		"did not detect missing key when MustExist precondition was present")

	tx1, err := db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustNotExist([]byte("key")),
		},
	})
	require.NoError(t, err,
		"failed to add a key with MustNotExist precondition even though the key does not exist")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustNotExist([]byte("key")),
		},
	})
	require.ErrorIs(t, err, store.ErrPreconditionFailed,
		"did not detect existing key even though MustNotExist precondition was used")

	tx2, err := db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustExist([]byte("key")),
		},
	})
	require.NoError(t, err,
		"did not add a key even though MustExist precondition was successful")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyNotModifiedAfterTX([]byte("key"), tx1.Id),
		},
	})
	require.ErrorIs(t, err, store.ErrPreconditionFailed,
		"did not detect NotModifiedAfterTX precondition")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyNotModifiedAfterTX([]byte("key"), tx2.Id),
		},
	})
	require.NoError(t, err,
		"did not add valid entry with NotModifiedAfterTX precondition")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyNotModifiedAfterTX([]byte("key"), tx2.Id),
		},
	})
	require.ErrorIs(t, err, store.ErrPreconditionFailed,
		"did not detect failed NotModifiedAfterTX precondition after new entry was added")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key2"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyNotModifiedAfterTX([]byte("key2"), tx2.Id),
		},
	})
	require.NoError(t, err,
		"did not add entry with NotModifiedAfterTX precondition when the key does not exist")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key3"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustExist([]byte("key3")),
			schema.PreconditionKeyNotModifiedAfterTX([]byte("key3"), tx2.Id),
		},
	})
	require.ErrorIs(t, err, store.ErrPreconditionFailed,
		"did not detect failed mix of NotModifiedAfterTX and MustExist preconditions when the key does not exist")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key3"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustExist([]byte("key3")),
			schema.PreconditionKeyMustNotExist([]byte("key3")),
		},
	})
	require.ErrorIs(t, err, store.ErrPreconditionFailed,
		"did not detect failed mix of MustNotExist and MustExist preconditions when the key does not exist")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte("key4-no-preconditions"),
				Value: []byte("value"),
			},
			{
				Key:   []byte("key5-with-preconditions"),
				Value: []byte("value"),
			},
		},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustExist([]byte("key5-with-preconditions")),
		},
	})
	require.ErrorIs(t, err, store.ErrPreconditionFailed,
		"did not fail even though one of KV entries failed precondition check")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte("key4-no-preconditions"),
				Value: []byte("value"),
			},
		},
		Preconditions: []*schema.Precondition{nil},
	})
	require.ErrorIs(t, err, store.ErrInvalidPrecondition,
		"did not fail when invalid nil precondition was given")

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte("key6"),
				Value: []byte("value"),
			},
		},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustNotExist(
				[]byte("key6-too-long-key" + strings.Repeat("*", db.GetOptions().storeOpts.MaxKeyLen)),
			),
		},
	})
	require.ErrorIs(t, err, store.ErrInvalidPrecondition,
		"did not fail when invalid nil precondition was given")

	c := []*schema.Precondition{}
	for i := 0; i <= db.GetOptions().storeOpts.MaxTxEntries; i++ {
		c = append(c, schema.PreconditionKeyMustNotExist([]byte(fmt.Sprintf("key_%d", i))))
	}

	_, err = db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte("key6"),
				Value: []byte("value"),
			},
		},
		Preconditions: c,
	})
	require.ErrorIs(t, err, store.ErrInvalidPrecondition,
		"did not fail when too many preconditions were given")
}

func TestPreconditionedSetParallel(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	const parallelism = 10

	runInParallel := func(f func(i int) error) (failCount int32, successCount int32, badErrorCount int32) {
		var wg, wg2 sync.WaitGroup
		wg.Add(parallelism)
		wg2.Add(parallelism)

		for i := 0; i < parallelism; i++ {
			go func(i int) {
				defer wg2.Done()

				// Sync all goroutines to a single point
				wg.Done()
				wg.Wait()

				err := f(i)

				if err == nil {
					atomic.AddInt32(&successCount, 1)
				} else if errors.Is(err, store.ErrPreconditionFailed) {
					atomic.AddInt32(&failCount, 1)
				} else {
					log.Println(err)
					atomic.AddInt32(&badErrorCount, 1)
				}
			}(i)
		}

		wg2.Wait()

		return failCount, successCount, badErrorCount
	}

	t.Run("Set", func(t *testing.T) {

		t.Run("MustNotExist", func(t *testing.T) {

			var wg, wg2 sync.WaitGroup
			wg.Add(parallelism)
			wg2.Add(parallelism)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.Set(&schema.SetRequest{
					KVs: []*schema.KeyValue{{
						Key:   []byte(`key`),
						Value: []byte(fmt.Sprintf("goroutine: %d", i)),
					}},
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyMustNotExist([]byte(`key`)),
					},
				})
				return err
			})

			require.EqualValues(t, 1, successCount)
			require.EqualValues(t, parallelism-1, failCount)
			require.Zero(t, badError)
		})

		t.Run("MustExist", func(t *testing.T) {

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.Set(&schema.SetRequest{
					KVs: []*schema.KeyValue{{
						Key:   []byte(`key`),
						Value: []byte(fmt.Sprintf("goroutine: %d", i)),
					}},
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyMustExist([]byte(`key`)),
					},
				})
				return err
			})

			require.EqualValues(t, parallelism, successCount)
			require.Zero(t, failCount)
			require.Zero(t, badError)
		})

		t.Run("NotModifiedAfterTX", func(t *testing.T) {

			tx, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{
				Key:   []byte(`key`),
				Value: []byte(`base value`),
			}}})
			require.NoError(t, err)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.Set(&schema.SetRequest{
					KVs: []*schema.KeyValue{{
						Key:   []byte(`key`),
						Value: []byte(fmt.Sprintf("goroutine: %d", i)),
					}},
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyNotModifiedAfterTX([]byte(`key`), tx.Id),
					},
				})
				return err
			})

			require.EqualValues(t, 1, successCount)
			require.EqualValues(t, parallelism-1, failCount)
			require.Zero(t, badError)
		})
	})

	t.Run("Reference", func(t *testing.T) {

		t.Run("MustNotExist", func(t *testing.T) {

			var wg, wg2 sync.WaitGroup
			wg.Add(parallelism)
			wg2.Add(parallelism)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.SetReference(&schema.ReferenceRequest{
					Key:           []byte(`reference`),
					ReferencedKey: []byte(`key`),
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyMustNotExist([]byte(`reference`)),
					},
				})
				return err
			})

			require.EqualValues(t, 1, successCount)
			require.EqualValues(t, parallelism-1, failCount)
			require.Zero(t, badError)
		})

		t.Run("MustExist", func(t *testing.T) {

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.SetReference(&schema.ReferenceRequest{
					Key:           []byte(`reference`),
					ReferencedKey: []byte(`key`),
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyMustExist([]byte(`reference`)),
					},
				})
				return err
			})

			require.EqualValues(t, parallelism, successCount)
			require.Zero(t, failCount)
			require.Zero(t, badError)
		})

		t.Run("NotModifiedAfterTX", func(t *testing.T) {

			tx, err := db.SetReference(&schema.ReferenceRequest{
				Key:           []byte(`reference`),
				ReferencedKey: []byte(`key`),
			})
			require.NoError(t, err)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.SetReference(&schema.ReferenceRequest{
					Key:           []byte(`reference`),
					ReferencedKey: []byte(`key`),
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyNotModifiedAfterTX([]byte(`reference`), tx.Id),
					},
				})
				return err
			})

			require.EqualValues(t, 1, successCount)
			require.EqualValues(t, parallelism-1, failCount)
			require.Zero(t, badError)
		})
	})

	t.Run("ExecAll-KV", func(t *testing.T) {

		t.Run("MustNotExist", func(t *testing.T) {

			var wg, wg2 sync.WaitGroup
			wg.Add(parallelism)
			wg2.Add(parallelism)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.ExecAll(&schema.ExecAllRequest{
					Operations: []*schema.Op{{
						Operation: &schema.Op_Kv{
							Kv: &schema.KeyValue{
								Key:   []byte(`key-ea`),
								Value: []byte(fmt.Sprintf("goroutine: %d", i)),
							},
						},
					}},
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyMustNotExist([]byte(`key-ea`)),
					},
				})
				return err
			})

			require.EqualValues(t, 1, successCount)
			require.EqualValues(t, parallelism-1, failCount)
			require.Zero(t, badError)
		})

		t.Run("MustExist", func(t *testing.T) {

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.ExecAll(&schema.ExecAllRequest{
					Operations: []*schema.Op{{
						Operation: &schema.Op_Kv{
							Kv: &schema.KeyValue{
								Key:   []byte(`key-ea`),
								Value: []byte(fmt.Sprintf("goroutine: %d", i)),
							},
						},
					}},
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyMustExist([]byte(`key-ea`)),
					},
				})
				return err
			})

			require.EqualValues(t, parallelism, successCount)
			require.Zero(t, failCount)
			require.Zero(t, badError)
		})

		t.Run("NotModifiedAfterTX", func(t *testing.T) {

			tx, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{
				Key:   []byte(`key-ea`),
				Value: []byte(`base value`),
			}}})
			require.NoError(t, err)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.ExecAll(&schema.ExecAllRequest{
					Operations: []*schema.Op{{
						Operation: &schema.Op_Kv{
							Kv: &schema.KeyValue{
								Key:   []byte(`key-ea`),
								Value: []byte(fmt.Sprintf("goroutine: %d", i)),
							},
						},
					}},
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyNotModifiedAfterTX([]byte(`key-ea`), tx.Id),
					},
				})
				return err
			})

			require.EqualValues(t, 1, successCount)
			require.EqualValues(t, parallelism-1, failCount)
			require.Zero(t, badError)
		})
	})

	t.Run("ExecAll-Ref", func(t *testing.T) {

		t.Run("MustNotExist", func(t *testing.T) {

			var wg, wg2 sync.WaitGroup
			wg.Add(parallelism)
			wg2.Add(parallelism)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.ExecAll(&schema.ExecAllRequest{
					Operations: []*schema.Op{{
						Operation: &schema.Op_Ref{
							Ref: &schema.ReferenceRequest{
								Key:           []byte(`reference-ea`),
								ReferencedKey: []byte(`key-ea`),
							},
						},
					}},
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyMustNotExist([]byte(`reference-ea`)),
					},
				})
				return err
			})

			require.EqualValues(t, 1, successCount)
			require.EqualValues(t, parallelism-1, failCount)
			require.Zero(t, badError)
		})

		t.Run("MustExist", func(t *testing.T) {

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.ExecAll(&schema.ExecAllRequest{
					Operations: []*schema.Op{{
						Operation: &schema.Op_Ref{
							Ref: &schema.ReferenceRequest{
								Key:           []byte(`reference-ea`),
								ReferencedKey: []byte(`key-ea`),
							},
						},
					}},
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyMustExist([]byte(`reference-ea`)),
					},
				})
				return err
			})

			require.EqualValues(t, parallelism, successCount)
			require.Zero(t, failCount)
			require.Zero(t, badError)
		})

		t.Run("NotModifiedAfterTX", func(t *testing.T) {

			tx, err := db.SetReference(&schema.ReferenceRequest{
				Key:           []byte(`reference-ea`),
				ReferencedKey: []byte(`key-ea`),
			})
			require.NoError(t, err)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.ExecAll(&schema.ExecAllRequest{
					Operations: []*schema.Op{{
						Operation: &schema.Op_Ref{
							Ref: &schema.ReferenceRequest{
								Key:           []byte(`reference-ea`),
								ReferencedKey: []byte(`key-ea`),
							},
						},
					}},
					Preconditions: []*schema.Precondition{
						schema.PreconditionKeyNotModifiedAfterTX([]byte(`reference-ea`), tx.Id),
					},
				})
				return err
			})

			require.EqualValues(t, 1, successCount)
			require.EqualValues(t, parallelism-1, failCount)
			require.Zero(t, badError)
		})
	})

}

func TestCheckInvalidKeyRequest(t *testing.T) {
	for _, d := range []struct {
		req         *schema.KeyRequest
		errTextPart string
	}{
		{
			nil, "empty request",
		},
		{
			&schema.KeyRequest{}, "empty key",
		},
		{
			&schema.KeyRequest{
				Key:     []byte("test"),
				AtTx:    1,
				SinceTx: 2,
			},
			"SinceTx should not be specified when AtTx is used",
		},
		{
			&schema.KeyRequest{
				Key:        []byte("test"),
				AtTx:       1,
				AtRevision: 2,
			},
			"AtRevision should not be specified when AtTx is used",
		},
	} {
		t.Run(fmt.Sprintf("%+v", d), func(t *testing.T) {
			err := checkKeyRequest(d.req)
			require.ErrorIs(t, err, ErrIllegalArguments)
			require.Contains(t, err.Error(), d.errTextPart)
		})
	}
}

func TestGetAtRevision(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	const histCount = 10

	for i := 0; i < histCount; i++ {
		_, err := db.Set(&schema.SetRequest{
			KVs: []*schema.KeyValue{{
				Key:   []byte("key"),
				Value: []byte(fmt.Sprintf("value%d", i)),
			}},
		})
		require.NoError(t, err)
	}

	t.Run("get the most recent value if no revision is specified", func(t *testing.T) {
		k, err := db.Get(&schema.KeyRequest{
			Key: []byte("key"),
		})
		require.NoError(t, err)
		require.Equal(t, []byte("value9"), k.Value)
	})

	t.Run("get correct values for positive revision numbers", func(t *testing.T) {
		for i := 0; i < histCount; i++ {
			k, err := db.Get(&schema.KeyRequest{
				Key:        []byte("key"),
				AtRevision: int64(i) + 1,
			})
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("value%d", i)), k.Value)
		}
	})

	t.Run("get correct error if positive revision number is to high", func(t *testing.T) {
		_, err := db.Get(&schema.KeyRequest{
			Key:        []byte("key"),
			AtRevision: 11,
		})
		require.ErrorIs(t, err, ErrInvalidRevision)
	})

	t.Run("get correct error if maximum integer value is used for the revision number", func(t *testing.T) {
		_, err := db.Get(&schema.KeyRequest{
			Key:        []byte("key"),
			AtRevision: math.MaxInt64,
		})
		require.ErrorIs(t, err, ErrInvalidRevision)
	})

	t.Run("get correct values for negative revision numbers", func(t *testing.T) {
		for i := 1; i < histCount; i++ {
			k, err := db.Get(&schema.KeyRequest{
				Key:        []byte("key"),
				AtRevision: -int64(i),
			})
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("value%d", 9-i)), k.Value)
		}
	})

	t.Run("get correct error if negative revision number is to low", func(t *testing.T) {
		_, err := db.Get(&schema.KeyRequest{
			Key:        []byte("key"),
			AtRevision: -10,
		})
		require.ErrorIs(t, err, ErrInvalidRevision)
	})

	t.Run("get correct error if minimum negative revision number is used", func(t *testing.T) {
		_, err := db.Get(&schema.KeyRequest{
			Key:        []byte("key"),
			AtRevision: math.MinInt64,
		})
		require.ErrorIs(t, err, ErrInvalidRevision)
	})

	t.Run("get correct error if non-existing key is specified", func(t *testing.T) {
		_, err := db.Get(&schema.KeyRequest{
			Key:        []byte("non-existing-key"),
			AtRevision: 1,
		})
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	})

	t.Run("get correct error if expired entry is fetched through revision", func(t *testing.T) {
		_, err := db.Set(&schema.SetRequest{
			KVs: []*schema.KeyValue{{
				Key:   []byte("exp-key"),
				Value: []byte("expired-value"),
				Metadata: &schema.KVMetadata{
					Expiration: &schema.Expiration{
						ExpiresAt: time.Now().Unix() - 1,
					},
				},
			}},
		})
		require.NoError(t, err)

		_, err = db.Set(&schema.SetRequest{
			KVs: []*schema.KeyValue{{
				Key:   []byte("exp-key"),
				Value: []byte("not-expired-value"),
			}},
		})
		require.NoError(t, err)

		entry, err := db.Get(&schema.KeyRequest{
			Key: []byte("exp-key"),
		})
		require.NoError(t, err)
		require.Equal(t, []byte("not-expired-value"), entry.Value)
		require.EqualValues(t, 2, entry.Revision)

		_, err = db.Get(&schema.KeyRequest{
			Key:        []byte("exp-key"),
			AtRevision: 1,
		})
		require.ErrorIs(t, err, store.ErrExpiredEntry)
	})
}

/*
func TestReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	_, err := db.Set(kvs[0])
	if err != nil {
		t.Fatalf("Reference error %s", err)
	}
	ref, err := db.Reference(&schema.ReferenceOptions{
		Reference: []byte(`tag`),
		Key:       kvs[0].Key,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Index != 1 {
		t.Fatalf("Reference, expected %v, got %v", 1, ref.Index)
	}
	item, err := db.Get(&schema.Key{Key: []byte(`tag`)})
	if err != nil {
		t.Fatalf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Value, kvs[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(item.Value), string(kvs[0].Value))
	}
	item, err = db.GetReference(&schema.Key{Key: []byte(`tag`)})
	if err != nil {
		t.Fatalf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Value, kvs[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(item.Value), string(kvs[0].Value))
	}
}

func TestGetReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	_, err := db.Set(kvs[0])
	if err != nil {
		t.Fatalf("Reference error %s", err)
	}
	ref, err := db.Reference(&schema.ReferenceOptions{
		Reference: []byte(`tag`),
		Key:       kvs[0].Key,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Index != 1 {
		t.Fatalf("Reference, expected %v, got %v", 1, ref.Index)
	}
	item, err := db.GetReference(&schema.Key{Key: []byte(`tag`)})
	if err != nil {
		t.Fatalf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Value, kvs[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(item.Value), string(kvs[0].Value))
	}
	item, err = db.GetReference(&schema.Key{Key: []byte(`tag`)})
	if err != nil {
		t.Fatalf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Value, kvs[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(item.Value), string(kvs[0].Value))
	}
}

func TestZAdd(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	_, _ = db.Set(&schema.KeyValue{
		Key:   []byte(`key`),
		Value: []byte(`val`),
	})

	ref, err := db.ZAdd(&schema.ZAddOptions{
		Key:   []byte(`key`),
		Score: &schema.Score{Score: float64(1)},
		Set:   []byte(`mySet`),
	})
	if err != nil {
		t.Fatal(err)
	}

	if ref.Index != 1 {
		t.Fatalf("Reference, expected %v, got %v", 1, ref.Index)
	}
	item, err := db.ZScan(&schema.ZScanOptions{
		Set:     []byte(`mySet`),
		Offset:  []byte(""),
		Limit:   3,
		Reverse: false,
	})
	if err != nil {
		t.Fatalf("Reference  Get error %s", err)
	}

	assert.Equal(t, item.Items[0].Item.Value, []byte(`val`))
}
*/

/*
func TestScan(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.Set(kv[0])
	if err != nil {
		t.Fatalf("set error %s", err)
	}
	ref, err := db.ZAdd(&schema.ZAddOptions{
		Key:   kv[0].Key,
		Score: &schema.Score{Score: float64(3)},
		Set:   kv[0].Value,
	})
	if err != nil {
		t.Fatalf("zadd error %s", err)
	}
	if ref.Index != 1 {
		t.Fatalf("Reference, expected %v, got %v", 1, ref.Index)
	}

	it, err := db.SafeZAdd(&schema.SafeZAddOptions{
		Zopts: &schema.ZAddOptions{
			Key:   kv[0].Key,
			Score: &schema.Score{Score: float64(0)},
			Set:   kv[0].Value,
		},
		RootIndex: &schema.Index{
			Index: 0,
		},
	})
	if err != nil {
		t.Fatalf("SafeZAdd error %s", err)
	}
	if it.InclusionProof.I != 2 {
		t.Fatalf("SafeZAdd index, expected %v, got %v", 2, it.InclusionProof.I)
	}

	item, err := db.Scan(&schema.ScanOptions{
		Offset: nil,
		Deep:   false,
		Limit:  1,
		Prefix: kv[0].Key,
	})

	if err != nil {
		t.Fatalf("ZScanSV  Get error %s", err)
	}
	if !bytes.Equal(item.Items[0].Value, kv[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(kv[0].Value), string(item.Items[0].Value))
	}

	scanItem, err := db.IScan(&schema.IScanOptions{
		PageNumber: 2,
		PageSize:   1,
	})
	if err != nil {
		t.Fatalf("IScan  Get error %s", err)
	}
	// reference contains also the timestamp
	key, _, _ := store.UnwrapZIndexReference(scanItem.Items[0].Value)
	if !bytes.Equal(key, kv[0].Key) {
		t.Fatalf("Reference, expected %v, got %v", string(kv[0].Key), string(scanItem.Items[0].Value))
	}
}
*/

/*

func TestCount(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	root, err := db.CurrentRoot()
	if err != nil {
		t.Error(err)
	}

	kv := []*schema.SafeSetOptions{
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Alberto"),
				Value: []byte("Tomba"),
			},
			RootIndex: &schema.Index{
				Index: root.GetIndex(),
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Jean-Claude"),
				Value: []byte("Killy"),
			},
			RootIndex: &schema.Index{
				Index: root.GetIndex(),
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Franz"),
				Value: []byte("Clamer"),
			},
			RootIndex: &schema.Index{
				Index: root.GetIndex(),
			},
		},
	}

	for _, val := range kv {
		_, err := db.SafeSet(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}

	// Count
	c, err := db.Count(&schema.KeyPrefix{
		Prefix: []byte("Franz"),
	})
	if err != nil {
		t.Fatalf("Error count %s", err)
	}
	if c.Count != 1 {
		t.Fatalf("Error count expected %d got %d", 1, c.Count)
	}

	// CountAll
	// for each key there's an extra entry in the db:
	// 3 entries (with different keys) + 3 extra = 6 entries in total
	countAll := db.CountAll().Count
	if countAll != 6 {
		t.Fatalf("Error CountAll expected %d got %d", 6, countAll)
	}
}
*/

/*
func TestSafeReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	root, err := db.CurrentRoot()
	if err != nil {
		t.Error(err)
	}
	kv := []*schema.SafeSetOptions{
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Alberto"),
				Value: []byte("Tomba"),
			},
			RootIndex: &schema.Index{
				Index: root.GetIndex(),
			},
		},
	}
	for _, val := range kv {
		_, err := db.SafeSet(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}
	_, err = db.SafeReference(&schema.SafeReferenceOptions{
		Ro: &schema.ReferenceOptions{
			Key:       []byte("Alberto"),
			Reference: []byte("Skii"),
		},
		RootIndex: &schema.Index{
			Index: root.GetIndex(),
		},
	})
	if err != nil {
		t.Fatalf("SafeReference Error %s", err)
	}

	_, err = db.SafeReference(&schema.SafeReferenceOptions{
		Ro: &schema.ReferenceOptions{
			Key:       []byte{},
			Reference: []byte{},
		},
	})
	if err == nil {
		t.Fatalf("SafeReference expected error %s", err)
	}
}


func TestDump(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	root, err := db.CurrentRoot()
	if err != nil {
		t.Error(err)
	}

	kvs := []*schema.SafeSetOptions{
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Alberto"),
				Value: []byte("Tomba"),
			},
			RootIndex: &schema.Index{
				Index: root.GetIndex(),
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Jean-Claude"),
				Value: []byte("Killy"),
			},
			RootIndex: &schema.Index{
				Index: root.GetIndex(),
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Franz"),
				Value: []byte("Clamer"),
			},
			RootIndex: &schema.Index{
				Index: root.GetIndex(),
			},
		},
	}
	for _, val := range kvs {
		_, err := db.SafeSet(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}

	dump := &mockImmuService_DumpServer{}
	err = db.Dump(&emptypb.Empty{}, dump)
	require.NoError(t, err)
	require.Less(t, 0, len(dump.results))
}
*/

/*
type mockImmuService_DumpServer struct {
	grpc.ServerStream
	results []*pb.KVList
}

func (_m *mockImmuService_DumpServer) Send(kvs *pb.KVList) error {
	_m.results = append(_m.results, kvs)
	return nil
}
*/

/*
func TestDb_SetBatchAtomicOperations(t *testing.T) {
	db, closer := makeDb()
	defer closer()

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

	_, err := db.ExecAllOps(aOps)

	require.NoError(t, err)
}
*/
