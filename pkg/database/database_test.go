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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/fs"
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

func makeDb(t *testing.T) *db {
	rootPath := t.TempDir()

	options := DefaultOption().WithDBRootPath(rootPath)
	options.storeOpts.WithIndexOptions(options.storeOpts.IndexOpts.WithCompactionThld(2))

	return makeDbWith(t, "db", options)
}

func makeDbWith(t *testing.T, dbName string, opts *Options) *db {
	d, err := NewDB(dbName, &dummyMultidbHandler{}, opts, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	t.Cleanup(func() {
		err := d.Close()
		if !t.Failed() {
			require.NoError(t, err)
		}
	})

	return d.(*db)
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

func (h *dummyMultidbHandler) ListUsers(ctx context.Context) ([]sql.User, error) {
	return nil, sql.ErrNoSupported
}

func (h *dummyMultidbHandler) CreateUser(ctx context.Context, username, password string, permission sql.Permission) error {
	return sql.ErrNoSupported
}

func (h *dummyMultidbHandler) AlterUser(ctx context.Context, username, password string, permission sql.Permission) error {
	return sql.ErrNoSupported
}

func (h *dummyMultidbHandler) DropUser(ctx context.Context, username string) error {
	return sql.ErrNoSupported
}

func (h *dummyMultidbHandler) ExecPreparedStmts(
	ctx context.Context,
	opts *sql.TxOptions,
	stmts []sql.SQLStmt,
	params map[string]interface{}) (ntx *sql.SQLTx, committedTxs []*sql.SQLTx, err error) {
	return nil, nil, sql.ErrNoSupported
}

func TestDefaultDbCreation(t *testing.T) {
	options := DefaultOption().WithDBRootPath(t.TempDir())
	db, err := NewDB("mydb", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	require.Equal(t, options, db.GetOptions())

	defer db.Close()

	n, err := db.TxCount()
	require.NoError(t, err)
	require.Zero(t, n)

	_, err = db.Count(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	res, err := db.CountAll(context.Background())
	require.NoError(t, err)
	require.Zero(t, res.Count)

	dbPath := path.Join(options.GetDBRootPath(), db.GetName())
	require.DirExists(t, dbPath)
}

func TestDbCreationInAlreadyExistentDirectories(t *testing.T) {
	options := DefaultOption().WithDBRootPath(filepath.Join(t.TempDir(), "Paris"))

	err := os.MkdirAll(filepath.Join(options.GetDBRootPath(), "EdithPiaf"), os.ModePerm)
	require.NoError(t, err)

	_, err = NewDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.ErrorContains(t, err, "already exist")
}

func TestDbCreationInInvalidDirectory(t *testing.T) {
	options := DefaultOption().WithDBRootPath("/?")

	_, err := NewDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.Error(t, err)
}

func TestDbCreation(t *testing.T) {
	options := DefaultOption().WithDBRootPath(filepath.Join(t.TempDir(), "Paris"))
	db, err := NewDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	defer db.Close()

	dbPath := path.Join(options.GetDBRootPath(), db.GetName())
	require.DirExists(t, dbPath)
}

func TestOpenWithMissingDBDirectories(t *testing.T) {
	options := DefaultOption().WithDBRootPath(filepath.Join(t.TempDir(), "Paris"))
	_, err := OpenDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.ErrorContains(t, err, "missing database directories")
}

func TestOpenWithIllegalDBName(t *testing.T) {
	options := DefaultOption().WithDBRootPath(filepath.Join(t.TempDir(), "Paris"))
	_, err := OpenDB("", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestOpenDB(t *testing.T) {
	options := DefaultOption().WithDBRootPath(filepath.Join(t.TempDir(), "Paris"))
	db, err := NewDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	db, err = OpenDB("EdithPiaf", nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
}

func TestOpenV1_0_1_DB(t *testing.T) {
	copier := fs.NewStandardCopier()
	dir := filepath.Join(t.TempDir(), "db")
	require.NoError(t, copier.CopyDir("../../test/data_v1.1.0", dir))

	sysOpts := DefaultOption().WithDBRootPath(dir)
	sysDB, err := OpenDB("systemdb", nil, sysOpts, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	dbOpts := DefaultOption().WithDBRootPath(dir)
	db, err := OpenDB("defaultdb", nil, dbOpts, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	err = sysDB.Close()
	require.NoError(t, err)
}

func TestDbSynchronousSet(t *testing.T) {
	db := makeDb(t)

	for _, kv := range kvs {
		_, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{kv}})
		require.NoError(t, err)

		item, err := db.Get(context.Background(), &schema.KeyRequest{Key: kv.Key})
		require.NoError(t, err)
		require.Equal(t, kv.Key, item.Key)
		require.Equal(t, kv.Value, item.Value)
	}
}

func TestDbSetGet(t *testing.T) {
	db := makeDb(t)

	var trustedAlh [sha256.Size]byte
	var trustedIndex uint64

	_, err := db.Set(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.VerifiableGet(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	for i, kv := range kvs[:1] {
		txhdr, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{kv}})
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.Id)

		if i == 0 {
			alh := schema.TxHeaderFromProto(txhdr).Alh()
			copy(trustedAlh[:], alh[:])
			trustedIndex = 1
		}

		keyReq := &schema.KeyRequest{Key: kv.Key, SinceTx: txhdr.Id}

		item, err := db.Get(context.Background(), keyReq)
		require.NoError(t, err)
		require.Equal(t, kv.Key, item.Key)
		require.Equal(t, kv.Value, item.Value)

		_, err = db.Get(context.Background(), &schema.KeyRequest{Key: kv.Key, SinceTx: txhdr.Id, AtTx: txhdr.Id})
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, err = db.Get(context.Background(), &schema.KeyRequest{Key: kv.Key, SinceTx: txhdr.Id + 1})
		require.ErrorIs(t, err, ErrIllegalArguments)

		vitem, err := db.VerifiableGet(context.Background(), &schema.VerifiableGetRequest{
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

	_, err = db.Get(context.Background(), &schema.KeyRequest{Key: []byte{}})
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestDelete(t *testing.T) {
	db := makeDb(t)

	_, err := db.Delete(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.Set(context.Background(), &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   nil,
				Value: []byte("value1"),
			},
		},
	})
	require.ErrorIs(t, err, ErrIllegalArguments)

	hdr, err := db.Set(context.Background(), &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte("key1"),
				Value: []byte("value1"),
			},
		},
	})
	require.NoError(t, err)

	t.Run("deletion with invalid indexing spec should return an error", func(t *testing.T) {
		_, err = db.Delete(context.Background(), &schema.DeleteKeysRequest{
			Keys: [][]byte{
				[]byte("key1"),
			},
			SinceTx: hdr.Id + 1,
		})
		require.ErrorIs(t, err, store.ErrTxNotFound)
	})

	_, err = db.Get(context.Background(), &schema.KeyRequest{
		Key: []byte("key1"),
	})
	require.NoError(t, err)

	hdr, err = db.Delete(context.Background(), &schema.DeleteKeysRequest{
		Keys: [][]byte{
			[]byte("key1"),
		},
	})
	require.NoError(t, err)

	_, err = db.Get(context.Background(), &schema.KeyRequest{
		Key: []byte("key1"),
	})
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	_, err = db.VerifiableGet(context.Background(), &schema.VerifiableGetRequest{
		KeyRequest: &schema.KeyRequest{
			Key:  []byte("key1"),
			AtTx: hdr.Id,
		},
	})
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	tx, err := db.TxByID(context.Background(), &schema.TxRequest{
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
	db := makeDb(t)

	for ind, val := range kvs {
		txhdr, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
		require.Equal(t, uint64(ind+1), txhdr.Id)

		time.Sleep(1 * time.Second)

		state, err := db.CurrentState()
		require.NoError(t, err)
		require.Equal(t, uint64(ind+1), state.TxId)
	}
}

func TestSafeSetGet(t *testing.T) {
	db := makeDb(t)

	state, err := db.CurrentState()
	require.NoError(t, err)

	_, err = db.VerifiableSet(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.VerifiableSet(context.Background(), &schema.VerifiableSetRequest{
		SetRequest: &schema.SetRequest{
			KVs: []*schema.KeyValue{
				{
					Key:   []byte("Alberto"),
					Value: []byte("Tomba"),
				},
			},
		},
		ProveSinceTx: 1,
	})
	require.ErrorIs(t, err, ErrIllegalState)

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
		vtx, err := db.VerifiableSet(context.Background(), val)
		require.NoError(t, err)
		require.NotNil(t, vtx)

		vit, err := db.VerifiableGet(context.Background(), &schema.VerifiableGetRequest{
			KeyRequest: &schema.KeyRequest{
				Key:     val.SetRequest.KVs[0].Key,
				SinceTx: vtx.Tx.Header.Id,
			},
		})
		require.NoError(t, err)
		require.Equal(t, uint64(ind+1), vit.Entry.Tx)
	}
}

func TestSetGetAll(t *testing.T) {
	db := makeDb(t)

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

	txhdr, err := db.Set(context.Background(), &schema.SetRequest{KVs: kvs})
	require.NoError(t, err)
	require.Equal(t, uint64(1), txhdr.Id)

	itList, err := db.GetAll(context.Background(), &schema.KeyListRequest{
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
	db := makeDb(t)

	_, err := db.TxByID(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	txhdr1, err := db.Set(context.Background(), &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{Key: []byte("key0"), Value: []byte("value0")},
		},
	})
	require.NoError(t, err)

	txhdr2, err := db.ExecAll(context.Background(), &schema.ExecAllRequest{
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

	_, _, err = db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: "CREATE TABLE mytable(id INTEGER AUTO_INCREMENT, PRIMARY KEY id)"})
	require.NoError(t, err)

	_, ctx1, err := db.SQLExec(context.Background(), nil, &schema.SQLExecRequest{Sql: "INSERT INTO mytable() VALUES ()"})
	require.NoError(t, err)
	require.Len(t, ctx1, 1)

	txhdr3 := ctx1[0].TxHeader()

	t.Run("values should not be resolved but digests returned in entries field", func(t *testing.T) {
		tx, err := db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr1.Id})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 1)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			require.Len(t, e.Value, 0)
		}

		tx, err = db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr2.Id})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Len(t, tx.Entries, 3)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			require.Len(t, e.Value, 0)
		}

		tx, err = db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr3.ID})
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
		tx, err := db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr1.Id, EntriesSpec: &schema.EntriesSpec{
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

		tx, err = db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{
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

		tx, err = db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr3.ID, EntriesSpec: &schema.EntriesSpec{
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
		tx, err := db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr1.Id, EntriesSpec: &schema.EntriesSpec{}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		tx, err = db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		tx, err = db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr3.ID, EntriesSpec: &schema.EntriesSpec{}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)
	})

	t.Run("no entries should be returned if explicitly excluded", func(t *testing.T) {
		tx, err := db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr1.Id, EntriesSpec: &schema.EntriesSpec{
			KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		tx, err = db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{
			KvEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
			ZEntriesSpec:  &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		tx, err = db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr3.ID, EntriesSpec: &schema.EntriesSpec{
			SqlEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_EXCLUDE},
		}})
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)
	})

	t.Run("raw entries should be returned", func(t *testing.T) {
		tx, err := db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr1.Id, EntriesSpec: &schema.EntriesSpec{
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

		tx, err = db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{
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

		tx, err = db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr3.ID, EntriesSpec: &schema.EntriesSpec{
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
		tx, err := db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{
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
		tx, err := db.TxByID(context.Background(), &schema.TxRequest{
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
		tx, err := db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr2.Id, EntriesSpec: &schema.EntriesSpec{
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
		tx, err := db.TxByID(context.Background(), &schema.TxRequest{
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
		_, err := db.TxByID(context.Background(), &schema.TxRequest{Tx: txhdr3.ID, EntriesSpec: &schema.EntriesSpec{
			SqlEntriesSpec: &schema.EntryTypeSpec{Action: schema.EntryTypeAction_RESOLVE},
		}})
		require.ErrorIs(t, err, ErrIllegalArguments)
	})
}

func TestVerifiableTxByID(t *testing.T) {
	db := makeDb(t)

	_, err := db.VerifiableTxByID(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	var txhdr *schema.TxHeader

	for _, val := range kvs {
		txhdr, err = db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
	}

	t.Run("values should be returned", func(t *testing.T) {
		vtx, err := db.VerifiableTxByID(context.Background(), &schema.VerifiableTxRequest{
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
		vtx, err := db.VerifiableTxByID(context.Background(), &schema.VerifiableTxRequest{
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
	db := makeDb(t)

	db.maxResultSize = len(kvs)

	initialState, err := db.CurrentState()
	require.NoError(t, err)

	for _, val := range kvs {
		_, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
	}

	_, err = db.TxScan(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.TxScan(context.Background(), &schema.TxScanRequest{
		InitialTx: 0,
	})
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.TxScan(context.Background(), &schema.TxScanRequest{
		InitialTx: 1,
		Limit:     uint32(db.MaxResultSize() + 1),
	})
	require.ErrorIs(t, err, ErrResultSizeLimitExceeded)

	t.Run("values should be returned reaching result size limit", func(t *testing.T) {
		txList, err := db.TxScan(context.Background(), &schema.TxScanRequest{
			InitialTx: initialState.TxId + 1,
			EntriesSpec: &schema.EntriesSpec{
				KvEntriesSpec: &schema.EntryTypeSpec{
					Action: schema.EntryTypeAction_RAW_VALUE,
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, txList.Txs, len(kvs))

		for i := 0; i < len(kvs); i++ {
			require.Equal(t, kvs[i].Key, TrimPrefix(txList.Txs[i].Entries[0].Key))

			hval := sha256.Sum256(txList.Txs[i].Entries[0].Value)
			require.Equal(t, txList.Txs[i].Entries[0].HValue, hval[:])
		}
	})

	t.Run("values should be returned without reaching result size limit", func(t *testing.T) {
		limit := db.MaxResultSize() - 1

		txList, err := db.TxScan(context.Background(), &schema.TxScanRequest{
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
		txList, err := db.TxScan(context.Background(), &schema.TxScanRequest{
			InitialTx: initialState.TxId + 1,
		})
		require.NoError(t, err)
		require.Len(t, txList.Txs, len(kvs))

		for i := 0; i < len(kvs); i++ {
			require.Equal(t, kvs[i].Key, TrimPrefix(txList.Txs[i].Entries[0].Key))
			require.Len(t, txList.Txs[i].Entries[0].Value, 0)
		}
	})
}

func TestHistory(t *testing.T) {
	db := makeDb(t)

	db.maxResultSize = 2

	var lastTx uint64

	for _, val := range kvs {
		_, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
	}

	err := db.FlushIndex(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = db.FlushIndex(&schema.FlushIndexRequest{CleanupPercentage: 100, Synced: true})
	require.NoError(t, err)

	_, err = db.Delete(context.Background(), &schema.DeleteKeysRequest{Keys: [][]byte{kvs[0].Key}})
	require.NoError(t, err)

	meta, err := db.Set(context.Background(), &schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   kvs[0].Key,
			Value: kvs[0].Value,
		}},
	})
	require.NoError(t, err)
	lastTx = meta.Id

	time.Sleep(1 * time.Millisecond)

	_, err = db.History(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.History(context.Background(), &schema.HistoryRequest{
		Key:     kvs[0].Key,
		SinceTx: lastTx,
		Limit:   int32(db.MaxResultSize() + 1),
	})
	require.ErrorIs(t, err, ErrResultSizeLimitExceeded)

	inc, err := db.History(context.Background(), &schema.HistoryRequest{
		Key:     kvs[0].Key,
		SinceTx: lastTx,
	})
	require.NoError(t, err)

	for i, val := range inc.Entries {
		require.Equal(t, kvs[0].Key, val.Key)
		if val.GetMetadata().GetDeleted() {
			require.Empty(t, val.Value)
		} else {
			require.Equal(t, kvs[0].Value, val.Value)
		}
		require.EqualValues(t, i+1, val.Revision)
	}

	dec, err := db.History(context.Background(), &schema.HistoryRequest{
		Key:     kvs[0].Key,
		SinceTx: lastTx,
		Desc:    true,
	})
	require.NoError(t, err)

	for i, val := range dec.Entries {
		require.Equal(t, kvs[0].Key, val.Key)
		if val.GetMetadata().GetDeleted() {
			require.Empty(t, val.Value)
		} else {
			require.Equal(t, kvs[0].Value, val.Value)
		}
		require.EqualValues(t, 3-i, val.Revision)
	}

	inc, err = db.History(context.Background(), &schema.HistoryRequest{
		Key:     kvs[0].Key,
		Offset:  uint64(len(kvs) + 1),
		SinceTx: lastTx,
	})
	require.NoError(t, err)
	require.Empty(t, inc.Entries)
}

func TestPreconditionedSet(t *testing.T) {
	db := makeDb(t)

	_, err := db.Set(context.Background(), &schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key-no-preconditions"),
			Value: []byte("value"),
		}},
	})
	require.NoError(t, err, "could not set a value without preconditions")

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	tx1, err := db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	tx2, err := db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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

	_, err = db.Set(context.Background(), &schema.SetRequest{
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
	db := makeDb(t)

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
				_, err := db.Set(context.Background(), &schema.SetRequest{
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
				_, err := db.Set(context.Background(), &schema.SetRequest{
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

			tx, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{
				Key:   []byte(`key`),
				Value: []byte(`base value`),
			}}})
			require.NoError(t, err)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.Set(context.Background(), &schema.SetRequest{
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
				_, err := db.SetReference(context.Background(), &schema.ReferenceRequest{
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
				_, err := db.SetReference(context.Background(), &schema.ReferenceRequest{
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

			tx, err := db.SetReference(context.Background(), &schema.ReferenceRequest{
				Key:           []byte(`reference`),
				ReferencedKey: []byte(`key`),
			})
			require.NoError(t, err)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.SetReference(context.Background(), &schema.ReferenceRequest{
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
				_, err := db.ExecAll(context.Background(), &schema.ExecAllRequest{
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
				_, err := db.ExecAll(context.Background(), &schema.ExecAllRequest{
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

			tx, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{
				Key:   []byte(`key-ea`),
				Value: []byte(`base value`),
			}}})
			require.NoError(t, err)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.ExecAll(context.Background(), &schema.ExecAllRequest{
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
				_, err := db.ExecAll(context.Background(), &schema.ExecAllRequest{
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
				_, err := db.ExecAll(context.Background(), &schema.ExecAllRequest{
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

			tx, err := db.SetReference(context.Background(), &schema.ReferenceRequest{
				Key:           []byte(`reference-ea`),
				ReferencedKey: []byte(`key-ea`),
			})
			require.NoError(t, err)

			failCount, successCount, badError := runInParallel(func(i int) error {
				_, err := db.ExecAll(context.Background(), &schema.ExecAllRequest{
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
	db := makeDb(t)

	const histCount = 10

	for i := 0; i < histCount; i++ {
		_, err := db.Set(context.Background(), &schema.SetRequest{
			KVs: []*schema.KeyValue{{
				Key:   []byte("key"),
				Value: []byte(fmt.Sprintf("value%d", i)),
			}},
		})
		require.NoError(t, err)
	}

	t.Run("get the most recent value if no revision is specified", func(t *testing.T) {
		k, err := db.Get(context.Background(), &schema.KeyRequest{
			Key: []byte("key"),
		})
		require.NoError(t, err)
		require.Equal(t, []byte("value9"), k.Value)
	})

	t.Run("get correct values for positive revision numbers", func(t *testing.T) {
		for i := 0; i < histCount; i++ {
			k, err := db.Get(context.Background(), &schema.KeyRequest{
				Key:        []byte("key"),
				AtRevision: int64(i) + 1,
			})
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("value%d", i)), k.Value)
		}
	})

	t.Run("get correct error if positive revision number is to high", func(t *testing.T) {
		_, err := db.Get(context.Background(), &schema.KeyRequest{
			Key:        []byte("key"),
			AtRevision: 11,
		})
		require.ErrorIs(t, err, ErrInvalidRevision)
	})

	t.Run("get correct error if maximum integer value is used for the revision number", func(t *testing.T) {
		_, err := db.Get(context.Background(), &schema.KeyRequest{
			Key:        []byte("key"),
			AtRevision: math.MaxInt64,
		})
		require.ErrorIs(t, err, ErrInvalidRevision)
	})

	t.Run("get correct values for negative revision numbers", func(t *testing.T) {
		for i := 1; i < histCount; i++ {
			k, err := db.Get(context.Background(), &schema.KeyRequest{
				Key:        []byte("key"),
				AtRevision: -int64(i),
			})
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("value%d", 9-i)), k.Value)
		}
	})

	t.Run("get correct error if negative revision number is to low", func(t *testing.T) {
		_, err := db.Get(context.Background(), &schema.KeyRequest{
			Key:        []byte("key"),
			AtRevision: -10,
		})
		require.ErrorIs(t, err, ErrInvalidRevision)
	})

	t.Run("get correct error if minimum negative revision number is used", func(t *testing.T) {
		_, err := db.Get(context.Background(), &schema.KeyRequest{
			Key:        []byte("key"),
			AtRevision: math.MinInt64,
		})
		require.ErrorIs(t, err, ErrInvalidRevision)
	})

	t.Run("get correct error if non-existing key is specified", func(t *testing.T) {
		_, err := db.Get(context.Background(), &schema.KeyRequest{
			Key:        []byte("non-existing-key"),
			AtRevision: 1,
		})
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	})

	t.Run("get correct error if expired entry is fetched through revision", func(t *testing.T) {
		_, err := db.Set(context.Background(), &schema.SetRequest{
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

		_, err = db.Set(context.Background(), &schema.SetRequest{
			KVs: []*schema.KeyValue{{
				Key:   []byte("exp-key"),
				Value: []byte("not-expired-value"),
			}},
		})
		require.NoError(t, err)

		entry, err := db.Get(context.Background(), &schema.KeyRequest{
			Key: []byte("exp-key"),
		})
		require.NoError(t, err)
		require.Equal(t, []byte("not-expired-value"), entry.Value)
		require.EqualValues(t, 2, entry.Revision)

		_, err = db.Get(context.Background(), &schema.KeyRequest{
			Key:        []byte("exp-key"),
			AtRevision: 1,
		})
		require.ErrorIs(t, err, store.ErrExpiredEntry)
	})
}

func TestRevisionGetConsistency(t *testing.T) {
	db := makeDb(t)

	var keyTxId uint64

	// Repeat the test for different revision numbers
	for i := 0; i < 10; i++ {

		keyTx, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{
			Key:   []byte("key"),
			Value: []byte(fmt.Sprintf("value_%d", i)),
		}}})
		require.NoError(t, err)

		if i == 0 {
			keyTxId = keyTx.Id
		}

		_, err = db.SetReference(context.Background(), &schema.ReferenceRequest{
			Key:           []byte("reference_unbound"),
			ReferencedKey: []byte("key"),
		})
		require.NoError(t, err)

		_, err = db.SetReference(context.Background(), &schema.ReferenceRequest{
			Key:           []byte("reference_bound"),
			ReferencedKey: []byte("key"),
			AtTx:          keyTxId,
			BoundRef:      true,
		})
		require.NoError(t, err)

		t.Run("get and scan should return consistent revision on direct entries", func(t *testing.T) {
			entryFromGet, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte("key")})
			require.NoError(t, err)

			scanResults, err := db.Scan(context.Background(), &schema.ScanRequest{Prefix: []byte("key")})
			require.NoError(t, err)
			require.Len(t, scanResults.Entries, 1)

			require.EqualValues(t, i+1, entryFromGet.Revision)
			require.EqualValues(t, i+1, scanResults.Entries[0].Revision)
		})

		t.Run("get and scan should return consistent revision on unbound references", func(t *testing.T) {
			entryFromGet, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte("reference_unbound")})
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("value_%d", i)), entryFromGet.Value)

			scanResults, err := db.Scan(context.Background(), &schema.ScanRequest{Prefix: []byte("reference_unbound")})
			require.NoError(t, err)
			require.Len(t, scanResults.Entries, 1)
			require.Equal(t, []byte(fmt.Sprintf("value_%d", i)), scanResults.Entries[0].Value)

			require.EqualValues(t, i+1, entryFromGet.Revision)
			require.EqualValues(t, i+1, scanResults.Entries[0].Revision)
			require.EqualValues(t, i+1, entryFromGet.ReferencedBy.Revision)
			require.EqualValues(t, i+1, scanResults.Entries[0].ReferencedBy.Revision)
		})

		t.Run("get and scan should return consistent revision on bound references", func(t *testing.T) {
			entryFromGet, err := db.Get(context.Background(), &schema.KeyRequest{Key: []byte("reference_bound")})
			require.NoError(t, err)
			require.Equal(t, []byte("value_0"), entryFromGet.Value)

			scanResults, err := db.Scan(context.Background(), &schema.ScanRequest{Prefix: []byte("reference_bound")})
			require.NoError(t, err)
			require.Len(t, scanResults.Entries, 1)
			require.Equal(t, []byte("value_0"), scanResults.Entries[0].Value)

			require.EqualValues(t, 0, entryFromGet.Revision)
			require.EqualValues(t, 0, scanResults.Entries[0].Revision)

			require.EqualValues(t, i+1, entryFromGet.ReferencedBy.Revision)
			require.EqualValues(t, i+1, scanResults.Entries[0].ReferencedBy.Revision)
		})
	}
}

/*
func TestReference(t *testing.T) {
db := makeDb(t)
	_, err := db.Set(context.Background(), kvs[0])
	if err != nil {
		t.Fatalf("Reference error %s", err)
	}
	ref, err := db.Reference(&schema.ReferenceOptions{
		Reference: []byte(`tag`),
		Key:       kvs[0].Key,
	})
	require.NoError(t, err)
	if ref.Index != 1 {
		t.Fatalf("Reference, expected %v, got %v", 1, ref.Index)
	}
	item, err := db.Get(context.Background(), &schema.Key{Key: []byte(`tag`)})
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
db := makeDb(t)
	_, err := db.Set(context.Background(), kvs[0])
	if err != nil {
		t.Fatalf("Reference error %s", err)
	}
	ref, err := db.Reference(&schema.ReferenceOptions{
		Reference: []byte(`tag`),
		Key:       kvs[0].Key,
	})
	require.NoError(t, err)
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
db := makeDb(t)
	_, _ = db.Set(context.Background(), &schema.KeyValue{
		Key:   []byte(`key`),
		Value: []byte(`val`),
	})

	ref, err := db.ZAdd(&schema.ZAddOptions{
		Key:   []byte(`key`),
		Score: &schema.Score{Score: float64(1)},
		Set:   []byte(`mySet`),
	})
	require.NoError(t, err)

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
db := makeDb(t)

	_, err := db.Set(context.Background(), kv[0])
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

	item, err := db.Scan(context.Background(), &schema.ScanOptions{
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
db := makeDb(t)

	root, err := db.CurrentRoot()
	require.NoError(t, err)

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
	c, err := db.Count(context.Background(), &schema.KeyPrefix{
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
db := makeDb(t)
	root, err := db.CurrentRoot()
	require.NoError(t, err)
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
db := makeDb(t)

	root, err := db.CurrentRoot()
	require.NoError(t, err)

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
db := makeDb(t)

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

func Test_database_truncate(t *testing.T) {
	options := DefaultOption().WithDBRootPath(t.TempDir())
	options.storeOpts.
		WithEmbeddedValues(false).
		WithPreallocFiles(false).
		WithIndexOptions(options.storeOpts.IndexOpts.WithCompactionThld(2)).
		WithFileSize(8).
		WithVLogCacheSize(0)

	db := makeDbWith(t, "db", options)

	var queryTime time.Time

	for i := 0; i <= 20; i++ {
		kv := &schema.KeyValue{
			Key:   []byte(fmt.Sprintf("key_%d", i)),
			Value: []byte(fmt.Sprintf("val_%d", i)),
		}
		_, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{kv}})
		require.NoError(t, err)

		if i == 10 {
			queryTime = time.Now()
		}
	}

	c := NewVlogTruncator(db)

	hdr, err := c.Plan(context.Background(), queryTime)
	require.NoError(t, err)
	require.LessOrEqual(t, time.Unix(hdr.Ts, 0), queryTime)

	err = c.TruncateUptoTx(context.Background(), hdr.ID)
	require.NoError(t, err)

	for i := hdr.ID; i <= 20; i++ {
		tx := store.NewTx(db.st.MaxTxEntries(), db.st.MaxKeyLen())

		err = db.st.ReadTx(i, false, tx)
		require.NoError(t, err)

		for _, e := range tx.Entries() {
			_, err := db.st.ReadValue(e)
			require.NoError(t, err)
		}
	}

	// ensure that the earlier txs are truncated
	for i := uint64(5); i > 0; i-- {
		tx := store.NewTx(db.st.MaxTxEntries(), db.st.MaxKeyLen())

		err = db.st.ReadTx(i, false, tx)
		require.NoError(t, err)

		for _, e := range tx.Entries() {
			_, err := db.st.ReadValue(e)
			require.Error(t, err)
		}
	}

}
