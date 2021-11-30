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
package database

import (
	"crypto/sha256"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

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

func makeDb() (DB, func()) {
	rootPath := "data_" + strconv.FormatInt(time.Now().UnixNano(), 10)

	options := DefaultOption().WithDBRootPath(rootPath).WithDBName("db").WithCorruptionChecker(false)
	options.storeOpts.WithIndexOptions(options.storeOpts.IndexOpts.WithCompactionThld(0))

	return makeDbWith(options)
}

func makeDbWith(opts *Options) (DB, func()) {
	db, err := NewDB(opts, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		log.Fatalf("Error creating Db instance %s", err)
	}

	return db, func() {
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}

		if err := os.RemoveAll(db.GetOptions().dbRootPath); err != nil {
			log.Fatal(err)
		}
	}
}

func TestDefaultDbCreation(t *testing.T) {
	options := DefaultOption()
	db, err := NewDB(options, logger.NewSimpleLogger("immudb ", os.Stderr))
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

	dbPath := path.Join(options.GetDBRootPath(), options.GetDBName())
	if _, err = os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Db dir not created")
	}

	_, err = os.Stat(path.Join(options.GetDBRootPath()))
	if os.IsNotExist(err) {
		t.Fatalf("Data dir not created")
	}
}

func TestDbCreationInAlreadyExistentDirectories(t *testing.T) {
	options := DefaultOption().WithDBRootPath("Paris").WithDBName("EdithPiaf")
	defer os.RemoveAll(options.GetDBRootPath())

	err := os.MkdirAll(options.GetDBRootPath(), os.ModePerm)
	require.NoError(t, err)

	err = os.MkdirAll(filepath.Join(options.GetDBRootPath(), options.GetDBName()), os.ModePerm)
	require.NoError(t, err)

	_, err = NewDB(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.Error(t, err)
}

func TestDbCreationInInvalidDirectory(t *testing.T) {
	options := DefaultOption().WithDBRootPath("/?").WithDBName("EdithPiaf")
	defer os.RemoveAll(options.GetDBRootPath())

	_, err := NewDB(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.Error(t, err)
}

func TestDbCreation(t *testing.T) {
	options := DefaultOption().WithDBName("EdithPiaf").WithDBRootPath("Paris")
	db, err := NewDB(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Fatalf("Error creating Db instance %s", err)
	}

	defer func() {
		db.Close()
		time.Sleep(1 * time.Second)
		os.RemoveAll(options.GetDBRootPath())
	}()

	dbPath := path.Join(options.GetDBRootPath(), options.GetDBName())
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
	_, err := OpenDB(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.Error(t, err)
}

func TestOpenDB(t *testing.T) {
	options := DefaultOption().WithDBName("EdithPiaf").WithDBRootPath("Paris")
	db, err := NewDB(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Fatalf("Error creating Db instance %s", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Error closing store %s", err)
	}

	db, err = OpenDB(options, logger.NewSimpleLogger("immudb ", os.Stderr))
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

	sysOpts := DefaultOption().WithDBName("systemdb").WithDBRootPath("./data_v1.1.0")
	sysDB, err := OpenDB(sysOpts, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	dbOpts := DefaultOption().WithDBName("defaultdb").WithDBRootPath("./data_v1.1.0")
	db, err := OpenDB(dbOpts, logger.NewSimpleLogger("immudb ", os.Stderr))
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
		require.Equal(t, ErrIllegalArguments, err)

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

	_, err = db.Delete(&schema.DeleteKeysRequest{
		Keys: [][]byte{
			[]byte("key1"),
		},
	})
	require.NoError(t, err)

	_, err = db.Get(&schema.KeyRequest{
		Key: []byte("key1"),
	})
	require.ErrorIs(t, err, store.ErrKeyNotFound)
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

	for ind, val := range kvs {
		txhdr, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
		require.Equal(t, uint64(ind+2), txhdr.Id)
	}

	_, err = db.TxByID(&schema.TxRequest{Tx: uint64(1)})
	require.NoError(t, err)
}

func TestVerifiableTxByID(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.VerifiableTxByID(nil)
	require.Error(t, ErrIllegalArguments, err)

	for _, val := range kvs {
		_, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
	}

	_, err = db.VerifiableTxByID(&schema.VerifiableTxRequest{
		Tx:           uint64(1),
		ProveSinceTx: 0,
	})
	require.NoError(t, err)
}

func TestTxScan(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for _, val := range kvs {
		_, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)
	}

	_, err := db.TxScan(nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.TxScan(&schema.TxScanRequest{
		InitialTx: 0,
	})
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.TxScan(&schema.TxScanRequest{
		InitialTx: 1,
		Limit:     MaxKeyScanLimit + 1,
	})
	require.Equal(t, ErrMaxKeyScanLimitExceeded, err)

	txList, err := db.TxScan(&schema.TxScanRequest{
		InitialTx: 1,
	})
	require.NoError(t, err)
	require.Len(t, txList.Txs, len(kvs)+1)

	for i := 0; i < len(kvs); i++ {
		require.Equal(t, kvs[i].Key, TrimPrefix(txList.Txs[i+1].Entries[0].Key))
	}
}

func TestHistory(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	var lastTx uint64

	for _, val := range kvs {
		meta, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: val.Key, Value: val.Value}}})
		require.NoError(t, err)

		lastTx = meta.Id
	}

	time.Sleep(1 * time.Millisecond)

	_, err := db.History(nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.History(&schema.HistoryRequest{
		Key:     kvs[0].Key,
		SinceTx: lastTx,
		Limit:   MaxKeyScanLimit + 1,
	})
	require.Equal(t, ErrMaxKeyScanLimitExceeded, err)

	inc, err := db.History(&schema.HistoryRequest{
		Key:     kvs[0].Key,
		SinceTx: lastTx,
	})
	require.NoError(t, err)

	for _, val := range inc.Entries {
		require.Equal(t, kvs[0].Key, val.Key)
		require.Equal(t, kvs[0].Value, val.Value)
	}

	inc, err = db.History(&schema.HistoryRequest{
		Key:     kvs[0].Key,
		Offset:  uint64(len(kvs) + 1),
		SinceTx: lastTx,
	})
	require.NoError(t, err)
	require.Empty(t, inc.Entries)
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
