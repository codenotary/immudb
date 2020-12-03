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

package server

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/store"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var Skv = &schema.SKVList{
	SKVs: []*schema.StructuredKeyValue{
		{
			Key: []byte("Alberto"),
			Value: &schema.Content{
				Timestamp: uint64(1257894000),
				Payload:   []byte("Tomba"),
			},
		},
		{
			Key: []byte("Jean-Claude"),
			Value: &schema.Content{
				Timestamp: uint64(1257894001),
				Payload:   []byte("Killy"),
			},
		},
		{
			Key: []byte("Franz"),
			Value: &schema.Content{
				Timestamp: uint64(1257894002),
				Payload:   []byte("Clamer"),
			},
		},
	},
}

var kv = []*schema.KeyValue{
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

func makeDb() (*Db, func()) {
	dbName := "EdithPiaf" + strconv.FormatInt(time.Now().UnixNano(), 10)
	options := DefaultOption().WithDbName(dbName).WithInMemoryStore(true).WithCorruptionChecker(false)
	d, err := NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		log.Fatalf("Error creating Db instance %s", err)
	}
	return d, func() {
		if err := d.Store.Close(); err != nil {
			log.Fatal(err)
		}
		if err := os.RemoveAll(options.GetDbName()); err != nil {
			log.Fatal(err)
		}
	}
}

func TestDefaultDbCreation(t *testing.T) {
	options := DefaultOption()
	db, err := NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Fatalf("Error creating Db instance %s", err)
	}
	defer func() {
		db.Store.Close()
		time.Sleep(1 * time.Second)
		os.RemoveAll(options.GetDbRootPath())
	}()
	dbPath := path.Join(options.GetDbRootPath(), options.GetDbName())
	if _, err = os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Db dir not created")
	}

	_, err = os.Stat(path.Join(options.GetDbRootPath()))
	if os.IsNotExist(err) {
		t.Fatalf("Data dir not created")
	}
}

func TestDbCreationInAlreadyExistentDirectories(t *testing.T) {
	options := DefaultOption().WithDbRootPath("Paris").WithDbName("EdithPiaf")
	defer os.RemoveAll(options.GetDbRootPath())

	err := os.MkdirAll(options.GetDbRootPath(), os.ModePerm)
	require.NoError(t, err)

	err = os.MkdirAll(filepath.Join(options.GetDbRootPath(), options.GetDbName()), os.ModePerm)
	require.NoError(t, err)

	_, err = NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.Error(t, err)
}

func TestDbCreationInInvalidDirectory(t *testing.T) {
	options := DefaultOption().WithDbRootPath("/?").WithDbName("EdithPiaf")
	defer os.RemoveAll(options.GetDbRootPath())

	_, err := NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.Error(t, err)
}

func TestDbCreation(t *testing.T) {
	options := DefaultOption().WithDbName("EdithPiaf").WithDbRootPath("Paris")
	db, err := NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Fatalf("Error creating Db instance %s", err)
	}
	defer func() {
		db.Store.Close()
		time.Sleep(1 * time.Second)
		os.RemoveAll(options.GetDbRootPath())
	}()

	dbPath := path.Join(options.GetDbRootPath(), options.GetDbName())
	if _, err = os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Db dir not created")
	}

	_, err = os.Stat(options.GetDbRootPath())
	if os.IsNotExist(err) {
		t.Fatalf("Data dir not created")
	}
}

func TestOpenWithMissingDBDirectories(t *testing.T) {
	options := DefaultOption().WithDbRootPath("Paris")
	_, err := OpenDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.Error(t, err)
}

func TestOpenDb(t *testing.T) {
	options := DefaultOption().WithDbName("EdithPiaf").WithDbRootPath("Paris")
	db, err := NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Fatalf("Error creating Db instance %s", err)
	}
	err = db.Store.Close()
	if err != nil {
		t.Fatalf("Error closing store %s", err)
	}

	db, err = OpenDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Fatalf("Error opening database %s", err)
	}
	db.Store.Close()
	time.Sleep(1 * time.Second)
	os.RemoveAll(options.GetDbRootPath())
}

func TestDbSetGet(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range kv {
		it, err := db.Set(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Fatalf("index error expecting %v got %v", ind, it.GetIndex())
		}
		k := &schema.Key{
			Key: []byte(val.Key),
		}
		item, err := db.Get(k)
		if err != nil {
			t.Fatalf("Error reading key %s", err)
		}
		if !bytes.Equal(item.Key, val.Key) {
			t.Fatalf("Inserted CurrentOffset not equal to read CurrentOffset")
		}
		if !bytes.Equal(item.Value, val.Value) {
			t.Fatalf("Inserted value not equal to read value")
		}
	}

	k := &schema.Key{
		Key: []byte{},
	}
	_, err := db.Get(k)
	if err == nil {
		t.Fatalf("Get error expected")
	}
}

func TestCurrentRoot(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range kv {
		it, err := db.Set(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Fatalf("index error expecting %v got %v", ind, it.GetIndex())
		}
		time.Sleep(1 * time.Second)
		r, err := db.CurrentRoot(&emptypb.Empty{})
		if err != nil {
			t.Fatalf("Error getting current root %s", err)
		}
		if r.GetIndex() != uint64(ind) {
			t.Fatalf("root error expecting %v got %v", ind, r.GetIndex())
		}
	}
}

func TestSafeSetGet(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	root, err := db.CurrentRoot(&emptypb.Empty{})
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
	for ind, val := range kv {
		proof, err := db.SafeSet(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
		if proof == nil {
			t.Fatalf("Nil proof after SafeSet")
		}
		if proof.GetIndex() != uint64(ind) {
			t.Fatalf("SafeSet proof index error, expected %d, got %d", uint64(ind), proof.GetIndex())
		}

		it, err := db.SafeGet(&schema.SafeGetOptions{
			Key: val.Kv.Key,
		})
		if err != nil {
			t.Fatal(err)
		}
		if it.GetItem().GetIndex() != uint64(ind) {
			t.Fatalf("SafeGet index error, expected %d, got %d", uint64(ind), it.GetItem().GetIndex())
		}
	}
}

func TestSetGetBatch(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	Skv := &schema.KVList{
		KVs: []*schema.KeyValue{
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
		},
	}
	ind, err := db.SetBatch(Skv)
	if err != nil {
		t.Fatalf("Error Inserting to db %s", err)
	}
	if ind == nil {
		t.Fatalf("Nil index after Setbatch")
	}
	if ind.GetIndex() != 2 {
		t.Fatalf("SafeSet proof index error, expected %d, got %d", 2, ind.GetIndex())
	}

	itList, err := db.GetBatch(&schema.KeyList{
		Keys: []*schema.Key{
			{
				Key: []byte("Alberto"),
			},
			{
				Key: []byte("Jean-Claude"),
			},
			{
				Key: []byte("Franz"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for ind, val := range itList.Items {
		if !bytes.Equal(val.Value, Skv.KVs[ind].Value) {
			t.Fatalf("BatchSet value not equal to BatchGet value, expected %s, got %s", string(Skv.KVs[ind].Value), string(val.Value))
		}
	}
}

func TestInclusion(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range kv {
		it, err := db.Set(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Fatalf("index error expecting %v got %v", ind, it.GetIndex())
		}
	}
	ind := uint64(1)
	//TODO find a better way without sleep
	time.Sleep(2 * time.Second)
	inc, err := db.Inclusion(&schema.Index{Index: ind})
	if err != nil {
		t.Fatalf("Error Inserting to db %s", err)
	}
	if inc.Index != ind {
		t.Fatalf("Inclusion, expected %d, got %d", inc.Index, ind)
	}
}

func TestConsintency(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range kv {
		it, err := db.Set(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Fatalf("index error expecting %v got %v", ind, it.GetIndex())
		}
	}
	time.Sleep(1 * time.Second)
	ind := uint64(1)
	inc, err := db.Consistency(&schema.Index{Index: ind})
	if err != nil {
		t.Fatalf("Error Inserting to db %s", err)
	}
	if inc.First != ind {
		t.Fatalf("Consistency, expected %d, got %d", inc.First, ind)
	}
}

func TestByIndex(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range kv {
		it, err := db.Set(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Fatalf("index error expecting %v got %v", ind, it.GetIndex())
		}
	}
	time.Sleep(1 * time.Second)
	ind := uint64(1)
	inc, err := db.ByIndex(&schema.Index{Index: ind})
	if err != nil {
		t.Fatalf("Error Inserting to db %s", err)
	}
	if !bytes.Equal(inc.Value, kv[ind].Value) {
		t.Fatalf("ByIndex, expected %s, got %d", kv[ind].Value, inc.Value)
	}
}

func TestBySafeIndex(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	for _, val := range kv {
		_, err := db.Set(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}
	time.Sleep(1 * time.Second)
	ind := uint64(1)
	inc, err := db.BySafeIndex(&schema.SafeIndexOptions{Index: ind})
	if err != nil {
		t.Fatalf("Error Inserting to db %s", err)
	}
	if inc.Item.Index != ind {
		t.Fatalf("ByIndexSV, expected %d, got %d", ind, inc.Item.Index)
	}
}

func TestHistory(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	for _, val := range kv {
		_, err := db.Set(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}
	_, err := db.Set(kv[0])
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	inc, err := db.History(&schema.HistoryOptions{
		Key: kv[0].Key,
	})
	if err != nil {
		t.Fatalf("Error Inserting to db %s", err)
	}
	for _, val := range inc.Items {
		if !bytes.Equal(val.Value, kv[0].Value) {
			t.Fatalf("History, expected %s, got %s", kv[0].Value, val.GetValue())
		}
	}
}

func TestHealth(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	h, err := db.Health(&emptypb.Empty{})
	if err != nil {
		t.Fatalf("health error %s", err)
	}
	if !h.GetStatus() {
		t.Fatalf("Health, expected %v, got %v", true, h.GetStatus())
	}
}

func TestReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	_, err := db.Set(kv[0])
	if err != nil {
		t.Fatalf("Reference error %s", err)
	}
	ref, err := db.Reference(&schema.ReferenceOptions{
		Reference: []byte(`tag`),
		Key:       kv[0].Key,
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
	if !bytes.Equal(item.Value, kv[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(item.Value), string(kv[0].Value))
	}
	item, err = db.GetReference(&schema.Key{Key: []byte(`tag`)})
	if err != nil {
		t.Fatalf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Value, kv[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(item.Value), string(kv[0].Value))
	}
}

func TestGetReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	_, err := db.Set(kv[0])
	if err != nil {
		t.Fatalf("Reference error %s", err)
	}
	ref, err := db.Reference(&schema.ReferenceOptions{
		Reference: []byte(`tag`),
		Key:       kv[0].Key,
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
	if !bytes.Equal(item.Value, kv[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(item.Value), string(kv[0].Value))
	}
	item, err = db.GetReference(&schema.Key{Key: []byte(`tag`)})
	if err != nil {
		t.Fatalf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Value, kv[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(item.Value), string(kv[0].Value))
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
	if it.Index != 2 {
		t.Fatalf("SafeZAdd index, expected %v, got %v", 2, it.Index)
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

func TestCount(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	root, err := db.CurrentRoot(&emptypb.Empty{})
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

func TestSafeReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	root, err := db.CurrentRoot(&emptypb.Empty{})
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

	root, err := db.CurrentRoot(&emptypb.Empty{})
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

type mockImmuService_DumpServer struct {
	grpc.ServerStream
	results []*pb.KVList
}

func (_m *mockImmuService_DumpServer) Send(kvs *pb.KVList) error {
	_m.results = append(_m.results, kvs)
	return nil
}

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
