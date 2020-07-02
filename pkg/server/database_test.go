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
	"log"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
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
		t.Errorf("Error creating Db instance %s", err)
	}
	defer func() {
		db.Store.Close()
		time.Sleep(1 * time.Second)
		os.RemoveAll(options.GetDbRootPath())
	}()
	dbPath := path.Join(options.GetDbRootPath(), options.GetDbName())
	if _, err = os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("Db dir not created")
	}

	_, err = os.Stat(path.Join(options.GetDbRootPath()))
	if os.IsNotExist(err) {
		t.Errorf("Data dir not created")
	}
}
func TestDbCreation(t *testing.T) {
	options := DefaultOption().WithDbName("EdithPiaf").WithDbRootPath("Paris")
	db, err := NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	defer func() {
		db.Store.Close()
		time.Sleep(1 * time.Second)
		os.RemoveAll(options.GetDbRootPath())
	}()

	dbPath := path.Join(options.GetDbRootPath(), options.GetDbName())
	if _, err = os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("Db dir not created")
	}

	_, err = os.Stat(options.GetDbRootPath())
	if os.IsNotExist(err) {
		t.Errorf("Data dir not created")
	}
}

func TestOpenDb(t *testing.T) {
	options := DefaultOption().WithDbName("EdithPiaf").WithDbRootPath("Paris")
	db, err := NewDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	err = db.Store.Close()
	if err != nil {
		t.Errorf("Error closing store %s", err)
	}

	db, err = OpenDb(options, logger.NewSimpleLogger("immudb ", os.Stderr))
	if err != nil {
		t.Errorf("Error opening database %s", err)
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
			t.Errorf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Errorf("index error expecting %v got %v", ind, it.GetIndex())
		}
		k := &schema.Key{
			Key: []byte(val.Key),
		}
		item, err := db.Get(k)
		if err != nil {
			t.Errorf("Error reading key %s", err)
		}
		if !bytes.Equal(item.Key, val.Key) {
			t.Errorf("Inserted Key not equal to read Key")
		}
		if !bytes.Equal(item.Value, val.Value) {
			t.Errorf("Inserted value not equal to read value")
		}
	}
}

func TestCurrentRoot(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range kv {
		it, err := db.Set(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Errorf("index error expecting %v got %v", ind, it.GetIndex())
		}
		time.Sleep(1 * time.Second)
		r, err := db.CurrentRoot(&emptypb.Empty{})
		if err != nil {
			t.Errorf("Error getting current root %s", err)
		}
		if r.Index != uint64(ind) {
			t.Errorf("root error expecting %v got %v", ind, r.Index)
		}
	}
}

func TestSVSetGet(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range Skv.SKVs {
		it, err := db.SetSV(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Errorf("index error expecting %v got %v", ind, it.GetIndex())
		}
		k := &schema.Key{
			Key: []byte(val.Key),
		}
		item, err := db.GetSV(k)
		if err != nil {
			t.Errorf("Error reading key %s", err)
		}
		if !bytes.Equal(item.GetKey(), val.Key) {
			t.Errorf("Inserted Key not equal to read Key")
		}
		sk := item.GetValue()
		if sk.GetTimestamp() != val.GetValue().GetTimestamp() {
			t.Errorf("Inserted value not equal to read value")
		}
		if !bytes.Equal(sk.GetPayload(), val.GetValue().Payload) {
			t.Errorf("Inserted Payload not equal to read value")
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
				Index: root.Index,
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Jean-Claude"),
				Value: []byte("Killy"),
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Franz"),
				Value: []byte("Clamer"),
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
	}
	for ind, val := range kv {
		proof, err := db.SafeSet(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if proof == nil {
			t.Errorf("Nil proof after SafeSet")
		}
		if proof.GetIndex() != uint64(ind) {
			t.Errorf("SafeSet proof index error, expected %d, got %d", uint64(ind), proof.GetIndex())
		}

		it, err := db.SafeGet(&schema.SafeGetOptions{
			Key: val.Kv.Key,
		})
		if it.GetItem().GetIndex() != uint64(ind) {
			t.Errorf("SafeGet index error, expected %d, got %d", uint64(ind), it.GetItem().GetIndex())
		}
	}
}

func TestSafeSetGetSV(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	root, err := db.CurrentRoot(&emptypb.Empty{})
	if err != nil {
		t.Error(err)
	}
	SafeSkv := []*schema.SafeSetSVOptions{
		{
			Skv: &schema.StructuredKeyValue{
				Key: []byte("Alberto"),
				Value: &schema.Content{
					Timestamp: uint64(time.Now().Unix()),
					Payload:   []byte("Tomba"),
				},
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
		{
			Skv: &schema.StructuredKeyValue{
				Key: []byte("Jean-Claude"),
				Value: &schema.Content{
					Timestamp: uint64(time.Now().Unix()),
					Payload:   []byte("Killy"),
				},
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
		{
			Skv: &schema.StructuredKeyValue{
				Key: []byte("Franz"),
				Value: &schema.Content{
					Timestamp: uint64(time.Now().Unix()),
					Payload:   []byte("Clamer"),
				},
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		},
	}
	for ind, val := range SafeSkv {
		proof, err := db.SafeSetSV(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if proof == nil {
			t.Errorf("Nil proof after SafeSet")
		}
		if proof.GetIndex() != uint64(ind) {
			t.Errorf("SafeSet proof index error, expected %d, got %d", uint64(ind), proof.GetIndex())
		}

		it, err := db.SafeGetSV(&schema.SafeGetOptions{
			Key: val.Skv.Key,
		})
		if it.GetItem().GetIndex() != uint64(ind) {
			t.Errorf("SafeGet index error, expected %d, got %d", uint64(ind), it.GetItem().GetIndex())
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
		t.Errorf("Error Inserting to db %s", err)
	}
	if ind == nil {
		t.Errorf("Nil index after Setbatch")
	}
	if ind.GetIndex() != 2 {
		t.Errorf("SafeSet proof index error, expected %d, got %d", 2, ind.GetIndex())
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
	for ind, val := range itList.Items {
		if !bytes.Equal(val.Value, Skv.KVs[ind].Value) {
			t.Errorf("BatchSet value not equal to BatchGet value, expected %s, got %s", string(Skv.KVs[ind].Value), string(val.Value))
		}
	}
}

func TestSetGetBatchSV(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	ind, err := db.SetBatchSV(Skv)
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if ind == nil {
		t.Errorf("Nil index after Setbatch")
	}
	if ind.GetIndex() != 2 {
		t.Errorf("SafeSet proof index error, expected %d, got %d", 2, ind.GetIndex())
	}

	itList, err := db.GetBatchSV(&schema.KeyList{
		Keys: []*schema.Key{
			{
				Key: Skv.SKVs[0].Key,
			},
			{
				Key: Skv.SKVs[1].Key,
			},
			{
				Key: Skv.SKVs[2].Key,
			},
		},
	})
	for ind, val := range itList.Items {
		if !bytes.Equal(val.Value.Payload, Skv.SKVs[ind].Value.Payload) {
			t.Errorf("BatchSetSV value not equal to BatchGetSV value, expected %s, got %s", string(Skv.SKVs[ind].Value.Payload), string(val.Value.Payload))
		}
		if val.Value.Timestamp != Skv.SKVs[ind].Value.Timestamp {
			t.Errorf("BatchSetSV value not equal to BatchGetSV value, expected %d, got %d", Skv.SKVs[ind].Value.Timestamp, val.Value.Timestamp)
		}
	}
}

func TestInclusion(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range kv {
		it, err := db.Set(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Errorf("index error expecting %v got %v", ind, it.GetIndex())
		}
	}
	ind := uint64(1)
	//TODO find a better way without sleep
	time.Sleep(2 * time.Second)
	inc, err := db.Inclusion(&schema.Index{Index: ind})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if inc.Index != ind {
		t.Errorf("Inclusion, expected %d, got %d", inc.Index, ind)
	}
}

func TestConsintency(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range kv {
		it, err := db.Set(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Errorf("index error expecting %v got %v", ind, it.GetIndex())
		}
	}
	time.Sleep(1 * time.Second)
	ind := uint64(1)
	inc, err := db.Consistency(&schema.Index{Index: ind})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if inc.First != ind {
		t.Errorf("Consistency, expected %d, got %d", inc.First, ind)
	}
}

func TestByIndex(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range kv {
		it, err := db.Set(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Errorf("index error expecting %v got %v", ind, it.GetIndex())
		}
	}
	time.Sleep(1 * time.Second)
	ind := uint64(1)
	inc, err := db.ByIndex(&schema.Index{Index: ind})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if !bytes.Equal(inc.Value, kv[ind].Value) {
		t.Errorf("ByIndex, expected %s, got %d", kv[ind].Value, inc.Value)
	}
}

func TestByIndexSV(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	for _, val := range Skv.SKVs {
		_, err := db.SetSV(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
	}
	time.Sleep(1 * time.Second)
	ind := uint64(1)
	inc, err := db.ByIndexSV(&schema.Index{Index: ind})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if inc.Value.Timestamp != Skv.SKVs[ind].Value.Timestamp {
		t.Errorf("ByIndexSV timestamp, expected %d, got %d", Skv.SKVs[ind].Value.GetTimestamp(), inc.Value.GetTimestamp())
	}
}

func TestBySafeIndex(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	for _, val := range Skv.SKVs {
		_, err := db.SetSV(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
	}
	time.Sleep(1 * time.Second)
	ind := uint64(1)
	inc, err := db.BySafeIndex(&schema.SafeIndexOptions{Index: ind})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if inc.Item.Index != ind {
		t.Errorf("ByIndexSV, expected %d, got %d", ind, inc.Item.Index)
	}
}

func TestHistory(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	for _, val := range kv {
		_, err := db.Set(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
	}
	_, err := db.Set(kv[0])
	time.Sleep(1 * time.Second)

	inc, err := db.History(&schema.Key{
		Key: kv[0].Key,
	})
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	for _, val := range inc.Items {
		if !bytes.Equal(val.Value, kv[0].Value) {
			t.Errorf("History, expected %s, got %s", kv[0].Value, val.GetValue())
		}
	}
}

func TestHistorySV(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for _, val := range Skv.SKVs {
		_, err := db.SetSV(val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
	}
	_, err := db.SetSV(Skv.SKVs[0])

	k := &schema.Key{
		Key: []byte(Skv.SKVs[0].Key),
	}
	items, err := db.HistorySV(k)
	if err != nil {
		t.Errorf("Error reading key %s", err)
	}
	for _, val := range items.Items {
		if !bytes.Equal(val.Value.Payload, Skv.SKVs[0].Value.Payload) {
			t.Errorf("HistorySV, expected %s, got %s", Skv.SKVs[0].Value.Payload, val.Value.Payload)
		}
	}
}

func TestHealth(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	h, err := db.Health(&emptypb.Empty{})
	if err != nil {
		t.Errorf("health error %s", err)
	}
	if !h.GetStatus() {
		t.Errorf("Health, expected %v, got %v", true, h.GetStatus())
	}
}

func TestReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	_, err := db.Set(kv[0])
	if err != nil {
		t.Errorf("Reference error %s", err)
	}
	ref, err := db.Reference(&schema.ReferenceOptions{
		Reference: []byte(`tag`),
		Key:       kv[0].Key,
	})
	if ref.Index != 1 {
		t.Errorf("Reference, expected %v, got %v", 1, ref.Index)
	}
	item, err := db.Get(&schema.Key{Key: []byte(`tag`)})
	if err != nil {
		t.Errorf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Value, kv[0].Value) {
		t.Errorf("Reference, expected %v, got %v", string(item.Value), string(kv[0].Value))
	}
}

func TestZAdd(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	_, err := db.Set(kv[0])
	if err != nil {
		t.Errorf("Reference error %s", err)
	}
	ref, err := db.ZAdd(&schema.ZAddOptions{
		Key:   kv[0].Key,
		Score: 1,
		Set:   kv[0].Value,
	})

	if ref.Index != 1 {
		t.Errorf("Reference, expected %v, got %v", 1, ref.Index)
	}
	item, err := db.ZScan(&schema.ZScanOptions{
		Offset:  []byte(""),
		Limit:   3,
		Reverse: false,
	})
	if err != nil {
		t.Errorf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Items[0].Value, kv[0].Value) {
		t.Errorf("Reference, expected %v, got %v", string(kv[0].Value), string(item.Items[0].Value))
	}
}

func TestScan(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.Set(kv[0])
	if err != nil {
		t.Errorf("set error %s", err)
	}
	ref, err := db.ZAdd(&schema.ZAddOptions{
		Key:   kv[0].Key,
		Score: 3,
		Set:   kv[0].Value,
	})
	if err != nil {
		t.Errorf("zadd error %s", err)
	}
	if ref.Index != 1 {
		t.Errorf("Reference, expected %v, got %v", 1, ref.Index)
	}

	it, err := db.SafeZAdd(&schema.SafeZAddOptions{
		Zopts: &schema.ZAddOptions{
			Key:   kv[0].Key,
			Score: 0,
			Set:   kv[0].Value,
		},
		RootIndex: &schema.Index{
			Index: 0,
		},
	})
	if err != nil {
		t.Errorf("SafeZAdd error %s", err)
	}
	if it.Index != 2 {
		t.Errorf("SafeZAdd index, expected %v, got %v", 2, it.Index)
	}

	item, err := db.Scan(&schema.ScanOptions{
		Offset: nil,
		Deep:   false,
		Limit:  1,
		Prefix: kv[0].Key,
	})

	if err != nil {
		t.Errorf("ZScanSV  Get error %s", err)
	}
	if !bytes.Equal(item.Items[0].Value, kv[0].Value) {
		t.Errorf("Reference, expected %v, got %v", string(kv[0].Value), string(item.Items[0].Value))
	}

	scanItem, err := db.IScan(&schema.IScanOptions{
		PageNumber: 2,
		PageSize:   1,
	})
	if err != nil {
		t.Errorf("ZScanSV  Get error %s", err)
	}
	if !bytes.Equal(scanItem.Items[0].Value, kv[0].Key) {
		t.Errorf("Reference, expected %v, got %v", string(kv[0].Key), string(scanItem.Items[0].Value))
	}
}
