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
			t.Fatalf("Inserted Key not equal to read Key")
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
		if r.Payload.Index != uint64(ind) {
			t.Fatalf("root error expecting %v got %v", ind, r.Payload.Index)
		}
	}
}

func TestSVSetGet(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for ind, val := range Skv.SKVs {
		it, err := db.SetSV(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Fatalf("index error expecting %v got %v", ind, it.GetIndex())
		}
		k := &schema.Key{
			Key: []byte(val.Key),
		}
		item, err := db.GetSV(k)
		if err != nil {
			t.Fatalf("Error reading key %s", err)
		}
		if !bytes.Equal(item.GetKey(), val.Key) {
			t.Fatalf("Inserted Key not equal to read Key")
		}
		sk := item.GetValue()
		if sk.GetTimestamp() != val.GetValue().GetTimestamp() {
			t.Fatalf("Inserted value not equal to read value")
		}
		if !bytes.Equal(sk.GetPayload(), val.GetValue().Payload) {
			t.Fatalf("Inserted Payload not equal to read value")
		}
	}
	_, err := db.SetSV(&schema.StructuredKeyValue{
		Key: []byte{},
	})
	if err == nil {
		t.Fatalf("SetSV error exptected")
	}
	_, err = db.GetSV(&schema.Key{
		Key: []byte{},
	})
	if err == nil {
		t.Fatalf("GetSV error exptected")
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
				Index: root.Payload.Index,
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Jean-Claude"),
				Value: []byte("Killy"),
			},
			RootIndex: &schema.Index{
				Index: root.Payload.Index,
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Franz"),
				Value: []byte("Clamer"),
			},
			RootIndex: &schema.Index{
				Index: root.Payload.Index,
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
		if it.GetItem().GetIndex() != uint64(ind) {
			t.Fatalf("SafeGet index error, expected %d, got %d", uint64(ind), it.GetItem().GetIndex())
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
				Index: root.Payload.Index,
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
				Index: root.Payload.Index,
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
				Index: root.Payload.Index,
			},
		},
	}
	for ind, val := range SafeSkv {
		proof, err := db.SafeSetSV(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
		if proof == nil {
			t.Fatalf("Nil proof after SafeSet")
		}
		if proof.GetIndex() != uint64(ind) {
			t.Fatalf("SafeSet proof index error, expected %d, got %d", uint64(ind), proof.GetIndex())
		}

		it, err := db.SafeGetSV(&schema.SafeGetOptions{
			Key: val.Skv.Key,
		})
		if it.GetItem().GetIndex() != uint64(ind) {
			t.Fatalf("SafeGet index error, expected %d, got %d", uint64(ind), it.GetItem().GetIndex())
		}
	}
	_, err = db.SafeSetSV(&schema.SafeSetSVOptions{
		Skv: &schema.StructuredKeyValue{
			Key: []byte{},
		},
	})
	if err == nil {
		t.Fatalf("SafeSetSV expected error")
	}
	_, err = db.Set(&schema.KeyValue{
		Key:   []byte("pc"),
		Value: []byte("x86"),
	})
	if err != nil {
		t.Fatalf("Error Inserting to db %s", err)
	}
	_, err = db.SafeGetSV(&schema.SafeGetOptions{
		Key: []byte("pc"),
	})
	if err == nil {
		t.Fatalf("SafeGetSV expected error")
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
	for ind, val := range itList.Items {
		if !bytes.Equal(val.Value, Skv.KVs[ind].Value) {
			t.Fatalf("BatchSet value not equal to BatchGet value, expected %s, got %s", string(Skv.KVs[ind].Value), string(val.Value))
		}
	}
}

func TestSetGetBatchSV(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	ind, err := db.SetBatchSV(Skv)
	if err != nil {
		t.Fatalf("SetBatchSV Error Inserting to db %s", err)
	}
	if ind == nil {
		t.Fatalf("Nil index after Setbatch")
	}
	if ind.GetIndex() != 2 {
		t.Fatalf("SetBatchSV error, expected %d, got %d", 2, ind.GetIndex())
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
			t.Fatalf("BatchSetSV value not equal to BatchGetSV value, expected %s, got %s", string(Skv.SKVs[ind].Value.Payload), string(val.Value.Payload))
		}
		if val.Value.Timestamp != Skv.SKVs[ind].Value.Timestamp {
			t.Fatalf("BatchSetSV value not equal to BatchGetSV value, expected %d, got %d", Skv.SKVs[ind].Value.Timestamp, val.Value.Timestamp)
		}
	}

	_, err = db.SetBatchSV(&schema.SKVList{
		SKVs: nil,
	})
	if err == nil {
		t.Fatalf("SetBatchSV expected error")
	}

	_, err = db.SetBatchSV(&schema.SKVList{
		SKVs: []*schema.StructuredKeyValue{
			{
				Key: []byte{},
			},
		},
	})
	if err == nil {
		t.Fatalf("SetBatchSV expected error %s", err)
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

func TestByIndexSV(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	for _, val := range Skv.SKVs {
		_, err := db.SetSV(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}
	time.Sleep(1 * time.Second)
	ind := uint64(1)
	inc, err := db.ByIndexSV(&schema.Index{Index: ind})
	if err != nil {
		t.Fatalf("Error Inserting to db %s", err)
	}
	if inc.Value.Timestamp != Skv.SKVs[ind].Value.Timestamp {
		t.Fatalf("ByIndexSV timestamp, expected %d, got %d", Skv.SKVs[ind].Value.GetTimestamp(), inc.Value.GetTimestamp())
	}
}

func TestBySafeIndex(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	for _, val := range Skv.SKVs {
		_, err := db.SetSV(val)
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
	time.Sleep(1 * time.Second)

	inc, err := db.History(&schema.Key{
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

func TestHistorySV(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for _, val := range Skv.SKVs {
		_, err := db.SetSV(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}
	_, err := db.SetSV(Skv.SKVs[0])

	k := &schema.Key{
		Key: []byte(Skv.SKVs[0].Key),
	}
	items, err := db.HistorySV(k)
	if err != nil {
		t.Fatalf("Error reading key %s", err)
	}
	for _, val := range items.Items {
		if !bytes.Equal(val.Value.Payload, Skv.SKVs[0].Value.Payload) {
			t.Fatalf("HistorySV, expected %s, got %s", Skv.SKVs[0].Value.Payload, val.Value.Payload)
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
}

func TestZAdd(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	_, err := db.Set(kv[0])
	if err != nil {
		t.Fatalf("Reference error %s", err)
	}
	ref, err := db.ZAdd(&schema.ZAddOptions{
		Key:   kv[0].Key,
		Score: 1,
		Set:   kv[0].Value,
	})

	if ref.Index != 1 {
		t.Fatalf("Reference, expected %v, got %v", 1, ref.Index)
	}
	item, err := db.ZScan(&schema.ZScanOptions{
		Set:     kv[0].Value,
		Offset:  []byte(""),
		Limit:   3,
		Reverse: false,
	})
	if err != nil {
		t.Fatalf("Reference  Get error %s", err)
	}
	if !bytes.Equal(item.Items[0].Value, kv[0].Value) {
		t.Fatalf("Reference, expected %v, got %v", string(kv[0].Value), string(item.Items[0].Value))
	}
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
		Score: 3,
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
			Score: 0,
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
		t.Fatalf("ZScanSV  Get error %s", err)
	}
	if !bytes.Equal(scanItem.Items[0].Value, kv[0].Key) {
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
				Index: root.Payload.Index,
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Jean-Claude"),
				Value: []byte("Killy"),
			},
			RootIndex: &schema.Index{
				Index: root.Payload.Index,
			},
		},
		{
			Kv: &schema.KeyValue{
				Key:   []byte("Franz"),
				Value: []byte("Clamer"),
			},
			RootIndex: &schema.Index{
				Index: root.Payload.Index,
			},
		},
	}
	for _, val := range kv {
		_, err := db.SafeSet(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}
	c, err := db.Count(&schema.KeyPrefix{
		Prefix: []byte("Franz"),
	})
	if err != nil {
		t.Fatalf("Error count %s", err)
	}
	if c.Count != 1 {
		t.Fatalf("Error count expected %d got %d", 1, c.Count)
	}
}
func TestScanSV(t *testing.T) {
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
				Index: root.Payload.Index,
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
				Index: root.Payload.Index,
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
				Index: root.Payload.Index,
			},
		},
	}
	for _, val := range SafeSkv {
		_, err := db.SafeSetSV(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}
	sc, err := db.ScanSV(&schema.ScanOptions{
		Offset: []byte("Franz"),
	})
	if err != nil {
		t.Fatalf("ScanSV error %s", err)
	}
	if len(sc.Items) != 3 {
		t.Fatalf("ScanSV count expected %d got %d", 3, len(sc.Items))
	}
}
func TestIscanSv(t *testing.T) {
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
				Index: root.Payload.Index,
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
				Index: root.Payload.Index,
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
				Index: root.Payload.Index,
			},
		},
	}
	for _, val := range SafeSkv {
		_, err := db.SafeSetSV(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}
	sc, err := db.IScanSV(&schema.IScanOptions{
		PageNumber: 0,
		PageSize:   1,
	})
	if err != nil {
		t.Fatalf("IScanSV error %s", err)
	}
	if len(sc.Items) != 1 {
		t.Fatalf("IScanSV count expected %d got %d", 1, len(sc.Items))
	}

	_, err = db.IScanSV(&schema.IScanOptions{
		PageNumber: 111111111,
		PageSize:   111111111,
	})
	if err == nil {
		t.Fatalf("IScanSV expected error")
	}
}

func TestZScanSV(t *testing.T) {
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
				Index: root.Payload.Index,
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
				Index: root.Payload.Index,
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
				Index: root.Payload.Index,
			},
		},
	}
	for _, val := range SafeSkv {
		_, err := db.SafeSetSV(val)
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}

		_, err = db.ZAdd(&schema.ZAddOptions{
			Set:   []byte("test-set"),
			Key:   val.Skv.Key,
			Score: 1,
		})
		if err != nil {
			t.Fatalf("Error Inserting to db %s", err)
		}
	}

	sc, err := db.ZScanSV(&schema.ZScanOptions{
		Set:    []byte("test-set"),
		Offset: []byte("Franz"),
	})
	if err != nil {
		t.Fatalf("ZScanSV error %s", err)
	}
	if len(sc.Items) != 3 {
		t.Fatalf("ZScanSV count expected %d got %d", 3, len(sc.Items))
	}

	_, err = db.Set(&schema.KeyValue{
		Key:   []byte("keyboard"),
		Value: []byte("qwerty"),
	})
	if err != nil {
		t.Fatalf("ZScanSV expected error %s", err)
	}
	_, err = db.ZAdd(&schema.ZAddOptions{
		Set:   []byte("test-set"),
		Key:   []byte("keyboard"),
		Score: 1,
	})
	if err != nil {
		t.Fatalf("Error Inserting to db %s", err)
	}

	_, err = db.ZScanSV(&schema.ZScanOptions{
		Set:    []byte("test-set"),
		Offset: []byte("keyboard"),
	})
	if err == nil {
		t.Fatalf("ZScanSV expected error %s", err)
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
				Index: root.Payload.Index,
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
			Index: root.Payload.Index,
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
