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

package db

import (
	"bytes"
	"context"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/protobuf/types/known/emptypb"
)

func makeDb() (*Db, func()) {
	options := DefaultOption().WithDbName("EdithPiaf")
	d, err := NewDb(options)
	if err != nil {
		log.Fatalf("Error creating Db instance %s", err)
	}
	return d, func() {
		if err := d.Store.Close(); err != nil {
			log.Fatal(err)
		}
		if err := d.SysStore.Close(); err != nil {
			log.Fatal(err)
		}
		if err := os.RemoveAll(options.DbName); err != nil {
			log.Fatal(err)
		}
	}
}

func TestDefaultDbCreation(t *testing.T) {
	options := DefaultOption()
	_, err := NewDb(options)
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	defer os.RemoveAll(options.DbName)

	if _, err = os.Stat(options.DbName); os.IsNotExist(err) {
		t.Errorf("Db dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Data dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetSysDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Sys dir not created")
	}
}
func TestDbCreation(t *testing.T) {
	options := DefaultOption().WithDbName("EdithPiaf")
	_, err := NewDb(options)
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	defer os.RemoveAll(options.DbName)

	if _, err = os.Stat(options.DbName); os.IsNotExist(err) {
		t.Errorf("Db dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Data dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetSysDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Sys dir not created")
	}
}

func TestDbSetGet(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	kv := []*schema.KeyValue{
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
	for ind, val := range kv {
		it, err := db.Set(context.Background(), val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Errorf("index error expecting %v got %v", ind, it.GetIndex())
		}
		k := &schema.Key{
			Key: []byte(val.Key),
		}
		item, err := db.Get(context.Background(), k)
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

//TODO gj unfinished
func TestCurrentRoot(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	kv := []*schema.KeyValue{
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
	for ind, val := range kv {
		it, err := db.Set(context.Background(), val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Errorf("index error expecting %v got %v", ind, it.GetIndex())
		}
		// root, err := db.CurrentRoot(context.Background(), &emptypb.Empty{})
		// if err != nil {
		// 	t.Errorf("Error getting current root %s", err)
		// }
	}
}

func TestSVSetGet(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	kv := []*schema.StructuredKeyValue{
		{
			Key: []byte("Alberto"),
			Value: &schema.Content{
				Timestamp: uint64(time.Now().Unix()),
				Payload:   []byte("Tomba"),
			},
		},
		{
			Key: []byte("Jean-Claude"),
			Value: &schema.Content{
				Timestamp: uint64(time.Now().Unix()),
				Payload:   []byte("Killy"),
			},
		},
		{
			Key: []byte("Franz"),
			Value: &schema.Content{
				Timestamp: uint64(time.Now().Unix()),
				Payload:   []byte("Clamer"),
			},
		},
	}
	for ind, val := range kv {
		it, err := db.SetSV(context.Background(), val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if it.GetIndex() != uint64(ind) {
			t.Errorf("index error expecting %v got %v", ind, it.GetIndex())
		}
		k := &schema.Key{
			Key: []byte(val.Key),
		}
		item, err := db.GetSV(context.Background(), k)
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
	root, err := db.CurrentRoot(context.Background(), &emptypb.Empty{})
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
		proof, err := db.SafeSet(context.Background(), val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if proof == nil {
			t.Errorf("Nil proof after SafeSet")
		}
		if proof.GetIndex() != uint64(ind) {
			t.Errorf("SafeSet proof index error, expected %d, got %d", uint64(ind), proof.GetIndex())
		}

		it, err := db.SafeGet(context.Background(), &schema.SafeGetOptions{
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
	root, err := db.CurrentRoot(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Error(err)
	}
	Skv := []*schema.SafeSetSVOptions{
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
	for ind, val := range Skv {
		proof, err := db.SafeSetSV(context.Background(), val)
		if err != nil {
			t.Errorf("Error Inserting to db %s", err)
		}
		if proof == nil {
			t.Errorf("Nil proof after SafeSet")
		}
		if proof.GetIndex() != uint64(ind) {
			t.Errorf("SafeSet proof index error, expected %d, got %d", uint64(ind), proof.GetIndex())
		}

		it, err := db.SafeGetSV(context.Background(), &schema.SafeGetOptions{
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
	ind, err := db.SetBatch(context.Background(), Skv)
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if ind == nil {
		t.Errorf("Nil index after Setbatch")
	}
	if ind.GetIndex() != 2 {
		t.Errorf("SafeSet proof index error, expected %d, got %d", 2, ind.GetIndex())
	}

	itList, err := db.GetBatch(context.Background(), &schema.KeyList{
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

	Skv := &schema.SKVList{
		SKVs: []*schema.StructuredKeyValue{
			{
				Key: []byte("Alberto"),
				Value: &schema.Content{
					Timestamp: uint64(time.Now().Unix()),
					Payload:   []byte("Tomba"),
				},
			},
			{
				Key: []byte("Jean-Claude"),
				Value: &schema.Content{
					Timestamp: uint64(time.Now().Unix()),
					Payload:   []byte("Killy"),
				},
			},
			{
				Key: []byte("Franz"),
				Value: &schema.Content{
					Timestamp: uint64(time.Now().Unix()),
					Payload:   []byte("Clamer"),
				},
			},
		},
	}
	ind, err := db.SetBatchSV(context.Background(), Skv)
	if err != nil {
		t.Errorf("Error Inserting to db %s", err)
	}
	if ind == nil {
		t.Errorf("Nil index after Setbatch")
	}
	if ind.GetIndex() != 2 {
		t.Errorf("SafeSet proof index error, expected %d, got %d", 2, ind.GetIndex())
	}

	itList, err := db.GetBatchSV(context.Background(), &schema.KeyList{
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
		if !bytes.Equal(val.Value.Payload, Skv.SKVs[ind].Value.Payload) {
			t.Errorf("BatchSetSV value not equal to BatchGetSV value, expected %s, got %s", string(Skv.SKVs[ind].Value.Payload), string(val.Value.Payload))
		}
		if val.Value.Timestamp != Skv.SKVs[ind].Value.Timestamp {
			t.Errorf("BatchSetSV value not equal to BatchGetSV value, expected %d, got %d", Skv.SKVs[ind].Value.Timestamp, val.Value.Timestamp)
		}
	}
}
