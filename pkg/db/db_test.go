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
	"os"
	"path"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
)

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
	options := DefaultOption().WithDbName("EdithPiaf")
	db, err := NewDb(options)
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	defer os.RemoveAll(options.DbName)

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
	options := DefaultOption().WithDbName("EdithPiaf")
	db, err := NewDb(options)
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	defer os.RemoveAll(options.DbName)

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
	options := DefaultOption().WithDbName("EdithPiaf")
	db, err := NewDb(options)
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	defer os.RemoveAll(options.DbName)

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
