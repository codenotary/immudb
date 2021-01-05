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

package cache

import (
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
)

var dirnamehfc = "./test"

func TestNewHistoryFileCache(t *testing.T) {
	fc := NewHistoryFileCache(dirnamehfc)
	assert.IsType(t, &historyFileCache{}, fc)
}

func TestNewHistoryFileCacheSet(t *testing.T) {
	fc := NewHistoryFileCache(dirnamehfc)

	err := fc.Set("uuid", "dbName", &schema.ImmutableState{TxId: 1, TxHash: []byte{1}})
	assert.Nil(t, err)

	err = fc.Set("uuid", "dbName", &schema.ImmutableState{TxId: 2, TxHash: []byte{2}})
	assert.Nil(t, err)

	root, err := fc.Get("uuid", "dbName")
	assert.Nil(t, err)
	assert.IsType(t, &schema.ImmutableState{}, root)

	_, err = fc.Get("uuid1", "dbName")
	assert.Nil(t, err)

	os.RemoveAll(dirnamehfc)
}

func TestNewHistoryFileCacheGet(t *testing.T) {
	os.Mkdir(dirnamehfc, os.ModePerm)
	fc := NewHistoryFileCache(dirnamehfc)
	root, err := fc.Get("uuid", "dbName")
	assert.Nil(t, err)
	assert.IsType(t, &schema.ImmutableState{}, root)
	os.RemoveAll(dirnamehfc)
}

func TestNewHistoryFileCacheWalk(t *testing.T) {
	os.Mkdir(dirnamehfc, os.ModePerm)
	defer os.RemoveAll(dirnamehfc)

	fc := NewHistoryFileCache(dirnamehfc)

	iface, err := fc.Walk("uuid", "dbName", func(root *schema.ImmutableState) interface{} {
		return nil
	})
	assert.Nil(t, err)
	assert.IsType(t, []interface{}{interface{}(nil)}, iface)

	err = fc.Set("uuid", "dbName", &schema.ImmutableState{})
	assert.Nil(t, err)

	iface, err = fc.Walk("uuid", "dbName", func(root *schema.ImmutableState) interface{} {
		return nil
	})
	assert.Nil(t, err)
	assert.IsType(t, []interface{}{interface{}(nil)}, iface)
}
