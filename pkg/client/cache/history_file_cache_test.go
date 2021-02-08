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

package cache

import (
	"encoding/base64"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codenotary/immudb/pkg/api/schema"
)

func TestNewHistoryFileCache(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewHistoryFileCache(dir)
	defer os.RemoveAll(dir)
	require.IsType(t, &historyFileCache{}, fc)
}

func TestNewHistoryFileCacheSet(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewHistoryFileCache(dir)
	defer os.RemoveAll(dir)

	err = fc.Set("uuid", "dbName", &schema.ImmutableState{TxId: 1, TxHash: []byte{1}})
	require.Nil(t, err)

	err = fc.Set("uuid", "dbName", &schema.ImmutableState{TxId: 2, TxHash: []byte{2}})
	require.Nil(t, err)

	root, err := fc.Get("uuid", "dbName")
	require.Nil(t, err)
	require.IsType(t, &schema.ImmutableState{}, root)

	_, err = fc.Get("uuid1", "dbName")
	require.Nil(t, err)

}

func TestNewHistoryFileCacheGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewHistoryFileCache(dir)
	defer os.RemoveAll(dir)

	root, err := fc.Get("uuid", "dbName")
	require.Nil(t, err)
	require.IsType(t, &schema.ImmutableState{}, root)
}

func TestNewHistoryFileCacheWalk(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewHistoryFileCache(dir)
	defer os.RemoveAll(dir)

	iface, err := fc.Walk("uuid", "dbName", func(root *schema.ImmutableState) interface{} {
		return nil
	})
	require.Nil(t, err)
	require.IsType(t, []interface{}{interface{}(nil)}, iface)

	err = fc.Set("uuid", "dbName", &schema.ImmutableState{
		TxId:      0,
		TxHash:    []byte(`hash`),
		Signature: nil,
	})
	require.Nil(t, err)

	iface, err = fc.Walk("uuid", "dbName", func(root *schema.ImmutableState) interface{} {
		return nil
	})
	require.Nil(t, err)
	require.IsType(t, []interface{}{interface{}(nil)}, iface)
}

func TestHistoryFileCache_SetError(t *testing.T) {
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewHistoryFileCache(dir)
	defer os.RemoveAll(dir)

	err = fc.Set("uuid", "dbName", nil)
	require.Error(t, err)
}

func TestHistoryFileCache_SetMissingFolder(t *testing.T) {
	dir := "/notExists"
	fc := NewHistoryFileCache(dir)
	defer os.RemoveAll(dir)

	err := fc.Set("uuid", "dbName", nil)
	require.Error(t, err)
}

func TestHistoryFileCache_WalkFolderNotExists(t *testing.T) {
	dir := "/notExists"
	fc := NewHistoryFileCache(dir)
	defer os.RemoveAll(dir)

	_, err := fc.Walk("uuid", "dbName", func(root *schema.ImmutableState) interface{} {
		return nil
	})
	require.Error(t, err)
}

func TestHistoryFileCache_getStatesFileInfosError(t *testing.T) {
	dir := "./testNotExists"
	err := os.MkdirAll(dir, 0000)
	defer os.RemoveAll(dir)
	fc := &historyFileCache{dir: dir}
	_, err = fc.getStatesFileInfos(dir)
	require.Error(t, err)
}

func TestHistoryFileCache_unmarshalRootErr(t *testing.T) {
	fc := &historyFileCache{}
	_, err := fc.unmarshalRoot("path", "db")
	require.Error(t, err)
}

func TestHistoryFileCache_unmarshalRootSingleLineErr(t *testing.T) {
	dbName := "dbt"
	tmpFile, err := ioutil.TempFile(os.TempDir(), "file-state")
	if err != nil {
		log.Fatal("Cannot create temporary file", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err = tmpFile.Write([]byte(dbName + ":")); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	fc := &historyFileCache{}
	_, err = fc.unmarshalRoot(tmpFile.Name(), dbName)
	require.Error(t, err)
}

func TestHistoryFileCache_unmarshalRootUnableToDecodeErr(t *testing.T) {
	dbName := "dbt"
	tmpFile, err := ioutil.TempFile(os.TempDir(), "file-state")
	if err != nil {
		log.Fatal("Cannot create temporary file", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err = tmpFile.Write([]byte(dbName + ":firstLine")); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	fc := &historyFileCache{}
	_, err = fc.unmarshalRoot(tmpFile.Name(), dbName)
	require.Error(t, err)
}

func TestHistoryFileCache_unmarshalRootUnmarshalErr(t *testing.T) {
	dbName := "dbt"
	tmpFile, err := ioutil.TempFile(os.TempDir(), "file-state")
	if err != nil {
		log.Fatal("Cannot create temporary file", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err = tmpFile.Write([]byte(dbName + ":" + base64.StdEncoding.EncodeToString([]byte("wrong-content")))); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	fc := &historyFileCache{}
	_, err = fc.unmarshalRoot(tmpFile.Name(), dbName)
	require.Error(t, err)
}

func TestHistoryFileCache_unmarshalRootEmptyFile(t *testing.T) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "file-state")
	if err != nil {
		log.Fatal("Cannot create temporary file", err)
	}
	defer os.Remove(tmpFile.Name())
	text := []byte("")
	if _, err = tmpFile.Write(text); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	fc := &historyFileCache{}
	state, err := fc.unmarshalRoot(tmpFile.Name(), "db")
	require.Nil(t, err)
	require.Nil(t, state)
}
