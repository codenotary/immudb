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

package cache

import (
	"encoding/base64"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/codenotary/immudb/pkg/api/schema"
)

func TestNewHistoryFileCache(t *testing.T) {
	dir := t.TempDir()

	fc := NewHistoryFileCache(dir)
	require.IsType(t, &historyFileCache{}, fc)

	require.Error(t, fc.Lock("foo"))
	require.Error(t, fc.Unlock())

	err := fc.ServerIdentityCheck("identity1", "uuid2")
	require.NoError(t, err)
}

func TestNewHistoryFileCacheSet(t *testing.T) {
	dir := t.TempDir()

	fc := NewHistoryFileCache(dir)

	err := fc.Set("uuid", "dbName", &schema.ImmutableState{TxId: 1, TxHash: []byte{1}})
	require.NoError(t, err)

	err = fc.Set("uuid", "dbName", &schema.ImmutableState{TxId: 2, TxHash: []byte{2}})
	require.NoError(t, err)

	root, err := fc.Get("uuid", "dbName")
	require.NoError(t, err)
	require.IsType(t, &schema.ImmutableState{}, root)

	_, err = fc.Get("uuid1", "dbName")
	require.NoError(t, err)
}

func TestNewHistoryFileCacheGet(t *testing.T) {
	dir := t.TempDir()

	fc := NewHistoryFileCache(dir)

	root, err := fc.Get("uuid", "dbName")
	require.NoError(t, err)
	require.IsType(t, &schema.ImmutableState{}, root)
}

func TestNewHistoryFileCacheWalk(t *testing.T) {
	dir := t.TempDir()

	fc := NewHistoryFileCache(dir)

	iface, err := fc.Walk("uuid", "dbName", func(root *schema.ImmutableState) interface{} {
		return nil
	})
	require.NoError(t, err)
	require.IsType(t, []interface{}{interface{}(nil)}, iface)

	err = fc.Set("uuid", "dbName", &schema.ImmutableState{
		TxId:      0,
		TxHash:    []byte(`hash`),
		Signature: nil,
	})
	require.NoError(t, err)

	iface, err = fc.Walk("uuid", "dbName", func(root *schema.ImmutableState) interface{} {
		return nil
	})
	require.NoError(t, err)
	require.IsType(t, []interface{}{interface{}(nil)}, iface)
}

func TestHistoryFileCache_SetError(t *testing.T) {
	dir := t.TempDir()

	fc := NewHistoryFileCache(dir)

	err := fc.Set("uuid", "dbName", nil)
	require.ErrorIs(t, err, proto.ErrNil)
}

func TestHistoryFileCache_GetError(t *testing.T) {
	dir := t.TempDir()

	fc := NewHistoryFileCache(dir)

	// create a dummy file so that the cache can't create the directory
	// automatically
	err := ioutil.WriteFile(filepath.Join(dir, "exists"), []byte("data"), 0644)
	require.NoError(t, err)
	_, err = fc.Get("exists", "dbName")
	require.ErrorContains(t, err, "exists")
}

func TestHistoryFileCache_SetMissingFolder(t *testing.T) {
	dir := "/notExists"
	fc := NewHistoryFileCache(dir)
	defer os.RemoveAll(dir)

	err := fc.Set("uuid", "dbName", nil)
	require.ErrorContains(t, err, "error ensuring states dir")
}

func TestHistoryFileCache_WalkFolderNotExistsCreated(t *testing.T) {
	dir := t.TempDir()

	notExists := filepath.Join(dir, "not-exists")
	fc := NewHistoryFileCache(notExists)

	_, err := fc.Walk("uuid", "dbName", func(root *schema.ImmutableState) interface{} {
		return nil
	})
	require.NoError(t, err)
}

func TestHistoryFileCache_getStatesFileInfosError(t *testing.T) {
	dir := t.TempDir()

	notExists := filepath.Join(dir, "does-not-exist")
	fc := &historyFileCache{dir: notExists}
	_, err := fc.getStatesFileInfos(dir)
	require.NoError(t, err)
}

func TestHistoryFileCache_unmarshalRootErr(t *testing.T) {
	fc := &historyFileCache{}
	_, err := fc.unmarshalRoot("path", "db")
	require.ErrorContains(t, err, "error reading state from")
}

func TestHistoryFileCache_unmarshalRootSingleLineErr(t *testing.T) {
	dbName := "dbt"
	tmpFile, err := ioutil.TempFile(os.TempDir(), "file-state")
	require.NoError(t, err, "Cannot create temporary file")
	defer os.Remove(tmpFile.Name())
	if _, err = tmpFile.Write([]byte(dbName + ":")); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	fc := &historyFileCache{}
	_, err = fc.unmarshalRoot(tmpFile.Name(), dbName)
	require.ErrorIs(t, err, ErrPrevStateNotFound)
}

func TestHistoryFileCache_unmarshalRootUnableToDecodeErr(t *testing.T) {
	dbName := "dbt"
	tmpFile, err := ioutil.TempFile(os.TempDir(), "file-state")
	require.NoError(t, err, "Cannot create temporary file")
	defer os.Remove(tmpFile.Name())
	if _, err = tmpFile.Write([]byte(dbName + ":firstLine")); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	fc := &historyFileCache{}
	_, err = fc.unmarshalRoot(tmpFile.Name(), dbName)
	require.ErrorIs(t, err, ErrPrevStateNotFound)
}

func TestHistoryFileCache_unmarshalRootUnmarshalErr(t *testing.T) {
	dbName := "dbt"
	tmpFile, err := ioutil.TempFile(os.TempDir(), "file-state")
	require.NoError(t, err, "Cannot create temporary file")
	defer os.Remove(tmpFile.Name())
	if _, err = tmpFile.Write([]byte(dbName + ":" + base64.StdEncoding.EncodeToString([]byte("wrong-content")))); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	fc := &historyFileCache{}
	_, err = fc.unmarshalRoot(tmpFile.Name(), dbName)
	require.ErrorContains(t, err, "error unmarshaling state from")
}

func TestHistoryFileCache_unmarshalRootEmptyFile(t *testing.T) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "file-state")
	require.NoError(t, err, "Cannot create temporary file")
	defer os.Remove(tmpFile.Name())
	text := []byte("")
	if _, err = tmpFile.Write(text); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	fc := &historyFileCache{}
	state, err := fc.unmarshalRoot(tmpFile.Name(), "db")
	require.NoError(t, err)
	require.Nil(t, state)
}
