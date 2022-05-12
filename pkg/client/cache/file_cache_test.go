package cache

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestNewFileCache(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	fc := NewFileCache(dirname)
	require.IsType(t, &fileCache{}, fc)
}

func TestFileCacheSetErrorNotLocked(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	fc := NewFileCache(dirname)
	err = fc.Set("uuid", "dbName", &schema.ImmutableState{
		TxId:      0,
		TxHash:    []byte(`hash`),
		Signature: nil,
	})
	require.ErrorIs(t, err, ErrCacheNotLocked)
}

func TestFileCacheSet(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	fc := NewFileCache(dirname)
	err = fc.Lock("uuid")
	require.NoError(t, err)
	defer fc.Unlock()

	err = fc.Set("uuid", "dbName", &schema.ImmutableState{
		TxId:      0,
		TxHash:    []byte(`hash`),
		Signature: nil,
	})
	require.NoError(t, err)
}

func TestFileCacheGet(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	fc := NewFileCache(dirname)
	err = fc.Lock("uuid")
	require.NoError(t, err)
	defer fc.Unlock()

	err = fc.Set("uuid", "dbName", &schema.ImmutableState{
		TxId:      0,
		TxHash:    []byte(`hash`),
		Signature: nil,
	})
	require.NoError(t, err)

	st, err := fc.Get("uuid", "dbName")
	require.NoError(t, err)

	require.Equal(t, []byte(`hash`), st.TxHash)
}

func TestFileCacheGetFailNotLocked(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	fc := NewFileCache(dirname)
	_, err = fc.Get("uuid", "dbName")

	require.ErrorIs(t, err, ErrCacheNotLocked)
}

func TestFileCacheGetSingleLineError(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	dbName := "dbt"

	err = ioutil.WriteFile(dirname+"/.state-test", []byte(dbName+":"), 0666)
	require.NoError(t, err)

	fc := NewFileCache(dirname)
	err = fc.Lock("test")
	require.NoError(t, err)
	defer fc.Unlock()

	_, err = fc.Get("test", dbName)
	require.ErrorIs(t, err, ErrLocalStateCorrupted)
}

func TestFileCacheGetRootUnableToDecodeErr(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	dbName := "dbt"

	err = ioutil.WriteFile(dirname+"/.state-test", []byte(dbName+":firstLine"), 0666)
	require.NoError(t, err)

	fc := NewFileCache(dirname)
	err = fc.Lock("test")
	require.NoError(t, err)
	defer fc.Unlock()

	_, err = fc.Get("test", dbName)
	require.ErrorIs(t, err, ErrLocalStateCorrupted)
}

func TestFileCacheGetRootUnmarshalErr(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	dbName := "dbt"

	err = ioutil.WriteFile(dirname+"/.state-test", []byte(dbName+":"+base64.StdEncoding.EncodeToString([]byte("wrong-content"))), 0666)
	require.NoError(t, err)

	fc := NewFileCache(dirname)
	err = fc.Lock("test")
	require.NoError(t, err)
	defer fc.Unlock()

	_, err = fc.Get("test", dbName)
	require.ErrorIs(t, err, ErrLocalStateCorrupted)
}

func TestFileCacheGetEmptyFile(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	dbName := "dbt"

	err = ioutil.WriteFile(dirname+"/.state-test", []byte(""), 0666)
	require.NoError(t, err)

	fc := NewFileCache(dirname)
	err = fc.Lock("test")
	require.NoError(t, err)
	defer fc.Unlock()

	_, err = fc.Get("test", dbName)
	require.ErrorIs(t, err, ErrPrevStateNotFound)
}

func TestFileCacheOverwriteHash(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	fc := NewFileCache(dirname)
	err = fc.Lock("test")
	require.NoError(t, err)
	defer fc.Unlock()

	err = fc.Set("test", "db1", &schema.ImmutableState{TxHash: []byte("hash1")})
	require.NoError(t, err)

	st, err := fc.Get("test", "db1")
	require.NoError(t, err)
	require.Equal(t, []byte("hash1"), st.TxHash)

	err = fc.Set("test", "db1", &schema.ImmutableState{TxHash: []byte("hash2")})
	require.NoError(t, err)

	st, err = fc.Get("test", "db1")
	require.NoError(t, err)
	require.Equal(t, []byte("hash2"), st.TxHash)
}

func TestFileCacheMultipleDatabases(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	require.NoError(t, err)
	defer os.RemoveAll(dirname)

	fc := NewFileCache(dirname)
	err = fc.Lock("test")
	require.NoError(t, err)
	defer fc.Unlock()

	for i := 0; i < 1000; i++ {

		db := fmt.Sprintf("db%d", i)
		hash := []byte(fmt.Sprintf("hash%d", i))

		err = fc.Set("test", db, &schema.ImmutableState{TxHash: hash})
		require.NoError(t, err)

		st, err := fc.Get("test", db)
		require.NoError(t, err)
		require.Equal(t, hash, st.TxHash)
	}

	for i := 0; i < 1000; i++ {
		db := fmt.Sprintf("db%d", i)
		hash := []byte(fmt.Sprintf("hash%d", i))

		st, err := fc.Get("test", db)
		require.NoError(t, err)
		require.Equal(t, hash, st.TxHash)
	}
}
