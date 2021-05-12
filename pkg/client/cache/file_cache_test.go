package cache

import (
	"encoding/base64"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestNewFileCache(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	os.Mkdir(dirname, os.ModePerm)
	fc := NewFileCache(dirname)
	require.IsType(t, &fileCache{}, fc)
	os.RemoveAll(dirname)
}

func TestFileCacheSetError(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewFileCache(dirname)
	err = fc.Set("uuid", "dbName", &schema.ImmutableState{
		TxId:      0,
		TxHash:    []byte(`hash`),
		Signature: nil,
	})
	require.Error(t, err)
	os.RemoveAll(dirname)
}

func TestFileCacheSet(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewFileCache(dirname)
	err = fc.Lock("uuid")
	require.NoError(t, err)
	err = fc.Set("uuid", "dbName", &schema.ImmutableState{
		TxId:      0,
		TxHash:    []byte(`hash`),
		Signature: nil,
	})
	require.NoError(t, err)
	os.RemoveAll(dirname)
}

func TestFileCacheGet(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewFileCache(dirname)
	err = fc.Lock("uuid")
	require.NoError(t, err)
	err = fc.Set("uuid", "dbName", &schema.ImmutableState{
		TxId:      0,
		TxHash:    []byte(`hash`),
		Signature: nil,
	})
	require.Nil(t, err)
	_, err = fc.Get("uuid", "dbName")
	require.Nil(t, err)
	os.RemoveAll(dirname)
}

func TestFileCacheGetFail(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewFileCache(dirname)
	_, err = fc.Get("uuid", "dbName")
	require.Error(t, err)
	os.RemoveAll(dirname)
}

func TestFileCacheGetSingleLineError(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(dirname + "/.state-test")
	if err != nil {
		log.Fatal("Cannot create state file", err)
		return
	}

	dbName := "dbt"

	defer os.Remove(f.Name())
	if _, err = f.Write([]byte(dbName + ":")); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}

	fc := NewFileCache(dirname)
	_, err = fc.Get("test", dbName)
	require.Error(t, err)
	os.RemoveAll(dirname)
}

func TestFileCacheGetRootUnableToDecodeErr(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(dirname + "/.state-test")
	if err != nil {
		log.Fatal("Cannot create state file", err)
		return
	}

	dbName := "dbt"

	defer os.Remove(f.Name())
	if _, err = f.Write([]byte(dbName + ":firstLine")); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}

	fc := NewFileCache(dirname)
	_, err = fc.Get("test", dbName)
	require.Error(t, err)
	os.RemoveAll(dirname)
}

func TestFileCacheGetRootUnmarshalErr(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(dirname + "/.state-test")
	if err != nil {
		log.Fatal("Cannot create state file", err)
		return
	}

	dbName := "dbt"

	defer os.Remove(f.Name())
	if _, err = f.Write([]byte(dbName + ":" + base64.StdEncoding.EncodeToString([]byte("wrong-content")))); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}

	fc := NewFileCache(dirname)
	_, err = fc.Get("test", dbName)
	require.Error(t, err)
	os.RemoveAll(dirname)
}

func TestFileCacheGetEmptyFile(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(dirname + "/.state-test")
	if err != nil {
		log.Fatal("Cannot create state file", err)
		return
	}

	dbName := "dbt"

	defer os.Remove(f.Name())
	if _, err = f.Write([]byte("")); err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}

	fc := NewFileCache(dirname)
	_, err = fc.Get("test", dbName)
	require.Error(t, err)
}
