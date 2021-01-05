package cache

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
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
	assert.IsType(t, &fileCache{}, fc)
	os.RemoveAll(dirname)
}

func TestFileCacheSet(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewFileCache(dirname)
	err = fc.Set("uuid", "dbName", &schema.ImmutableState{})
	assert.Nil(t, err)
	os.RemoveAll(dirname)
}

func TestFileCacheGet(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewFileCache(dirname)
	err = fc.Set("uuid", "dbName", &schema.ImmutableState{})
	assert.Nil(t, err)
	root, err := fc.Get("uuid", "dbName")
	assert.Nil(t, err)
	assert.IsType(t, &schema.ImmutableState{}, root)
	os.RemoveAll(dirname)
}

func TestFileCacheGetFail(t *testing.T) {
	dirname, err := ioutil.TempDir("", "example")
	if err != nil {
		log.Fatal(err)
	}
	fc := NewFileCache(dirname)
	_, err = fc.Get("uuid", "dbName")
	assert.Error(t, err)
	os.RemoveAll(dirname)
}
