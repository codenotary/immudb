package cache

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var dirname = "./test"

func TestNewFileCache(t *testing.T) {
	os.Mkdir(dirname, os.ModePerm)
	fc := NewFileCache(dirname)
	assert.IsType(t, &fileCache{}, fc)
	os.RemoveAll(dirname)
}

func TestFileCacheSet(t *testing.T) {
	os.Mkdir(dirname, os.ModePerm)
	fc := NewFileCache(dirname)
	err := fc.Set(&schema.Root{}, "uuid", "dbName")
	assert.Nil(t, err)
	os.RemoveAll(dirname)
}

func TestFileCacheGet(t *testing.T) {
	os.Mkdir(dirname, os.ModePerm)
	fc := NewFileCache(dirname)
	err := fc.Set(&schema.Root{}, "uuid", "dbName")
	root, err := fc.Get("uuid", "dbName")
	assert.Nil(t, err)
	assert.IsType(t, &schema.Root{}, root)
	os.RemoveAll(dirname)
}

func TestFileCacheGetFail(t *testing.T) {
	os.Mkdir(dirname, os.ModePerm)
	fc := NewFileCache(dirname)
	_, err := fc.Get("uuid", "dbName")
	assert.Error(t, err)
	os.RemoveAll(dirname)
}
