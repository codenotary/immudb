package cache

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var dirnamehfc = "./test"

func TestNewHistoryFileCache(t *testing.T) {
	fc := NewHistoryFileCache(dirnamehfc)
	assert.IsType(t, &historyFileCache{}, fc)
}

func TestNewHistoryFileCacheSet(t *testing.T) {
	fc := NewHistoryFileCache(dirnamehfc)
	err := fc.Set(&schema.Root{}, "uuid", "dbName")
	assert.Nil(t, err)
	os.RemoveAll(dirnamehfc)
}

func TestNewHistoryFileCacheGet(t *testing.T) {
	os.Mkdir(dirnamehfc, os.ModePerm)
	fc := NewHistoryFileCache(dirnamehfc)
	root, err := fc.Get("uuid", "dbName")
	assert.Nil(t, err)
	assert.IsType(t, &schema.Root{}, root)
	os.RemoveAll(dirnamehfc)
}

func TestNewHistoryFileCacheWalk(t *testing.T) {
	os.Mkdir(dirnamehfc, os.ModePerm)
	fc := NewHistoryFileCache(dirnamehfc)
	iface, err := fc.Walk("uuid", "dbName", func(root *schema.Root) interface{} {
		return nil
	})
	assert.Nil(t, err)
	assert.IsType(t, []interface{}{interface{}(nil)}, iface)
	os.RemoveAll(dirnamehfc)
}
