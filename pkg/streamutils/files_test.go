package streamutils

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestStreamUtilsFiles(t *testing.T) {
	tmpdir, err := ioutil.TempDir(os.TempDir(), "streamutils")
	defer os.RemoveAll(tmpdir)

	// stat will fail
	_, err = GetKeyValuesFromFiles(filepath.Join(tmpdir, "non-existant"))
	require.Error(t, err)

	unreadable := filepath.Join(tmpdir, "dir")
	os.Mkdir(unreadable, 200)
	// open will fail
	_, err = GetKeyValuesFromFiles(unreadable)
	require.Error(t, err)

	valid := filepath.Join(tmpdir, "data")
	err = ioutil.WriteFile(valid, []byte("content"), 0644)
	require.NoError(t, err)
	kvs, err := GetKeyValuesFromFiles(valid)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
}
