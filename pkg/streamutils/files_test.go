package streamutils

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestStreamUtilsFiles(t *testing.T) {
	tmpdir, err := ioutil.TempDir(os.TempDir(), "streamutils")
	defer os.RemoveAll(tmpdir)

	// stat will fail
	_, err = GetKeyValuesFromFiles(filepath.Join(tmpdir, "non-existant"))
	require.ErrorIs(t, err, syscall.ENOENT)

	unreadable := filepath.Join(tmpdir, "dir")
	os.Mkdir(unreadable, 200)
	// open will fail
	_, err = GetKeyValuesFromFiles(unreadable)
	require.ErrorIs(t, err, unix.EACCES)

	valid := filepath.Join(tmpdir, "data")
	err = ioutil.WriteFile(valid, []byte("content"), 0644)
	require.NoError(t, err)
	kvs, err := GetKeyValuesFromFiles(valid)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
}
