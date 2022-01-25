package embed

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestImmudb_Open(t *testing.T) {

	db, err := Open()
	defer func() {
		os.RemoveAll(filepath.Join(DefaultDir, DefaultDatabseName))
	}()

	require.NoError(t, err)
	err = db.VerifiedSet([]byte("key"), []byte("value"))
	require.NoError(t, err)
}
