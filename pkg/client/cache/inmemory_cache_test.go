package cache

import (
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestInMemoryCache(t *testing.T) {
	imc := NewInMemoryCache()
	require.IsType(t, &inMemoryCache{}, imc)

	err := imc.Set(&schema.Root{Payload: &schema.RootIndex{Index: 11, Root: []byte{11}}}, "server1", "db11")
	require.NoError(t, err)
	err = imc.Set(&schema.Root{Payload: &schema.RootIndex{Index: 12, Root: []byte{12}}}, "server1", "db12")
	require.NoError(t, err)
	err = imc.Set(&schema.Root{Payload: &schema.RootIndex{Index: 21, Root: []byte{21}}}, "server2", "db21")
	require.NoError(t, err)

	root, err := imc.Get("server1", "db11")
	require.NoError(t, err)
	require.Equal(t, uint64(11), root.Payload.Index)
	require.Equal(t, []byte{11}, root.Payload.Root)

	root, err = imc.Get("server1", "db12")
	require.NoError(t, err)
	require.Equal(t, uint64(12), root.Payload.Index)
	require.Equal(t, []byte{12}, root.Payload.Root)

	root, err = imc.Get("server2", "db21")
	require.NoError(t, err)
	require.Equal(t, uint64(21), root.Payload.Index)
	require.Equal(t, []byte{21}, root.Payload.Root)

	_, err = imc.Get("unknownServer", "db11")
	require.Error(t, err)
	_, err = imc.Get("server1", "unknownDb")
	require.Error(t, err)
}
