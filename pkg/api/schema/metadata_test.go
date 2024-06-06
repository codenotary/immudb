package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataMarshalUnmarshal(t *testing.T) {
	meta := Metadata{
		"user": "default",
		"ip":   "127.0.0.1:8080",
	}

	data, err := meta.Marshal()
	require.NoError(t, err)

	t.Run("valid metadata", func(t *testing.T) {
		unmarshalled := Metadata{}
		err := unmarshalled.Unmarshal(data)
		require.NoError(t, err)
		require.Equal(t, meta, unmarshalled)
	})

	t.Run("corrupted metadata", func(t *testing.T) {
		unmarshalled := Metadata{}
		err := unmarshalled.Unmarshal(data[:len(data)/2])
		require.ErrorIs(t, err, ErrCorruptedMetadata)
	})

	t.Run("empty metadata", func(t *testing.T) {
		m := Metadata{}
		data, err := m.Marshal()
		require.NoError(t, err)
		require.Empty(t, data)

		unmarshalled := Metadata{}
		err = unmarshalled.Unmarshal([]byte{})
		require.NoError(t, err)
		require.Empty(t, unmarshalled)
	})

	t.Run("invalid metadata", func(t *testing.T) {
		x := make([]byte, 256)

		m := Metadata{"x": string(x)}
		_, err := m.Marshal()
		require.ErrorIs(t, err, ErrMetadataTooLarge)

		m = Metadata{"": "v"}
		_, err = m.Marshal()
		require.ErrorIs(t, err, ErrEmptyMetadataKey)

		m = Metadata{"k": ""}
		_, err = m.Marshal()
		require.ErrorIs(t, err, ErrEmptyMetadataValue)
	})
}
