/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/mocked"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadTxFromCorruptedData(t *testing.T) {

	a := &mocked.MockedAppendable{}

	r := appendable.NewReaderFrom(a, 0, 1)
	require.NotNil(t, r)

	tx := NewTx(1, 32)

	// Should fail while reading TxID
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, errors.New("error")
	}
	err := tx.readFrom(r, false)
	require.Error(t, err)

	// Should fail while reading Ts
	r.Reset()
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		if off < 8 {
			return len(bs), nil
		}
		return 0, errors.New("error")
	}
	err = tx.readFrom(r, false)
	require.Error(t, err)

	// Should fail while reading BlTxID
	r.Reset()
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		if off < 16 {
			return len(bs), nil
		}
		return 0, errors.New("error")
	}
	err = tx.readFrom(r, false)
	require.Error(t, err)

	// Should fail while reading BlRoot
	r.Reset()
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		if off < 24+sha256.Size {
			return len(bs), nil
		}
		return 0, errors.New("error")
	}
	err = tx.readFrom(r, false)
	require.Error(t, err)

	// Should fail while reading PrevAlh
	r.Reset()
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		if off < 24+2*sha256.Size {
			return len(bs), nil
		}
		return 0, errors.New("error")
	}
	err = tx.readFrom(r, false)
	require.Error(t, err)

	// Should fail while reading nentries
	r.Reset()
	a.ReadAtFn = func(bs []byte, off int64) (int, error) {
		if off < 32+2*sha256.Size {
			return len(bs), nil
		}
		return 0, errors.New("error")
	}
	err = tx.readFrom(r, false)
	require.Error(t, err)
}

func TestTxHeaderBytes(t *testing.T) {
	// Default version-0 header
	h := TxHeader{
		ID:       0x01,
		Ts:       0x02,
		BlTxID:   0x03,
		BlRoot:   [sha256.Size]byte{0x04},
		PrevAlh:  [sha256.Size]byte{0x05},
		Metadata: nil,
		NEntries: 0x07,
		Eh:       [sha256.Size]byte{0x08},
	}

	t.Run("encoding of hdr version 0", func(t *testing.T) {
		h.Version = 0
		bytes, err := h.Bytes()
		require.NoError(t, err)
		assert.Equal(t, []byte{
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // ID
			0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // PrevAlh
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, // Ts
			0x0, 0x0, // Version
			0x0, 0x7, // nEntries
			0x8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Eh
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, // BlTxID
			0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // BlRoot
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		}, bytes)
	})

	t.Run("encoding of hdr version 1", func(t *testing.T) {
		h.Version = 1
		bytes, err := h.Bytes()
		require.NoError(t, err)
		assert.Equal(t, []byte{
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // ID
			0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // PrevAlh
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, // Ts
			0x0, 0x1, // Version
			0x0, 0x0, // Metadata size
			0x0, 0x0, 0x0, 0x7, // nEntries
			0x8, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Eh
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, // BlTxID
			0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // BlRoot
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		}, bytes)
	})

	t.Run("encoding of invalid hrd version must end up with an error", func(t *testing.T) {
		h2 := h
		h2.Version = -1
		_, err := h2.Bytes()
		require.ErrorIs(t, err, ErrUnsupportedTxHeaderVersion)
	})

	t.Run("encoding of hrd version 0 must end up with an error if used with non-empty metadata", func(t *testing.T) {
		h2 := h
		h2.Version = 0
		h2.Metadata = NewTxMetadata()
		_, err := h2.Bytes()
		require.NoError(t, err)
		// Currently the tx metadata can only be empty, it will not fail
	})
}

func TestEntryMetadataWithVersions(t *testing.T) {
	entries := []*TxEntry{
		NewTxEntry(
			[]byte("key"),
			nil,
			0,
			[sha256.Size]byte{},
			0,
		),
	}

	tx := NewTxWithEntries(&TxHeader{NEntries: len(entries)}, entries)

	t.Run("calculating TX hash tree for entries without metadata must succeed", func(t *testing.T) {
		tx.header.Version = 0

		err := tx.BuildHashTree()
		require.NoError(t, err)

		tx.header.Version = 1
		err = tx.BuildHashTree()
		require.NoError(t, err)
	})
	t.Run("calculating TX hash tree for entries with empty metadata must succeed", func(t *testing.T) {
		tx.entries[0].md = NewKVMetadata()

		tx.header.Version = 0

		err := tx.BuildHashTree()
		require.NoError(t, err)

		tx.header.Version = 1
		err = tx.BuildHashTree()
		require.NoError(t, err)
	})

	t.Run("calculating TX hash tree for entries with non-empty metadata must fail in version 0", func(t *testing.T) {
		tx.entries[0].md.AsDeleted(true)

		tx.header.Version = 0

		err := tx.BuildHashTree()
		require.ErrorIs(t, err, ErrMetadataUnsupported)

		tx.header.Version = 1
		err = tx.BuildHashTree()
		require.NoError(t, err)
	})

}
