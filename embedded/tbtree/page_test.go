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

package tbtree

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLeafPageInsert(t *testing.T) {
	key := make([]byte, 5)
	value := make([]byte, 100)

	n := 0

	ts := uint64(1)

	pg := NewLeafPage()
	for {
		if rand.Int()%2 == 1 {
			rand.Read(key)
		}
		rand.Read(value)

		keySize := 1 + rand.Intn(len(key))
		valueSize := 1 + rand.Intn(len(value))

		e := &Entry{
			Ts:    ts,
			HOff:  OffsetNone,
			Key:   key[:keySize],
			Value: value[:valueSize],
		}

		spaceBeforeInsert := pg.FreeSpace()

		oldEntry, _ := pg.GetEntry(e.Key)
		_, _, err := pg.insertEntry(e)
		if errors.Is(err, ErrPageFull) {
			requiredSize := e.requiredPageItemSize()
			if oldEntry != nil {
				requiredSize -= LeafPageEntryDataSize
			}
			require.Greater(t, e.requiredPageItemSize(), pg.FreeSpace())
			break
		}

		if oldEntry != nil {
			require.Equal(t, spaceBeforeInsert-e.requiredPageItemSize()+LeafPageEntryDataSize, pg.FreeSpace())
		} else {
			n++
			require.Equal(t, spaceBeforeInsert-e.requiredPageItemSize(), pg.FreeSpace())
		}

		entryFound, err := pg.GetEntry(e.Key)
		require.NoError(t, err)
		require.Equal(t, e, entryFound)
		ts++
	}

	require.NotZero(t, n)
	require.Equal(t, pg.NumEntries, uint16(n))
}

func TestInnerPageInsert(t *testing.T) {
	keyBuf := make([]byte, 8)
	n := 0

	pg := NewInnerPage()
	require.Equal(t, pg.UsedSpace(), int(InnerPageEntryDataSize)+8)
	require.Equal(t, int(pg.FreeSpaceStart), int(InnerPageEntryDataSize))
	require.Equal(t, int(pg.FreeSpaceEnd), int(PageSize-PageHeaderDataSize)-8)

	for {
		if rand.Int()%2 == 1 {
			rand.Read(keyBuf)
		}

		keySize := 1 + rand.Intn(len(keyBuf))
		key := keyBuf[:keySize]
		pgID := PageID(rand.Int63())

		spaceBeforeInsert := pg.FreeSpace()

		_, err := pg.Get(key)
		pageExists := err == nil

		_, err = pg.InsertKey(key, pgID)
		if errors.Is(err, ErrPageFull) {
			require.Greater(t, requiredInnerPageItemSize(keySize), pg.FreeSpace())
			break
		}

		if !pageExists {
			if spaceBeforeInsert-requiredInnerPageItemSize(keySize) != pg.FreeSpace() {
				fmt.Println(spaceBeforeInsert, requiredInnerPageItemSize(keySize))
			}
			require.Equal(t, spaceBeforeInsert-requiredInnerPageItemSize(keySize), pg.FreeSpace())
			n++
		} else {
			require.Equal(t, spaceBeforeInsert, pg.FreeSpace())
		}

		id, err := pg.Get(key)
		require.NoError(t, err)
		require.Equal(t, pgID, id)
	}

	require.NotZero(t, n)
	require.Equal(t, pg.NumEntries, uint16(n+1))
}

func TestCompactLeafPage(t *testing.T) {
	pg := NewLeafPage()

	key := make([]byte, 50)
	rand.Read(key)

	value := make([]byte, 100)
	rand.Read(value)

	ts := uint64(1)
	_, _, err := pg.insertEntry(&Entry{
		Ts:    ts,
		Key:   key,
		Value: value[:(1 + rand.Intn(len(value)))],
	})
	require.NoError(t, err)

	ts++
	vs := 0
	expectedUnusedSpace := 0
	for {
		nvs := 1 + rand.Intn(len(value))
		prevEntry, _, err := pg.insertEntry(&Entry{
			Ts:    ts,
			Key:   key,
			Value: value[:nvs],
		})
		if errors.Is(err, ErrPageFull) {
			break
		}

		vs = nvs

		require.NoError(t, err)
		require.NotNil(t, prevEntry)

		expectedUnusedSpace += prevEntry.requiredPageItemSize() - LeafPageEntryDataSize
		ts++
	}

	unusedSpace := int(pg.UnusedSpace)
	require.Equal(t, expectedUnusedSpace, unusedSpace)

	freedSpace, err := pg.compact()
	require.NoError(t, err)
	require.Equal(t, freedSpace, unusedSpace)
	require.Equal(t, 1, int(pg.NumEntries))

	e, err := pg.GetEntry(key)
	require.NoError(t, err)
	require.Equal(t, value[:vs], e.Value)
}

func TestLeafPageSplit(t *testing.T) {
	pgBeforeSplit := NewLeafPage()
	discardedEntry := insertKVsWhileFull(t, pgBeforeSplit)

	entriesBeforeSplit := pgBeforeSplit.NumEntries

	pg := pgBeforeSplit.copy()
	newPage := NewLeafPage()

	idx := pg.splitLeafPage(newPage, discardedEntry)
	require.Equal(t, entriesBeforeSplit+1, pg.NumEntries+newPage.NumEntries)

	if idx < int(pg.NumEntries) {
		foundEntry, err := pg.GetEntryAt(idx)
		require.NoError(t, err)
		require.Equal(t, discardedEntry, &foundEntry)
	} else {
		foundEntry, err := newPage.GetEntryAt(idx - int(pg.NumEntries))
		require.NoError(t, err)
		require.Equal(t, discardedEntry, &foundEntry)
	}

	for i := 0; i < int(pgBeforeSplit.NumEntries); i++ {
		oldEntry, err := pgBeforeSplit.GetEntryAt(i)
		require.NoError(t, err)

		pgEntry, err := pg.GetEntry(oldEntry.Key)
		newPageEntry, err1 := newPage.GetEntry(oldEntry.Key)

		if errors.Is(err, ErrKeyNotFound) {
			require.NoError(t, err1)
			require.Equal(t, &oldEntry, newPageEntry)
		} else {
			require.NoError(t, err)
			require.Equal(t, &oldEntry, pgEntry)
		}
	}
	require.Equal(t, discardedEntry.requiredPageItemSize()+pgBeforeSplit.UsedSpace(), pg.UsedSpace()+newPage.UsedSpace()+int(pgBeforeSplit.UnusedSpace))
}

func TestInnerPageSplit(t *testing.T) {
	pgBeforeSplit := NewInnerPage()

	key, pgID := insertKeysWhileFull(t, pgBeforeSplit)

	entriesBeforeSplit := pgBeforeSplit.NumEntries

	pg := pgBeforeSplit.copy()
	newPage := NewInnerPage()

	idx := pg.splitInnerPage(newPage, key, pgID, 0)

	// NOTE: an additional ghost entry
	// containing an empty key is created on the new page
	require.Equal(t, entriesBeforeSplit+1, pg.NumEntries+newPage.NumEntries)

	switch {
	case idx <= int(pg.NumEntries):
		foundKey := pg.keyAt(idx)
		require.Equal(t, key, foundKey)

		id := pg.ChildPageAt(idx)
		require.Equal(t, pgID, id)
	case idx > int(pg.NumEntries):
		foundKey := newPage.keyAt(idx - int(pg.NumEntries))
		require.Equal(t, key, foundKey)

		id := newPage.ChildPageAt(idx - int(pg.NumEntries))
		require.Equal(t, pgID, id)
	}

	i := 0
	for i < int(pgBeforeSplit.NumEntries) {
		key := pgBeforeSplit.keyAt(i)

		_, err := pg.Get(key)
		if errors.Is(err, ErrKeyNotFound) {
			if idx >= int(pg.NumEntries) {
				require.Equal(t, i, int(pg.NumEntries))
			} else {
				require.Equal(t, i+1, int(pg.NumEntries))

			}
			break
		}
		i++
	}
}

func insertKVsWhileFull(t *testing.T, pg *Page) *Entry {
	e, err := insertKVsUpToSpace(pg, math.MaxInt)
	require.NoError(t, err)
	require.NotNil(t, e)
	return e
}

func insertKeysWhileFull(t *testing.T, pg *Page) ([]byte, PageID) {
	key := make([]byte, 50)
	for {
		rand.Read(key)

		ks := 1 + rand.Intn(len(key))
		pgID := PageID(time.Now().UnixNano())

		_, err := pg.InsertKey(key[:ks], pgID)
		if errors.Is(err, ErrPageFull) {
			return key, pgID
		}
		require.NoError(t, err)
	}
}

func insertKVsUpToSpace(pg *Page, maxSpace int) (*Entry, error) {
	key := make([]byte, 50)
	value := make([]byte, 50)

	for pg.UsedSpace() < maxSpace {
		rand.Read(key)
		rand.Read(value)

		ks := 1 + rand.Intn(len(key))
		vs := 1 + rand.Intn(len(value))

		e := &Entry{
			Ts:    uint64(time.Now().UnixNano()),
			HOff:  OffsetNone,
			Key:   key[:ks],
			Value: value[:vs],
		}

		if pg.UsedSpace()+requiredPageItemSize(ks, vs) > maxSpace {
			return e, nil
		}

		_, _, err := pg.InsertEntry(e)
		if errors.Is(err, ErrPageFull) {
			return e, nil
		}
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func TestSerializePage(t *testing.T) {
	t.Run("serialize header", func(t *testing.T) {
		p := PageHeaderData{
			Flags:          uint16(rand.Int()),
			NumEntries:     uint16(rand.Int()),
			FreeSpaceStart: uint16(rand.Int()),
			FreeSpaceEnd:   uint16(rand.Int()),
			UnusedSpace:    uint16(rand.Int()),
		}

		var buf [PageHeaderDataSize]byte

		n := p.Put(buf[:])
		require.Equal(t, n, PageHeaderDataSize-PageFooterSize)

		var p1 PageHeaderData
		p1.Read(buf[:])
		require.Equal(t, p, p1)
	})

	t.Run("serialize leaf page", func(t *testing.T) {
		var pg Page
		pg.init(true)

		insertKVsWhileFull(t, &pg)
		require.Greater(t, int(pg.NumEntries), 0)

		require.Equal(t, pg.NumEntries*uint16(LeafPageEntryDataSize), pg.FreeSpaceStart)

		pgData := pg.Bytes()
		h := sha256.Sum256(pgData)

		var buf [PageSize]byte
		n := pg.Put(buf[:])
		require.Equal(t, pg.UsedSpace()+PageHeaderDataSize, n)

		var buf1 [PageSize]byte
		m := copy(buf1[PageSize-n:], buf[:n])
		require.Equal(t, n, m)

		recoveredPage, read, err := expandToFixedPage(buf1[:])
		require.NoError(t, err)
		require.Equal(t, read, m)

		require.Equal(t, pg.PageHeaderData, recoveredPage.PageHeaderData)
		for i := pg.FreeSpaceStart; i < pg.FreeSpaceEnd; i++ {
			recoveredPage.data[i] = 0
		}
		h1 := sha256.Sum256(recoveredPage.Bytes())
		require.Equal(t, h, h1)
		require.True(t, pg.IsLeaf())
	})

	t.Run("serialize inner page", func(t *testing.T) {
		var pg Page
		pg.init(false)

		insertKeysWhileFull(t, &pg)
		require.Greater(t, int(pg.NumEntries), 0)

		pgData := pg.Bytes()
		h := sha256.Sum256(pgData)

		var buf [PageSize]byte
		n := pg.Put(buf[:])
		require.Equal(t, pg.UsedSpace()+PageHeaderDataSize, n)

		var buf1 [PageSize]byte
		m := copy(buf1[PageSize-n:], buf[:n])
		require.Equal(t, n, m)

		recoveredPage, read, err := expandToFixedPage(buf1[:])
		require.NoError(t, err)
		require.Equal(t, read, m)

		require.Equal(t, pg.PageHeaderData, recoveredPage.PageHeaderData)
		for i := pg.FreeSpaceStart; i < pg.FreeSpaceEnd; i++ {
			recoveredPage.data[i] = 0
		}

		h1 := sha256.Sum256(recoveredPage.Bytes())
		require.Equal(t, h, h1)
		require.False(t, pg.IsLeaf())
	})
}
