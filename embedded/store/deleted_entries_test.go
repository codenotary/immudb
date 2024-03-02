package store

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeletedEntryIsNotAccesibleFromReadEntry(t *testing.T) {
	var key = []byte("test-key")

	immuStore, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	defer immustoreClose(t, immuStore)

	setKeys(t, immuStore, key, 1)

	entry, _, err := immuStore.ReadTxEntry(1, key, true)
	require.NoError(t, err)

	val, err := immuStore.ReadValue(entry)
	require.NoError(t, err)
	require.Equal(t, val, []byte("test-value-0"))

	deleteKey(t, immuStore, key)

	val, err = immuStore.ReadValue(entry)
	require.ErrorIs(t, err, ErrValueDeleted)
	require.Nil(t, val)
}

func TestDeletedEntriesAreNotAccessibleFromGetBetween(t *testing.T) {
	var (
		nRecordsBeforeDelete = 100
		key                  = []byte("test-key")
	)

	immuStore, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	defer immustoreClose(t, immuStore)

	setKeys(t, immuStore, key, nRecordsBeforeDelete)
	deleteKey(t, immuStore, key)

	for i := 0; i < nRecordsBeforeDelete; i++ {
		valRef, err := immuStore.GetBetween(context.Background(), key, 0, uint64(i))
		require.NoError(t, err)

		val, err := valRef.Resolve()
		require.Nil(t, val)
		require.ErrorIs(t, err, ErrValueDeleted)
	}

	valRef, err := immuStore.GetBetween(context.Background(), key, 0, uint64(nRecordsBeforeDelete+1))
	require.NoError(t, err)

	val, err := valRef.Resolve()
	require.NoError(t, err)
	require.Nil(t, val)

	md := valRef.KVMetadata()
	require.NotNil(t, md)
	require.True(t, md.Deleted())
}

func TestDeletedEntriesAreNotAccessibleFromHistory(t *testing.T) {
	var (
		nRecordsBeforeDelete = 100
		nRecordsAfterDelete  = 10
		key                  = []byte("test-key")
	)

	immuStore, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	defer immustoreClose(t, immuStore)

	setKeys(t, immuStore, key, nRecordsBeforeDelete)

	valRefs, _, err := immuStore.History(key, 0, false, nRecordsBeforeDelete)
	require.NoError(t, err)
	assertValuesAreVisible(t, valRefs, false)

	deleteKey(t, immuStore, key)
	setKeys(t, immuStore, key, nRecordsAfterDelete)

	t.Run("requesting deleted records only", func(t *testing.T) {
		valRefs, _, err = immuStore.History(key, uint64(nRecordsBeforeDelete/2), false, nRecordsBeforeDelete/2)
		require.NoError(t, err)

		assertValuesAreDeleted(t, valRefs)

		valRefs, _, err = immuStore.History(key, uint64(nRecordsAfterDelete+1), true, nRecordsBeforeDelete)
		require.NoError(t, err)

		assertValuesAreDeleted(t, valRefs)
	})

	t.Run("requesting deleted and non deleted records", func(t *testing.T) {
		valRefs, _, err = immuStore.History(key, uint64(nRecordsBeforeDelete/2), false, nRecordsBeforeDelete+1+nRecordsAfterDelete)
		require.NoError(t, err)

		assertValuesAreDeleted(t, valRefs[:(nRecordsBeforeDelete/2)])
		assertIsDeleteEntry(t, valRefs[nRecordsBeforeDelete/2])
		assertValuesAreVisible(t, valRefs[(nRecordsBeforeDelete/2+1):], false)

		valRefs, _, err = immuStore.History(key, 0, true, nRecordsBeforeDelete+1+nRecordsAfterDelete)
		require.NoError(t, err)

		assertValuesAreVisible(t, valRefs[:nRecordsAfterDelete], true)
		assertIsDeleteEntry(t, valRefs[nRecordsAfterDelete])
		assertValuesAreDeleted(t, valRefs[nRecordsAfterDelete+1:])
	})
}

func assertIsDeleteEntry(t *testing.T, valRef ValueRef) {
	require.NotNil(t, valRef.KVMetadata())
	require.True(t, valRef.KVMetadata().Deleted())

	value, err := valRef.Resolve()
	require.NoError(t, err)
	require.Nil(t, value)
}

func TestExpiredEntryIsNotAccessibleFromHistory(t *testing.T) {
	var (
		key      = []byte("test-key")
		nRecords = 100
	)

	immuStore, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	defer immustoreClose(t, immuStore)

	expiredRecordIndex := rand.Intn(nRecords)
	setKeys(t, immuStore, key, expiredRecordIndex)

	tx, err := immuStore.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	md := NewKVMetadata()
	err = md.ExpiresAt(time.Now().Add(-time.Second))
	require.NoError(t, err)

	err = tx.Set(key, md, []byte("expired-value"))
	require.NoError(t, err)

	_, err = tx.Commit(context.Background())
	require.NoError(t, err)

	setKeys(t, immuStore, key, nRecords-expiredRecordIndex-1)

	valRefs, _, err := immuStore.History(key, 0, false, nRecords)
	require.NoError(t, err)
	require.Len(t, valRefs, nRecords)

	for i, valRef := range valRefs {
		value, err := valRef.Resolve()
		if i == expiredRecordIndex {
			require.ErrorIs(t, err, ErrValueDeleted)
			require.Nil(t, value)
		} else {
			require.NoError(t, err)
		}
	}
}

func assertValuesAreVisible(t *testing.T, valRefs []ValueRef, reverse bool) {
	for i, valRef := range valRefs {
		value, err := valRef.Resolve()
		require.NoError(t, err)

		expectedIdx := i
		if reverse {
			expectedIdx = len(valRefs) - 1 - i
		}
		require.Equal(t, []byte(fmt.Sprintf("test-value-%d", expectedIdx)), value)
	}
}

func assertValuesAreDeleted(t *testing.T, valRefs []ValueRef) {
	for i := 0; i < len(valRefs)-1; i++ {
		value, err := valRefs[i].Resolve()
		require.Nil(t, value)
		require.ErrorIs(t, err, ErrValueDeleted)
	}
}

func setKeys(t *testing.T, store *ImmuStore, key []byte, nRecords int) {
	for i := 0; i < nRecords; i++ {
		tx, err := store.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		err = tx.Set(key, nil, []byte(fmt.Sprintf("test-value-%d", i)))
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)
	}
}

func deleteKey(t *testing.T, store *ImmuStore, key []byte) {
	tx, err := store.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	err = tx.Delete(context.Background(), key)
	require.NoError(t, err)

	_, err = tx.Commit(context.Background())
	require.NoError(t, err)
}
