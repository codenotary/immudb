/*
Copyright 2022 Codenotary Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
)

// OngoingTx (no-thread safe) represents an interactive or incremental transaction with support of RYOW.
// The snapshot may be locally modified but isolated from other transactions
type OngoingTx struct {
	st   *ImmuStore
	snap *Snapshot

	entries      []*EntrySpec
	entriesByKey map[[sha256.Size]byte]int

	preconditions []Precondition

	expectedGetsWithFilters []expectedGetWithFilters
	expectedGetsWithPrefix  []expectedGetWithPrefix
	expectedReaders         []*expectedReader

	metadata *TxMetadata

	ts time.Time

	closed bool
}

type expectedGetWithFilters struct {
	key        []byte
	filters    []FilterFn
	expectedTx uint64 // 0 used denotes non-existence
}

type expectedGetWithPrefix struct {
	prefix      []byte
	neq         []byte
	expectedKey []byte
	expectedTx  uint64 // 0 used denotes non-existence
}

type EntrySpec struct {
	Key      []byte
	Metadata *KVMetadata
	Value    []byte
}

func newWriteOnlyTx(s *ImmuStore) (*OngoingTx, error) {
	return &OngoingTx{
		st:           s,
		entriesByKey: make(map[[sha256.Size]byte]int),
		ts:           time.Now(),
	}, nil
}

func newReadWriteTx(s *ImmuStore) (*OngoingTx, error) {
	tx := &OngoingTx{
		st:           s,
		entriesByKey: make(map[[sha256.Size]byte]int),
		ts:           time.Now(),
	}

	precommittedTxID := s.lastPrecommittedTxID()

	err := s.WaitForIndexingUpto(precommittedTxID, nil)
	if err != nil {
		return nil, err
	}

	tx.snap, err = s.SnapshotSince(precommittedTxID)
	if err != nil {
		return nil, err
	}

	// using an "interceptor" to construct the valueRef from current entries
	// so to avoid storing more data into the snapshot
	tx.snap.refInterceptor = func(key []byte, valRef ValueRef) ValueRef {
		keyRef, ok := tx.entriesByKey[sha256.Sum256(key)]
		if !ok {
			return valRef
		}

		entrySpec := tx.entries[keyRef]

		return &ongoingValRef{
			hc:    valRef.HC(),
			value: entrySpec.Value,
			txmd:  tx.metadata,
			kvmd:  entrySpec.Metadata,
		}
	}

	return tx, nil
}

type ongoingValRef struct {
	value []byte
	hc    uint64
	txmd  *TxMetadata
	kvmd  *KVMetadata
}

func (oref *ongoingValRef) Resolve() (val []byte, err error) {
	return oref.value, nil
}

func (oref *ongoingValRef) Tx() uint64 {
	return 0
}

func (oref *ongoingValRef) HC() uint64 {
	return oref.hc
}

func (oref *ongoingValRef) TxMetadata() *TxMetadata {
	return oref.txmd
}

func (oref *ongoingValRef) KVMetadata() *KVMetadata {
	return oref.kvmd
}

func (oref *ongoingValRef) HVal() [sha256.Size]byte {
	return sha256.Sum256(oref.value)
}

func (oref *ongoingValRef) Len() uint32 {
	return uint32(len(oref.value))
}

func (tx *OngoingTx) IsWriteOnly() bool {
	return tx.snap == nil
}

func (tx *OngoingTx) WithMetadata(md *TxMetadata) *OngoingTx {
	tx.metadata = md
	return nil
}

func (tx *OngoingTx) Timestamp() time.Time {
	return tx.ts.Truncate(time.Microsecond).UTC()
}

func (tx *OngoingTx) Metadata() *TxMetadata {
	return tx.metadata
}

func (tx *OngoingTx) Set(key []byte, md *KVMetadata, value []byte) error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	if len(key) == 0 {
		return ErrNullKey
	}

	if len(key) > tx.st.maxKeyLen {
		return ErrorMaxKeyLenExceeded
	}

	if len(value) > tx.st.maxValueLen {
		return ErrorMaxValueLenExceeded
	}

	kid := sha256.Sum256(key)
	keyRef, isKeyUpdate := tx.entriesByKey[kid]

	if !isKeyUpdate && len(tx.entries) > tx.st.maxTxEntries {
		return ErrorMaxTxEntriesLimitExceeded
	}

	// updates are not needed because valueRef are resolved with the "interceptor"
	if !tx.IsWriteOnly() && !isKeyUpdate {
		// vLen=0 + vOff=0 + vHash=0 + txmdLen=0 + kvmdLen=0
		var indexedValue [lszSize + offsetSize + sha256.Size + sszSize + sszSize]byte

		err := tx.snap.set(key, indexedValue[:])
		if err != nil {
			return err
		}
	}

	e := &EntrySpec{
		Key:      key,
		Metadata: md,
		Value:    value,
	}

	if isKeyUpdate {
		tx.entries[keyRef] = e
	} else {
		tx.entries = append(tx.entries, e)
		tx.entriesByKey[kid] = len(tx.entriesByKey)
	}

	return nil
}

func (tx *OngoingTx) AddPrecondition(c Precondition) error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	if c == nil {
		return ErrIllegalArguments
	}

	err := c.Validate(tx.st)
	if err != nil {
		return err
	}

	tx.preconditions = append(tx.preconditions, c)
	return nil
}

func (tx *OngoingTx) GetWithPrefix(prefix, neq []byte) (key []byte, valRef ValueRef, err error) {
	if tx.closed {
		return nil, nil, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return nil, nil, ErrWriteOnlyTx
	}

	key, valRef, err = tx.snap.GetWithPrefix(prefix, neq)
	if errors.Is(err, ErrKeyNotFound) {
		expectedGetWith := expectedGetWithPrefix{
			prefix: prefix,
			neq:    neq,
		}

		tx.expectedGetsWithPrefix = append(tx.expectedGetsWithPrefix, expectedGetWith)
	}
	if err != nil {
		return nil, nil, err
	}

	if valRef.Tx() > 0 {
		// it only requires validation when the entry was pre-existent to ongoing tx
		expectedGetWithPrefix := expectedGetWithPrefix{
			prefix:      prefix,
			neq:         neq,
			expectedKey: key,
			expectedTx:  valRef.Tx(),
		}

		tx.expectedGetsWithPrefix = append(tx.expectedGetsWithPrefix, expectedGetWithPrefix)
	}

	return key, valRef, nil
}

func (tx *OngoingTx) Delete(key []byte) error {
	valRef, err := tx.Get(key)
	if err != nil {
		return err
	}

	if valRef.KVMetadata() != nil && valRef.KVMetadata().Deleted() {
		return ErrKeyNotFound
	}

	md := NewKVMetadata()

	md.AsDeleted(true)

	return tx.Set(key, md, nil)
}

func (tx *OngoingTx) Get(key []byte) (ValueRef, error) {
	return tx.GetWithFilters(key, IgnoreExpired, IgnoreDeleted)
}

func (tx *OngoingTx) GetWithFilters(key []byte, filters ...FilterFn) (ValueRef, error) {
	if tx.closed {
		return nil, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return nil, ErrWriteOnlyTx
	}

	valRef, err := tx.snap.GetWithFilters(key, filters...)
	if errors.Is(err, ErrKeyNotFound) {
		expectedGetWithFilters := expectedGetWithFilters{
			key:        key,
			filters:    filters,
			expectedTx: 0,
		}

		tx.expectedGetsWithFilters = append(tx.expectedGetsWithFilters, expectedGetWithFilters)
	}
	if err != nil {
		return nil, err
	}

	if valRef.Tx() > 0 {
		// it only requires validation when the entry was pre-existent to ongoing tx
		expectedGet := expectedGetWithFilters{
			key:        key,
			filters:    filters,
			expectedTx: valRef.Tx(),
		}

		tx.expectedGetsWithFilters = append(tx.expectedGetsWithFilters, expectedGet)
	}

	return valRef, nil
}

func (tx *OngoingTx) NewKeyReader(spec KeyReaderSpec) (KeyReader, error) {
	if tx.closed {
		return nil, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return nil, ErrWriteOnlyTx
	}

	keyReader, err := tx.snap.NewKeyReader(spec)
	if err != nil {
		return nil, err
	}

	expectedReader := newExpectedReader(spec)

	tx.expectedReaders = append(tx.expectedReaders, expectedReader)

	return newOngoingTxKeyReader(keyReader, expectedReader), nil
}

func (tx *OngoingTx) Commit() (*TxHeader, error) {
	return tx.commit(true)
}

func (tx *OngoingTx) AsyncCommit() (*TxHeader, error) {
	return tx.commit(false)
}

func (tx *OngoingTx) commit(waitForIndexing bool) (*TxHeader, error) {
	if tx.closed {
		return nil, ErrAlreadyClosed
	}

	if !tx.IsWriteOnly() {
		err := tx.snap.Close()
		if err != nil {
			return nil, err
		}
	}

	tx.closed = true

	return tx.st.commit(tx, nil, waitForIndexing)
}

func (tx *OngoingTx) Cancel() error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	if !tx.IsWriteOnly() {
		return tx.snap.Close()
	}

	tx.closed = true

	return nil
}

func (tx *OngoingTx) hasPreconditions() bool {
	return len(tx.preconditions) > 0 ||
		len(tx.expectedGetsWithFilters) > 0 ||
		len(tx.expectedGetsWithPrefix) > 0 ||
		len(tx.expectedReaders) > 0
}

func (tx *OngoingTx) checkPreconditions(st *ImmuStore) error {
	for _, c := range tx.preconditions {
		if c == nil {
			return ErrInvalidPreconditionNull
		}
		ok, err := c.Check(st)
		if err != nil {
			return fmt.Errorf("error checking %s precondition: %w", c, err)
		}
		if !ok {
			return fmt.Errorf("%w: %s", ErrPreconditionFailed, c)
		}
	}

	/*
		if tx.IsWriteOnly() || tx.snap.Ts() >= st.lastPrecommittedTxID() {
			return nil
		}
	*/

	for _, e := range tx.expectedGetsWithFilters {
		valRef, err := st.GetWithFilters(e.key, e.filters...)
		if errors.Is(err, ErrKeyNotFound) {
			if e.expectedTx > 0 {
				return ErrTxReadConflict
			}
			continue
		}
		if err != nil {
			return err
		}

		if e.expectedTx != valRef.Tx() {
			return ErrTxReadConflict
		}
	}

	for _, e := range tx.expectedGetsWithPrefix {
		key, valRef, err := st.GetWithPrefix(e.prefix, e.neq)
		if errors.Is(err, ErrKeyNotFound) {
			if e.expectedTx > 0 {
				return ErrTxReadConflict
			}
			continue
		}
		if err != nil {
			return err
		}

		if !bytes.Equal(e.expectedKey, key) || e.expectedTx != valRef.Tx() {
			return ErrTxReadConflict
		}
	}

	for _, eReader := range tx.expectedReaders {
		tbsnap, err := st.indexer.index.RSnapshot()
		if err != nil {
			return err
		}

		snap := &Snapshot{
			st:   st,
			snap: tbsnap,
			ts:   time.Now(),
		}

		reader, err := snap.NewKeyReader(eReader.spec)
		if err != nil {
			return err
		}

		defer reader.Close()

		for _, eReads := range eReader.expectedReads {
			for _, eRead := range eReads {
				var key []byte
				var valRef ValueRef

				if eRead.initialTxID == 0 && eRead.finalTxID == 0 {
					key, valRef, err = reader.Read()
				} else {
					key, valRef, err = reader.ReadBetween(eRead.initialTxID, eRead.finalTxID)
				}

				if err != nil {
					return err
				}

				if !bytes.Equal(eRead.expectedKey, key) || eRead.expectedTx != valRef.Tx() {
					return ErrTxReadConflict
				}
			}

			err = reader.Reset()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tx *OngoingTx) validateAgainst(hdr *TxHeader) error {
	if hdr == nil {
		return nil
	}

	if len(tx.entries) != hdr.NEntries {
		return fmt.Errorf("%w: number of entries differs", ErrIllegalArguments)
	}

	if tx.metadata != nil {
		if !tx.metadata.Equal(hdr.Metadata) {
			return fmt.Errorf("%w: metadata differs", ErrIllegalArguments)
		}
	} else if hdr.Metadata != nil {
		if !hdr.Metadata.Equal(tx.metadata) {
			return fmt.Errorf("%w: metadata differs", ErrIllegalArguments)
		}
	}

	return nil
}
