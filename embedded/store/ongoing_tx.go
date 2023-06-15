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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
)

// OngoingTx (no-thread safe) represents an interactive or incremental transaction with support of RYOW.
// The snapshot may be locally modified but isolated from other transactions
type OngoingTx struct {
	st *ImmuStore

	snap       *Snapshot
	readOnly   bool // MVCC validations are not needed for read-only transactions
	unsafeMVCC bool

	requireMVCCOnFollowingTxs bool

	entries      []*EntrySpec
	entriesByKey map[[sha256.Size]byte]int

	preconditions []Precondition

	//MVCC
	expectedGets           []expectedGet
	expectedGetsWithPrefix []expectedGetWithPrefix
	expectedReaders        []*expectedReader
	readsetSize            int

	metadata *TxMetadata

	ts time.Time

	closed bool
}

type expectedGet struct {
	key        []byte
	filters    []FilterFn
	expectedTx uint64 // 0 used to denote non-existence
}

type expectedGetWithPrefix struct {
	prefix      []byte
	neq         []byte
	filters     []FilterFn
	expectedKey []byte
	expectedTx  uint64 // 0 used to denote non-existence
}

type EntrySpec struct {
	Key      []byte
	Metadata *KVMetadata
	Value    []byte
	// hashValue is the hash of the value
	// if the actual value is truncated. This is
	// used during replication.
	HashValue [sha256.Size]byte
	// isValueTruncated is true if the value is
	// truncated. This is used during replication.
	IsValueTruncated bool
}

func newOngoingTx(ctx context.Context, s *ImmuStore, opts *TxOptions) (*OngoingTx, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	tx := &OngoingTx{
		st:           s,
		entriesByKey: make(map[[sha256.Size]byte]int),
		ts:           time.Now(),
		unsafeMVCC:   opts.UnsafeMVCC,
	}

	if opts.Mode == WriteOnlyTx {
		return tx, nil
	}

	tx.readOnly = opts.Mode == ReadOnlyTx

	var snapshotMustIncludeTxID uint64

	if opts.SnapshotMustIncludeTxID != nil {
		snapshotMustIncludeTxID = opts.SnapshotMustIncludeTxID(s.LastPrecommittedTxID())
	}

	mandatoryMVCCUpToTxID := s.MandatoryMVCCUpToTxID()

	if mandatoryMVCCUpToTxID > snapshotMustIncludeTxID {
		snapshotMustIncludeTxID = mandatoryMVCCUpToTxID
	}

	snap, err := s.SnapshotMustIncludeTxIDWithRenewalPeriod(ctx, snapshotMustIncludeTxID, opts.SnapshotRenewalPeriod)
	if err != nil {
		return nil, err
	}

	tx.snap = snap

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

func (tx *OngoingTx) IsReadOnly() bool {
	return tx.readOnly
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

func (tx *OngoingTx) set(key []byte, md *KVMetadata, value []byte, hashValue [sha256.Size]byte, isValueTruncated bool) error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	if tx.readOnly {
		return ErrReadOnlyTx
	}

	if len(key) == 0 {
		return ErrNullKey
	}

	if len(key) > tx.st.maxKeyLen {
		return ErrMaxKeyLenExceeded
	}

	if len(value) > tx.st.maxValueLen {
		return ErrMaxValueLenExceeded
	}

	kid := sha256.Sum256(key)
	keyRef, isKeyUpdate := tx.entriesByKey[kid]

	if !isKeyUpdate && len(tx.entries) > tx.st.maxTxEntries {
		return ErrMaxTxEntriesLimitExceeded
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
		Key:              key,
		Metadata:         md,
		Value:            value,
		HashValue:        hashValue,
		IsValueTruncated: isValueTruncated,
	}

	if isKeyUpdate {
		tx.entries[keyRef] = e
	} else {
		tx.entries = append(tx.entries, e)
		tx.entriesByKey[kid] = len(tx.entriesByKey)
	}

	return nil
}

func (tx *OngoingTx) Set(key []byte, md *KVMetadata, value []byte) error {
	var hashValue [sha256.Size]byte
	return tx.set(key, md, value, hashValue, false)
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

func (tx *OngoingTx) mvccReadSetLimitReached() bool {
	return tx.readsetSize == tx.st.mvccReadSetLimit
}

func (tx *OngoingTx) Delete(key []byte) error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	if tx.readOnly {
		return ErrReadOnlyTx
	}

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
	if !tx.readOnly && errors.Is(err, ErrKeyNotFound) {
		expectedGet := expectedGet{
			key:     cp(key),
			filters: filters,
		}

		if tx.mvccReadSetLimitReached() {
			return nil, ErrMVCCReadSetLimitExceeded
		}

		tx.expectedGets = append(tx.expectedGets, expectedGet)
		tx.readsetSize++
	}
	if err != nil {
		return nil, err
	}

	if !tx.readOnly && valRef.Tx() > 0 {
		// it only requires validation when the entry was pre-existent to ongoing tx
		expectedGet := expectedGet{
			key:        cp(key),
			filters:    filters,
			expectedTx: valRef.Tx(),
		}

		if tx.mvccReadSetLimitReached() {
			return nil, ErrMVCCReadSetLimitExceeded
		}

		tx.expectedGets = append(tx.expectedGets, expectedGet)
		tx.readsetSize++
	}

	return valRef, nil
}

func (tx *OngoingTx) GetWithPrefix(prefix, neq []byte) (key []byte, valRef ValueRef, err error) {
	return tx.GetWithPrefixAndFilters(prefix, neq, IgnoreExpired, IgnoreDeleted)
}

func (tx *OngoingTx) GetWithPrefixAndFilters(prefix, neq []byte, filters ...FilterFn) (key []byte, valRef ValueRef, err error) {
	if tx.closed {
		return nil, nil, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return nil, nil, ErrWriteOnlyTx
	}

	key, valRef, err = tx.snap.GetWithPrefixAndFilters(prefix, neq, filters...)
	if !tx.readOnly && errors.Is(err, ErrKeyNotFound) {
		expectedGetWithPrefix := expectedGetWithPrefix{
			prefix:  cp(prefix),
			neq:     cp(neq),
			filters: filters,
		}

		if tx.mvccReadSetLimitReached() {
			return nil, nil, ErrMVCCReadSetLimitExceeded
		}

		tx.expectedGetsWithPrefix = append(tx.expectedGetsWithPrefix, expectedGetWithPrefix)
		tx.readsetSize++
	}
	if err != nil {
		return nil, nil, err
	}

	if !tx.readOnly && valRef.Tx() > 0 {
		// it only requires validation when the entry was pre-existent to ongoing tx
		expectedGetWithPrefix := expectedGetWithPrefix{
			prefix:      cp(prefix),
			neq:         cp(neq),
			filters:     filters,
			expectedKey: cp(key),
			expectedTx:  valRef.Tx(),
		}

		if tx.mvccReadSetLimitReached() {
			return nil, nil, ErrMVCCReadSetLimitExceeded
		}

		tx.expectedGetsWithPrefix = append(tx.expectedGetsWithPrefix, expectedGetWithPrefix)
		tx.readsetSize++
	}

	return key, valRef, nil
}

func (tx *OngoingTx) NewKeyReader(spec KeyReaderSpec) (KeyReader, error) {
	if tx.closed {
		return nil, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return nil, ErrWriteOnlyTx
	}

	if tx.readOnly {
		return tx.snap.NewKeyReader(spec)
	}

	return newOngoingTxKeyReader(tx, spec)
}

func (tx *OngoingTx) RequireMVCCOnFollowingTxs(requireMVCCOnFollowingTxs bool) error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	tx.requireMVCCOnFollowingTxs = requireMVCCOnFollowingTxs

	return nil
}

func (tx *OngoingTx) Commit(ctx context.Context) (*TxHeader, error) {
	return tx.commit(ctx, true)
}

func (tx *OngoingTx) AsyncCommit(ctx context.Context) (*TxHeader, error) {
	return tx.commit(ctx, false)
}

func (tx *OngoingTx) commit(ctx context.Context, waitForIndexing bool) (*TxHeader, error) {
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

	if tx.readOnly {
		return nil, ErrReadOnlyTx
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return tx.st.commit(ctx, tx, nil, false, waitForIndexing)
}

func (tx *OngoingTx) Cancel() error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	tx.closed = true

	if !tx.IsWriteOnly() {
		return tx.snap.Close()
	}

	return nil
}

func (tx *OngoingTx) Closed() bool {
	return tx.closed
}

func (tx *OngoingTx) hasPreconditions() bool {
	return len(tx.preconditions) > 0 ||
		len(tx.expectedGets) > 0 ||
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

	if tx.IsWriteOnly() || tx.snap.Ts() > st.LastPrecommittedTxID() {
		// read-only transactions or read-write transactions when no other transaction was committed won't be invalidated
		return nil
	}

	// current snapshot is fetched without flushing
	snap, err := st.syncSnapshot()
	if err != nil {
		return err
	}
	defer snap.Close()

	for _, e := range tx.expectedGets {
		valRef, err := snap.GetWithFilters(e.key, e.filters...)
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
		key, valRef, err := snap.GetWithPrefixAndFilters(e.prefix, e.neq, e.filters...)
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
		rspec := KeyReaderSpec{
			SeekKey:       eReader.spec.SeekKey,
			EndKey:        eReader.spec.EndKey,
			Prefix:        eReader.spec.Prefix,
			InclusiveSeek: eReader.spec.InclusiveSeek,
			InclusiveEnd:  eReader.spec.InclusiveEnd,
			DescOrder:     eReader.spec.DescOrder,
		}

		reader, err := snap.NewKeyReader(rspec)
		if err != nil {
			return err
		}

		defer reader.Close()

		for _, eReads := range eReader.expectedReads {
			var key []byte
			var valRef ValueRef

			for _, eRead := range eReads {

				if len(key) == 0 {
					if eRead.initialTxID == 0 && eRead.finalTxID == 0 {
						key, valRef, err = reader.Read()
					} else {
						key, valRef, err = reader.ReadBetween(eRead.initialTxID, eRead.finalTxID)
					}

					if err != nil && !errors.Is(err, ErrNoMoreEntries) {
						return err
					}
				}

				if eRead.expectedNoMoreEntries {
					if err == nil {
						return fmt.Errorf("%w: fetching more entries than expected", ErrTxReadConflict)
					}

					break
				}

				if eRead.expectedTx == 0 {
					if err == nil && bytes.Equal(eRead.expectedKey, key) {
						// key was updated by the transaction
						key = nil
						valRef = nil
					}
				} else {
					if errors.Is(err, ErrNoMoreEntries) {
						return fmt.Errorf("%w: fetching less entries than expected", ErrTxReadConflict)
					}

					if !bytes.Equal(eRead.expectedKey, key) || eRead.expectedTx != valRef.Tx() {
						return fmt.Errorf("%w: fetching a different key or an updated one", ErrTxReadConflict)
					}

					key = nil
					valRef = nil
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

func cp(s []byte) []byte {
	if s == nil {
		return nil
	}

	c := make([]byte, len(s))
	copy(c, s)

	return c
}
