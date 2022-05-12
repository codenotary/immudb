/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"crypto/sha256"
	"fmt"
)

//OngoingTx (no-thread safe) represents an interactive or incremental transaction with support of RYOW.
//The snapshot may be locally modified but isolated from other transactions
type OngoingTx struct {
	st   *ImmuStore
	snap *Snapshot

	entries      []*EntrySpec
	entriesByKey map[[sha256.Size]byte]int

	preconditions []Precondition

	metadata *TxMetadata

	closed bool
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
	}, nil
}

func newReadWriteTx(s *ImmuStore) (*OngoingTx, error) {
	tx := &OngoingTx{
		st:           s,
		entriesByKey: make(map[[sha256.Size]byte]int),
	}

	err := s.WaitForIndexingUpto(s.committedTxID, nil)
	if err != nil {
		return nil, err
	}

	tx.snap, err = s.SnapshotSince(s.committedTxID)
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

func (tx *OngoingTx) ExistKeyWith(prefix, neq []byte) (bool, error) {
	if tx.closed {
		return false, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return false, ErrWriteOnlyTx
	}

	return tx.snap.ExistKeyWith(prefix, neq)
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
	return tx.GetWith(key, IgnoreExpired, IgnoreDeleted)
}

func (tx *OngoingTx) GetWith(key []byte, filters ...FilterFn) (ValueRef, error) {
	if tx.closed {
		return nil, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return nil, ErrWriteOnlyTx
	}

	return tx.snap.GetWith(key, filters...)
}

func (tx *OngoingTx) NewKeyReader(spec *KeyReaderSpec) (*KeyReader, error) {
	if tx.closed {
		return nil, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return nil, ErrWriteOnlyTx
	}

	return tx.snap.NewKeyReader(spec)
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
	return len(tx.preconditions) > 0
}

func (tx *OngoingTx) checkPreconditions(idx KeyIndex) error {
	for _, c := range tx.preconditions {
		if c == nil {
			return ErrInvalidPreconditionNull
		}
		ok, err := c.Check(idx)
		if err != nil {
			return fmt.Errorf("error checking %s precondition: %w", c, err)
		}
		if !ok {
			return fmt.Errorf("%w: %s", ErrPreconditionFailed, c)
		}
	}
	return nil
}
