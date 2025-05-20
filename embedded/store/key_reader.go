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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/codenotary/immudb/v2/embedded/tbtree"
)

type Snapshot struct {
	st             *Ledger
	prefix         []byte
	snap           tbtree.Snapshot
	ts             time.Time
	refInterceptor valueRefInterceptor
}

type valueRefInterceptor func(key []byte, valRef ValueRef) ValueRef

// filter out entries when filter evaluates to a non-nil error
type FilterFn func(valRef ValueRef, t time.Time) error

var (
	IgnoreDeleted FilterFn = func(valRef ValueRef, t time.Time) error {
		md := valRef.KVMetadata()
		if md != nil && md.Deleted() {
			return ErrKeyNotFound
		}
		return nil
	}

	IgnoreExpired FilterFn = func(valRef ValueRef, t time.Time) error {
		md := valRef.KVMetadata()
		if md != nil && md.ExpiredAt(t) {
			return ErrExpiredEntry
		}
		return nil
	}
)

type KeyReader interface {
	Read(ctx context.Context) (key []byte, val ValueRef, err error)
	Close() error
}

type KeyReaderSpec struct {
	SeekKey        []byte
	EndKey         []byte
	Prefix         []byte
	InclusiveSeek  bool
	InclusiveEnd   bool
	IncludeHistory bool
	DescOrder      bool
	Filters        []FilterFn
	Offset         uint64
	StartTx        uint64
	EndTx          uint64
}

func (s *Snapshot) set(key, value []byte) error {
	return s.snap.Set(key, value)
}

func (s *Snapshot) Get(ctx context.Context, key []byte) (valRef ValueRef, err error) {
	return s.GetWithFilters(ctx, key, IgnoreExpired, IgnoreDeleted)
}

func (s *Snapshot) GetBetween(ctx context.Context, key []byte, initialTxID, finalTxID uint64) (valRef ValueRef, err error) {
	indexedVal, tx, hc, err := s.snap.GetBetween(key, initialTxID, finalTxID)
	if err != nil {
		return nil, err
	}

	valRef, err = s.st.valueRefFrom(tx, hc, indexedVal)
	if err != nil {
		return nil, err
	}

	if s.refInterceptor != nil {
		return s.refInterceptor(key, valRef), nil
	}

	return valRef, nil
}

func (s *Snapshot) GetWithFilters(ctx context.Context, key []byte, filters ...FilterFn) (valRef ValueRef, err error) {
	indexedVal, tx, hc, err := s.snap.Get(key)
	if err != nil {
		return nil, err
	}

	valRef, err = s.st.valueRefFrom(tx, hc, indexedVal)
	if err != nil {
		return nil, err
	}

	for _, filter := range filters {
		if filter == nil {
			return nil, fmt.Errorf("%w: invalid filter function", ErrIllegalArguments)
		}

		err = filter(valRef, s.ts)
		if err != nil {
			return nil, err
		}
	}

	if s.refInterceptor != nil {
		return s.refInterceptor(key, valRef), nil
	}

	return valRef, nil
}

func (s *Snapshot) GetWithPrefix(ctx context.Context, prefix []byte, neq []byte) (key []byte, valRef ValueRef, err error) {
	return s.GetWithPrefixAndFilters(ctx, prefix, neq, IgnoreExpired, IgnoreDeleted)
}

func (s *Snapshot) GetWithPrefixAndFilters(ctx context.Context, prefix []byte, neq []byte, filters ...FilterFn) (key []byte, valRef ValueRef, err error) {
	key, indexedVal, tx, hc, err := s.snap.GetWithPrefix(prefix, neq)
	if err != nil {
		return nil, nil, err
	}

	valRef, err = s.st.valueRefFrom(tx, hc, indexedVal)
	if err != nil {
		return nil, nil, err
	}

	for _, filter := range filters {
		if filter == nil {
			return nil, nil, fmt.Errorf("%w: invalid filter function", ErrIllegalArguments)
		}

		err = filter(valRef, s.ts)
		if err != nil {
			return nil, nil, err
		}
	}

	if s.refInterceptor != nil {
		return key, s.refInterceptor(key, valRef), nil
	}

	return key, valRef, nil
}

func (s *Snapshot) History(key []byte, offset uint64, descOrder bool, limit int) (valRefs []ValueRef, hCount uint64, err error) {
	timedValues, hCount, err := s.snap.History(key, offset, descOrder, limit)
	if err != nil {
		return nil, 0, err
	}

	valRefs = make([]ValueRef, len(timedValues))

	for i, timedValue := range timedValues {
		valRef, err := s.st.valueRefFrom(timedValue.Ts, hCount-uint64(i), timedValue.Value)
		if err != nil {
			return nil, 0, err
		}

		if s.refInterceptor != nil {
			valRef = s.refInterceptor(key, valRef)
		}

		valRefs[i] = valRef
	}

	return valRefs, hCount, nil
}

func (s *Snapshot) Ts() uint64 {
	return s.snap.Ts()
}

func (s *Snapshot) Close() error {
	return s.snap.Close()
}

func identityValueRefInterceptor(key []byte, valRef ValueRef) ValueRef {
	return valRef
}

func (s *Snapshot) valueRefInterceptor() valueRefInterceptor {
	if s.refInterceptor != nil {
		return s.refInterceptor
	}
	return identityValueRefInterceptor
}

func (s *Snapshot) NewKeyReader(spec KeyReaderSpec) (KeyReader, error) {
	for _, filter := range spec.Filters {
		if filter == nil {
			return nil, fmt.Errorf("%w: invalid filter function", ErrIllegalArguments)
		}
	}

	seekSpec, err := seekSpec(spec)
	if err != nil {
		return nil, err
	}

	it, err := prepareIterator(seekSpec, s.snap)
	if err != nil {
		return nil, err
	}

	return &storeKeyReader{
		snap:           s,
		spec:           seekSpec,
		it:             it,
		refInterceptor: s.valueRefInterceptor(),
	}, nil
}

type ValueRef interface {
	Resolve() (io.Reader, error)
	TxID() uint64
	Revision() uint64
	Hash() [sha256.Size]byte
	Offset() int64
	Len() uint32
	TxMetadata() *TxMetadata
	KVMetadata() *KVMetadata
}

// Resolve reads the value referenced by the given ValueRef and updates the value cache.
// It returns the value as a byte slice or an error if the operation fails.
func (s *Ledger) Resolve(vRef ValueRef) ([]byte, error) {
	r, err := vRef.Resolve()
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, nil
	}

	v, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	err = s.updateValueCache(vRef.Offset(), v)
	return v, err
}

type valueRef struct {
	tx       uint64
	revision uint64
	hVal     [sha256.Size]byte
	vOff     int64
	valLen   uint32
	txmd     *TxMetadata
	kvmd     *KVMetadata
	st       *Ledger
}

func (st *Ledger) valueRefFrom(tx, hc uint64, indexedVal []byte) (ValueRef, error) {
	// vLen + vOff + vHash
	const valrLen = lszSize + offsetSize + sha256.Size

	if len(indexedVal) < valrLen {
		return nil, ErrCorruptedIndex
	}

	i := 0

	valLen := binary.BigEndian.Uint32(indexedVal[i:])
	i += lszSize

	vOff := int64(binary.BigEndian.Uint64(indexedVal[i:]))
	i += offsetSize

	var hVal [sha256.Size]byte
	copy(hVal[:], indexedVal[i:])
	i += sha256.Size

	var txmd *TxMetadata
	var kvmd *KVMetadata

	if len(indexedVal) > i {
		// index created with metadata fields
		// vLen + vOff + vHash + txmdLen + txmd + kvmdLen + kvmd
		if len(indexedVal) < i+2*sszSize {
			return nil, ErrCorruptedIndex
		}

		txmdLen := int(binary.BigEndian.Uint16(indexedVal[i:]))
		i += sszSize

		if txmdLen > maxTxMetadataLen || len(indexedVal) < i+txmdLen+sszSize {
			return nil, ErrCorruptedIndex
		}

		if txmdLen > 0 {
			txmd = NewTxMetadata()

			err := txmd.ReadFrom(indexedVal[i : i+txmdLen])
			if err != nil {
				return nil, err
			}
			i += txmdLen
		}

		kvmdLen := int(binary.BigEndian.Uint16(indexedVal[i:]))
		i += sszSize

		if kvmdLen > maxKVMetadataLen || len(indexedVal) < i+kvmdLen {
			return nil, ErrCorruptedIndex
		}

		if kvmdLen > 0 {
			kvmd = newReadOnlyKVMetadata()

			err := kvmd.unsafeReadFrom(indexedVal[i : i+kvmdLen])
			if err != nil {
				return nil, err
			}
			i += kvmdLen
		}
	}

	if len(indexedVal) > i {
		return nil, ErrCorruptedIndex
	}

	return &valueRef{
		tx:       tx,
		revision: hc,
		hVal:     hVal,
		vOff:     vOff,
		valLen:   valLen,
		txmd:     txmd,
		kvmd:     kvmd,
		st:       st,
	}, nil
}

// Resolve returns the value as a byte slice.
func (v *valueRef) Resolve() (io.Reader, error) {
	refVal := make([]byte, v.valLen)
	if v.kvmd != nil && v.kvmd.ExpiredAt(time.Now()) {
		return nil, ErrExpiredEntry
	}

	if v.valLen == 0 {
		// while not required, nil is returned instead of an empty slice

		// TODO: this step should be done after reading the value to ensure proper validations are made
		// But current changes in ExportTx with truncated transactions are not providing the value length
		// for truncated transactions
		return nil, nil
	}

	_, err := v.st.readValueAt(refVal, v.vOff, v.hVal, false)
	if err != nil {
		return nil, err
	}

	// TODO: should read value lazily.
	return bytes.NewReader(refVal), nil
}

func (v *valueRef) TxID() uint64 {
	return v.tx
}

func (v *valueRef) Revision() uint64 {
	return v.revision
}

func (v *valueRef) TxMetadata() *TxMetadata {
	return v.txmd
}

func (v *valueRef) KVMetadata() *KVMetadata {
	return v.kvmd
}

func (v *valueRef) Hash() [sha256.Size]byte {
	return v.hVal
}

func (v *valueRef) Len() uint32 {
	return v.valLen
}

func (v *valueRef) Offset() int64 {
	return v.vOff
}

type storeKeyReader struct {
	snap *Snapshot
	spec KeyReaderSpec

	it             tbtree.Iterator
	refInterceptor valueRefInterceptor
	skipped        uint64
	end            bool
}

func (r *storeKeyReader) Read(ctx context.Context) ([]byte, ValueRef, error) {
	for {
		// TODO: remember to copy key
		e, err := r.next()
		if err != nil {
			return nil, nil, err
		}

		if len(r.spec.Prefix) > 0 && !bytes.HasPrefix(e.Key, r.spec.Prefix) {
			continue
		}

		val, err := r.snap.st.valueRefFrom(e.Ts, e.HC+1, e.Value)
		if err != nil {
			return nil, nil, err
		}

		valRef := r.refInterceptor(e.Key, val)
		if r.skip(valRef) {
			continue
		}

		if r.skipped < r.spec.Offset {
			r.skipped++
			continue
		}

		return e.Key, valRef, nil
	}
}

func (r *storeKeyReader) skip(ref ValueRef) bool {
	if r.spec.IncludeHistory {
		return false
	}

	for _, filter := range r.spec.Filters {
		if err := filter(ref, r.snap.ts); err != nil {
			return true
		}
	}
	return false
}

func prepareIterator(spec KeyReaderSpec, snap tbtree.Snapshot) (tbtree.Iterator, error) {
	opts := tbtree.DefaultIteratorOptions()
	opts.StartTs = spec.StartTx

	if spec.EndTx == 0 {
		opts.EndTs = math.MaxUint64
	} else {
		opts.EndTs = spec.EndTx
	}
	opts.Reversed = spec.DescOrder
	opts.IncludeHistory = spec.IncludeHistory

	it, err := snap.NewIterator(opts)
	if err != nil {
		return nil, err
	}
	err = it.Seek(spec.SeekKey)
	return it, err
}

func (r *storeKeyReader) next() (*tbtree.Entry, error) {
	if r.end {
		return nil, ErrNoMoreEntries
	}

	it := r.it

	e, err := it.Next()
	if err != nil {
		return nil, err
	}

	if !r.spec.InclusiveSeek && bytes.Equal(e.Key, r.spec.SeekKey) {
		e, err = it.Next()
		if err != nil {
			return nil, err
		}
	}

	if len(r.spec.EndKey) > 0 {
		cmp := bytes.Compare(r.spec.EndKey, e.Key)

		if r.spec.DescOrder && (cmp > 0 || (cmp == 0 && !r.spec.InclusiveEnd)) {
			return nil, ErrNoMoreEntries
		}

		if !r.spec.DescOrder && (cmp < 0 || (cmp == 0 && !r.spec.InclusiveEnd)) {
			return nil, ErrNoMoreEntries
		}
	}
	return e, nil
}

func (r *storeKeyReader) Close() error {
	return r.it.Close()
}

func seekSpec(spec KeyReaderSpec) (KeyReaderSpec, error) {
	if len(spec.SeekKey) > tbtree.MaxEntrySize || len(spec.Prefix) > tbtree.MaxEntrySize {
		return KeyReaderSpec{}, ErrIllegalArguments
	}

	greatestPrefixedKey := greatestKeyOfSize(tbtree.MaxEntrySize)
	copy(greatestPrefixedKey, spec.Prefix)

	// Adjust seekKey based on key prefix
	seekKey := spec.SeekKey
	inclusiveSeek := spec.InclusiveSeek

	if spec.DescOrder {
		if len(spec.SeekKey) == 0 || bytes.Compare(spec.SeekKey, greatestPrefixedKey) > 0 {
			seekKey = greatestPrefixedKey
			inclusiveSeek = true
		}
	} else {
		if bytes.Compare(spec.SeekKey, spec.Prefix) < 0 {
			seekKey = spec.Prefix
			inclusiveSeek = true
		}
	}

	// Adjust endKey based on key prefix
	endKey := spec.EndKey
	inclusiveEnd := spec.InclusiveEnd

	if spec.DescOrder {
		if bytes.Compare(spec.EndKey, spec.Prefix) < 0 {
			endKey = spec.Prefix
			inclusiveEnd = true
		}
	} else {
		if len(spec.EndKey) == 0 || bytes.Compare(spec.EndKey, greatestPrefixedKey) > 0 {
			endKey = greatestPrefixedKey
			inclusiveEnd = true
		}
	}

	return KeyReaderSpec{
		SeekKey:        seekKey,
		EndKey:         endKey,
		Prefix:         spec.Prefix,
		InclusiveSeek:  inclusiveSeek,
		InclusiveEnd:   inclusiveEnd,
		IncludeHistory: spec.IncludeHistory,
		DescOrder:      spec.DescOrder,
		Filters:        spec.Filters,
		Offset:         spec.Offset,
		StartTx:        spec.StartTx,
		EndTx:          spec.EndTx,
	}, nil
}

func greatestKeyOfSize(size int) []byte {
	k := make([]byte, size)
	for i := 0; i < size; i++ {
		k[i] = 0xFF
	}
	return k
}
