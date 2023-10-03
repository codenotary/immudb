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
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/codenotary/immudb/embedded/tbtree"
)

type Snapshot struct {
	st             *ImmuStore
	prefix         []byte
	snap           *tbtree.Snapshot
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
	ReadBetween(ctx context.Context, initialTxID uint64, finalTxID uint64) (key []byte, val ValueRef, err error)
	Reset() error
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

func (s *Snapshot) NewKeyReader(spec KeyReaderSpec) (KeyReader, error) {
	r, err := s.snap.NewReader(tbtree.ReaderSpec{
		SeekKey:        spec.SeekKey,
		EndKey:         spec.EndKey,
		Prefix:         spec.Prefix,
		InclusiveSeek:  spec.InclusiveSeek,
		InclusiveEnd:   spec.InclusiveEnd,
		IncludeHistory: spec.IncludeHistory,
		DescOrder:      spec.DescOrder,
	})
	if err != nil {
		return nil, err
	}

	var refInterceptor valueRefInterceptor

	if s.refInterceptor == nil {
		refInterceptor = func(key []byte, valRef ValueRef) ValueRef {
			return valRef
		}
	} else {
		refInterceptor = s.refInterceptor
	}

	for _, filter := range spec.Filters {
		if filter == nil {
			return nil, fmt.Errorf("%w: invalid filter function", ErrIllegalArguments)
		}
	}

	return &storeKeyReader{
		snap:           s,
		reader:         r,
		filters:        spec.Filters,
		includeHistory: spec.IncludeHistory,
		refInterceptor: refInterceptor,
		offset:         spec.Offset,
	}, nil
}

type ValueRef interface {
	Resolve() (val []byte, err error)
	Tx() uint64
	HC() uint64
	TxMetadata() *TxMetadata
	KVMetadata() *KVMetadata
	HVal() [sha256.Size]byte
	Len() uint32
	VOff() int64
}

type valueRef struct {
	tx     uint64
	hc     uint64 // version
	hVal   [sha256.Size]byte
	vOff   int64
	valLen uint32
	txmd   *TxMetadata
	kvmd   *KVMetadata
	st     *ImmuStore
}

func (st *ImmuStore) valueRefFrom(tx, hc uint64, indexedVal []byte) (ValueRef, error) {
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
		tx:     tx,
		hc:     hc,
		hVal:   hVal,
		vOff:   vOff,
		valLen: valLen,
		txmd:   txmd,
		kvmd:   kvmd,
		st:     st,
	}, nil
}

// Resolve ...
func (v *valueRef) Resolve() (val []byte, err error) {
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

	_, err = v.st.readValueAt(refVal, v.vOff, v.hVal, false)
	if err != nil {
		return nil, err
	}

	return refVal, nil
}

func (v *valueRef) Tx() uint64 {
	return v.tx
}

func (v *valueRef) HC() uint64 {
	return v.hc
}

func (v *valueRef) TxMetadata() *TxMetadata {
	return v.txmd
}

func (v *valueRef) KVMetadata() *KVMetadata {
	return v.kvmd
}

func (v *valueRef) HVal() [sha256.Size]byte {
	return v.hVal
}

func (v *valueRef) Len() uint32 {
	return v.valLen
}

func (v *valueRef) VOff() int64 {
	return v.vOff
}

type storeKeyReader struct {
	snap           *Snapshot
	reader         *tbtree.Reader
	filters        []FilterFn
	includeHistory bool

	refInterceptor valueRefInterceptor

	offset  uint64
	skipped uint64
}

func (r *storeKeyReader) ReadBetween(ctx context.Context, initialTxID, finalTxID uint64) (key []byte, val ValueRef, err error) {
	for {
		key, indexedVal, tx, hc, err := r.reader.ReadBetween(initialTxID, finalTxID)
		if err != nil {
			return nil, nil, err
		}

		val, err = r.snap.st.valueRefFrom(tx, hc, indexedVal)
		if err != nil {
			return nil, nil, err
		}

		valRef := r.refInterceptor(key, val)

		filterEntry := false

		if !r.includeHistory {
			for _, filter := range r.filters {
				err = filter(valRef, r.snap.ts)
				if err != nil {
					filterEntry = true
					break
				}
			}
		}

		if filterEntry {
			continue
		}

		if r.skipped < r.offset {
			r.skipped++
			continue
		}

		return key, valRef, nil
	}
}

func (r *storeKeyReader) Read(ctx context.Context) (key []byte, val ValueRef, err error) {
	for {
		key, indexedVal, tx, hc, err := r.reader.Read()
		if err != nil {
			return nil, nil, err
		}

		val, err = r.snap.st.valueRefFrom(tx, hc, indexedVal)
		if err != nil {
			return nil, nil, err
		}

		valRef := r.refInterceptor(key, val)

		filterEntry := false

		if !r.includeHistory {
			for _, filter := range r.filters {
				err = filter(valRef, r.snap.ts)
				if err != nil {
					filterEntry = true
					break
				}
			}
		}

		if filterEntry {
			continue
		}

		if r.skipped < r.offset {
			r.skipped++
			continue
		}

		return key, valRef, nil
	}
}

func (r *storeKeyReader) Reset() error {
	err := r.reader.Reset()
	if err != nil {
		return err
	}

	r.skipped = 0

	return nil
}

func (r *storeKeyReader) Close() error {
	return r.reader.Close()
}
