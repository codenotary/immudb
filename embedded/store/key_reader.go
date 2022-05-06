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
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/codenotary/immudb/embedded/tbtree"
)

type Snapshot struct {
	st             *ImmuStore
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

type KeyReader struct {
	snap           *Snapshot
	reader         *tbtree.Reader
	filters        []FilterFn
	refInterceptor valueRefInterceptor
	_tx            *Tx
}

type KeyReaderSpec struct {
	SeekKey       []byte
	EndKey        []byte
	Prefix        []byte
	InclusiveSeek bool
	InclusiveEnd  bool
	DescOrder     bool
	Filters       []FilterFn
}

func (s *Snapshot) set(key, value []byte) error {
	return s.snap.Set(key, value)
}

func (s *Snapshot) Get(key []byte) (valRef ValueRef, err error) {
	return s.GetWith(key, IgnoreExpired, IgnoreDeleted)
}

func (s *Snapshot) GetWith(key []byte, filters ...FilterFn) (valRef ValueRef, err error) {
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

func (s *Snapshot) ExistKeyWith(prefix []byte, neq []byte) (bool, error) {
	return s.snap.ExistKeyWith(prefix, neq)
}

func (s *Snapshot) History(key []byte, offset uint64, descOrder bool, limit int) (tss []uint64, hCount uint64, err error) {
	return s.snap.History(key, offset, descOrder, limit)
}

func (s *Snapshot) Ts() uint64 {
	return s.snap.Ts()
}

func (s *Snapshot) Close() error {
	return s.snap.Close()
}

func (s *Snapshot) NewKeyReader(spec *KeyReaderSpec) (*KeyReader, error) {
	if spec == nil {
		return nil, ErrIllegalArguments
	}

	r, err := s.snap.NewReader(&tbtree.ReaderSpec{
		SeekKey:       spec.SeekKey,
		EndKey:        spec.EndKey,
		Prefix:        spec.Prefix,
		InclusiveSeek: spec.InclusiveSeek,
		InclusiveEnd:  spec.InclusiveEnd,
		DescOrder:     spec.DescOrder,
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

	return &KeyReader{
		snap:           s,
		reader:         r,
		filters:        spec.Filters,
		refInterceptor: refInterceptor,
		_tx:            s.st.NewTxHolder(),
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
}

type valueRef struct {
	tx     uint64
	hc     uint64 // version
	hVal   [32]byte
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
			txmd = &TxMetadata{}

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

	_, err = v.st.readValueAt(refVal, v.vOff, v.hVal)
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

func (r *KeyReader) ReadBetween(initialTxID, finalTxID uint64) (key []byte, val ValueRef, tx uint64, err error) {
	key, ktxID, hc, err := r.reader.ReadBetween(initialTxID, finalTxID)
	if err != nil {
		return nil, nil, 0, err
	}

	err = r.snap.st.ReadTx(ktxID, r._tx)
	if err != nil {
		return nil, nil, 0, err
	}

	for _, e := range r._tx.Entries() {
		if bytes.Equal(e.key(), key) {
			val = &valueRef{
				tx:     r._tx.header.ID,
				hc:     hc,
				hVal:   e.hVal,
				vOff:   int64(e.vOff),
				valLen: uint32(e.vLen),
				txmd:   r._tx.header.Metadata,
				kvmd:   e.md,
				st:     r.snap.st,
			}

			for _, filter := range r.filters {
				err = filter(val, r.snap.ts)
				if err != nil {
					return nil, nil, 0, err
				}
			}

			return key, r.refInterceptor(key, val), ktxID, nil
		}
	}

	return nil, nil, 0, ErrUnexpectedError
}

func (r *KeyReader) Read() (key []byte, val ValueRef, err error) {
	for {
		key, indexedVal, tx, hc, err := r.reader.Read()
		if err != nil {
			return nil, nil, err
		}

		val, err = r.snap.st.valueRefFrom(tx, hc, indexedVal)
		if err != nil {
			return nil, nil, err
		}

		skipEntry := false

		for _, filter := range r.filters {
			err = filter(val, r.snap.ts)
			if err != nil {
				skipEntry = true
				break
			}
		}

		if skipEntry {
			continue
		}

		return key, r.refInterceptor(key, val), nil
	}
}

func (r *KeyReader) Reset() error {
	return r.reader.Reset()
}

func (r *KeyReader) Close() error {
	return r.reader.Close()
}
