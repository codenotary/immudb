/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

	"github.com/codenotary/immudb/embedded/tbtree"
)

type Snapshot struct {
	st   *ImmuStore
	snap *tbtree.Snapshot
}

type FilterFn func(valRef *ValueRef) bool

var (
	IgnoreDeleted FilterFn = func(valRef *ValueRef) bool {
		return valRef.kvmd == nil || !valRef.kvmd.deleted
	}
)

type KeyReader struct {
	store  *ImmuStore
	reader *tbtree.Reader
	filter FilterFn
	_tx    *Tx
}

type KeyReaderSpec struct {
	SeekKey       []byte
	EndKey        []byte
	Prefix        []byte
	InclusiveSeek bool
	InclusiveEnd  bool
	DescOrder     bool
	Filter        FilterFn
}

func (s *Snapshot) Get(key []byte, filters ...FilterFn) (valRef *ValueRef, err error) {
	indexedVal, tx, hc, err := s.snap.Get(key)
	if err != nil {
		return nil, err
	}

	valRef, err = s.st.valueRefFrom(tx, hc, indexedVal)
	if err != nil {
		return nil, err
	}

	for _, filter := range filters {
		if !filter(valRef) {
			return nil, ErrKeyNotFound
		}
	}

	return valRef, nil
}

func (s *Snapshot) History(key []byte, offset uint64, descOrder bool, limit int) (tss []uint64, err error) {
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

	return &KeyReader{
		store:  s.st,
		reader: r,
		filter: spec.Filter,
		_tx:    s.st.NewTx(),
	}, nil
}

type ValueRef struct {
	tx     uint64
	hc     uint64 // version
	hVal   [32]byte
	vOff   int64
	valLen uint32
	txmd   *TxMetadata
	kvmd   *KVMetadata
	st     *ImmuStore
}

func (st *ImmuStore) valueRefFrom(tx, hc uint64, indexedVal []byte) (*ValueRef, error) {
	// vLen + vOff + vHash
	const valrLen = lszSize + offsetSize + sha256.Size

	if len(indexedVal) < valrLen {
		return nil, ErrCorruptedIndex
	}

	i := 0

	valLen := binary.BigEndian.Uint32(indexedVal[i:])
	i += lszSize

	vOff := binary.BigEndian.Uint64(indexedVal[i:])
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
			kvmd = &KVMetadata{}

			err := kvmd.ReadFrom(indexedVal[i : i+kvmdLen])
			if err != nil {
				return nil, err
			}
			i += kvmdLen
		}
	}

	if len(indexedVal) > i {
		return nil, ErrCorruptedIndex
	}

	return &ValueRef{
		tx:     tx,
		hc:     hc,
		hVal:   hVal,
		vOff:   int64(vOff),
		valLen: valLen,
		txmd:   txmd,
		kvmd:   kvmd,
		st:     st,
	}, nil
}

// Resolve ...
func (v *ValueRef) Resolve() ([]byte, error) {
	refVal := make([]byte, v.valLen)
	_, err := v.st.ReadValueAt(refVal, v.vOff, v.hVal)
	return refVal, err
}

func (v *ValueRef) Tx() uint64 {
	return v.tx
}

func (v *ValueRef) HC() uint64 {
	return v.hc
}

func (v *ValueRef) TxMetadata() *TxMetadata {
	return v.txmd
}

func (v *ValueRef) KVMetadata() *KVMetadata {
	return v.kvmd
}

func (v *ValueRef) HVal() [sha256.Size]byte {
	return v.hVal
}

func (v *ValueRef) Len() uint32 {
	return v.valLen
}

func (r *KeyReader) ReadAsBefore(txID uint64) (key []byte, val *ValueRef, tx uint64, err error) {
	key, ktxID, hc, err := r.reader.ReadAsBefore(txID)
	if err != nil {
		return nil, nil, 0, err
	}

	err = r.store.ReadTx(ktxID, r._tx)
	if err != nil {
		return nil, nil, 0, err
	}

	for _, e := range r._tx.Entries() {
		if bytes.Equal(e.key(), key) {
			val = &ValueRef{
				tx:     r._tx.ID,
				hc:     hc,
				hVal:   e.hVal,
				vOff:   int64(e.vOff),
				valLen: uint32(e.vLen),
				txmd:   r._tx.Metadata,
				kvmd:   e.md,
				st:     r.store,
			}

			if r.filter != nil && !r.filter(val) {
				return nil, nil, 0, ErrKeyNotFound
			}

			return key, val, ktxID, nil
		}
	}

	return nil, nil, 0, ErrUnexpectedError
}

func (r *KeyReader) Read() (key []byte, val *ValueRef, err error) {
	for {
		key, indexedVal, tx, hc, err := r.reader.Read()
		if err != nil {
			return nil, nil, err
		}

		val, err = r.store.valueRefFrom(tx, hc, indexedVal)
		if err != nil {
			return nil, nil, err
		}

		if r.filter != nil && !r.filter(val) {
			continue
		}

		return key, val, nil
	}
}

func (r *KeyReader) Reset() error {
	return r.reader.Reset()
}

func (r *KeyReader) Close() error {
	return r.reader.Close()
}
