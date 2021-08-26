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

type KeyReader struct {
	store  *ImmuStore
	reader *tbtree.Reader
	_tx    *Tx
}

type KeyReaderSpec struct {
	SeekKey       []byte
	EndKey        []byte
	Prefix        []byte
	InclusiveSeek bool
	InclusiveEnd  bool
	DescOrder     bool
}

func (s *Snapshot) Get(key []byte) (val []byte, tx uint64, hc uint64, err error) {
	indexedVal, tx, hc, err := s.snap.Get(key)
	if err != nil {
		return nil, 0, 0, err
	}

	valRef, err := s.st.valueRefFrom(indexedVal)
	if err != nil {
		return nil, 0, 0, err
	}

	val, err = valRef.Resolve()
	if err != nil {
		return nil, 0, 0, err
	}

	return val, tx, hc, nil
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
		_tx:    s.st.NewTx(),
	}, nil
}

type ValueRef struct {
	hVal   [32]byte
	vOff   int64
	valLen uint32
	st     *ImmuStore
}

func (st *ImmuStore) valueRefFrom(indexedVal []byte) (*ValueRef, error) {
	if len(indexedVal) != 4+8+32 {
		return nil, ErrCorruptedData
	}

	valLen := binary.BigEndian.Uint32(indexedVal)
	vOff := binary.BigEndian.Uint64(indexedVal[4:])

	var hVal [sha256.Size]byte
	copy(hVal[:], indexedVal[4+8:])

	return &ValueRef{
		hVal:   hVal,
		vOff:   int64(vOff),
		valLen: valLen,
		st:     st,
	}, nil
}

// Resolve ...
func (v *ValueRef) Resolve() ([]byte, error) {
	refVal := make([]byte, v.valLen)
	_, err := v.st.ReadValueAt(refVal, v.vOff, v.hVal)
	return refVal, err
}

func (r *KeyReader) ReadAsBefore(txID uint64) (key []byte, val *ValueRef, tx uint64, err error) {
	key, ktxID, err := r.reader.ReadAsBefore(txID)
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
				hVal:   e.hVal,
				vOff:   int64(e.vOff),
				valLen: uint32(e.vLen),
				st:     r.store,
			}

			return key, val, ktxID, nil
		}
	}

	return nil, nil, 0, ErrUnexpectedError
}

func (r *KeyReader) Read() (key []byte, val *ValueRef, tx uint64, hc uint64, err error) {
	key, indexedVal, tx, hc, err := r.reader.Read()
	if err != nil {
		return nil, nil, 0, 0, err
	}

	val, err = r.store.valueRefFrom(indexedVal)
	if err != nil {
		return nil, nil, 0, 0, err
	}

	return key, val, tx, hc, nil
}

func (r *KeyReader) Reset() error {
	return r.reader.Reset()
}

func (r *KeyReader) Close() error {
	return r.reader.Close()
}
