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

type KeyReader struct {
	store  *ImmuStore
	reader *tbtree.Reader
	_tx    *Tx
}

type KeyReaderSpec struct {
	SeekKey       []byte
	Prefix        []byte
	InclusiveSeek bool
	DescOrder     bool
}

// NewReader ...
func (st *ImmuStore) NewKeyReader(snap *tbtree.Snapshot, spec *KeyReaderSpec) (*KeyReader, error) {
	if snap == nil {
		return nil, ErrIllegalArguments
	}

	r, err := snap.NewReader(&tbtree.ReaderSpec{
		SeekKey:       spec.SeekKey,
		Prefix:        spec.Prefix,
		InclusiveSeek: spec.InclusiveSeek,
		DescOrder:     spec.DescOrder,
	})
	if err != nil {
		return nil, err
	}

	return &KeyReader{
		store:  st,
		reader: r,
		_tx:    st.NewTx(),
	}, nil
}

type ValueRef struct {
	hVal   [32]byte
	vOff   int64
	valLen uint32
	st     *ImmuStore
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
	key, vLogOffset, tx, hc, err := r.reader.Read()
	if err != nil {
		return nil, nil, 0, 0, err
	}

	valLen := binary.BigEndian.Uint32(vLogOffset)
	vOff := binary.BigEndian.Uint64(vLogOffset[4:])

	var hVal [sha256.Size]byte
	copy(hVal[:], vLogOffset[4+8:])

	val = &ValueRef{
		hVal:   hVal,
		vOff:   int64(vOff),
		valLen: valLen,
		st:     r.store,
	}

	return key, val, tx, hc, nil
}

func (r *KeyReader) Close() error {
	return r.reader.Close()
}
