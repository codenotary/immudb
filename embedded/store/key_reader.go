/*
Copyright 2019-2020 vChain, Inc.

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
	"encoding/binary"

	"github.com/codenotary/immudb/embedded/tbtree"
)

type KeyReader struct {
	store  *ImmuStore
	reader *tbtree.Reader
}

// NewReader ...
func (st *ImmuStore) NewKeyReader(snap *tbtree.Snapshot, spec *tbtree.ReaderSpec) (*KeyReader, error) {
	if snap == nil {
		return nil, ErrIllegalArguments
	}

	r, err := snap.NewReader(spec)
	if err != nil {
		return nil, err
	}

	return &KeyReader{
		store:  st,
		reader: r,
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

func (s *KeyReader) Read() (key []byte, val *ValueRef, tx uint64, hc uint64, err error) {
	key, vLogOffset, tx, hc, err := s.reader.Read()
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
		st:     s.store,
	}

	return key, val, tx, hc, nil
}

func (s *KeyReader) Close() error {
	return s.reader.Close()
}
