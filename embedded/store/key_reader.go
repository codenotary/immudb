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

	r, err := snap.Reader(spec)
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
