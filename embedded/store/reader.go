package store

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/codenotary/immudb/embedded/tbtree"
)

type Reader struct {
	store  *ImmuStore
	reader *tbtree.Reader
}

// NewReader ...
func (st *ImmuStore) NewReader(snap *tbtree.Snapshot, spec *tbtree.ReaderSpec) (*Reader, error) {
	if snap == nil || spec == nil {
		return nil, ErrIllegalArguments
	}

	r, err := snap.Reader(spec)
	if err != nil {
		return nil, err
	}

	return &Reader{
		store:  st,
		reader: r,
	}, nil
}

type IndexedValue struct {
	hVal   [32]byte
	vOff   int64
	valLen uint32
	st     *ImmuStore
}

// Resolve ...
func (v *IndexedValue) Resolve() ([]byte, error) {
	refVal := make([]byte, v.valLen)
	_, err := v.st.ReadValueAt(refVal, v.vOff, v.hVal)
	return refVal, err
}

func (s *Reader) Read() (key []byte, val *IndexedValue, tx uint64, err error) {
	key, vLogOffset, index, err := s.reader.Read()
	if err != nil {
		return nil, nil, 0, err
	}

	valLen := binary.BigEndian.Uint32(vLogOffset)
	vOff := binary.BigEndian.Uint64(vLogOffset[4:])

	var hVal [sha256.Size]byte
	copy(hVal[:], vLogOffset[4+8:])

	val = &IndexedValue{
		hVal:   hVal,
		vOff:   int64(vOff),
		valLen: valLen,
	}

	return key, val, index, nil
}

func (s *Reader) Close() error {
	return s.reader.Close()
}
