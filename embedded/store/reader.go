package store

import (
	"crypto/sha256"
	"encoding/binary"
	"github.com/codenotary/immudb/embedded/tbtree"
)

type Reader struct {
	initialKey []byte
	isPrefix   bool
	ascOrder   bool
	tbtReader  *tbtree.Reader
	snap       *tbtree.Snapshot
	st         *ImmuStore
}

type ReaderSpec struct {
	InitialKey []byte
	IsPrefix   bool
	AscOrder   bool
}

func NewReader(store *ImmuStore, spec ReaderSpec) (*Reader, error) {
	snapshot, err := store.Snapshot()
	if err != nil {
		return nil, err
	}
	tbtr, err := snapshot.Reader(&tbtree.ReaderSpec{
		IsPrefix:   spec.IsPrefix,
		InitialKey: spec.InitialKey,
		AscOrder:   spec.AscOrder})
	if err != nil {
		return nil, err
	}

	reader := &Reader{
		initialKey: spec.InitialKey,
		isPrefix:   spec.IsPrefix,
		ascOrder:   spec.AscOrder,
		st:         store,
		tbtReader:  tbtr,
		snap:       snapshot,
	}

	//defer snapshot.Close()

	//defer reader.Close()

	return reader, nil
}

type Value struct {
	hVal   [32]byte
	vOff   int64
	valLen uint32
}

// Resolve ...
func (s *ImmuStore) Resolve(val *Value) ([]byte, error) {
	refVal := make([]byte, val.valLen)
	_, err := s.ReadValueAt(refVal, val.vOff, val.hVal)
	return refVal, err
}

func (s *Reader) Read() (key []byte, val *Value, tx uint64, err error) {
	key, vLogOffset, index, err := s.tbtReader.Read()
	if err != nil {
		return nil, nil, 0, err
	}

	valLen := binary.BigEndian.Uint32(vLogOffset)
	vOff := binary.BigEndian.Uint64(vLogOffset[4:])

	var hVal [sha256.Size]byte
	copy(hVal[:], vLogOffset[4+8:])

	val = &Value{
		hVal:   hVal,
		vOff:   int64(vOff),
		valLen: valLen,
	}

	return key, val, index, nil
}

func (s *Reader) Close() {
	s.tbtReader.Close()
	s.snap.Close()

}
