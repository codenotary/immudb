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

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/htree"
)

type Tx struct {
	ID      uint64
	Ts      int64
	BlTxID  uint64
	BlRoot  [sha256.Size]byte
	PrevAlh [sha256.Size]byte

	nentries int
	entries  []*TxEntry

	htree *htree.HTree

	Alh       [sha256.Size]byte
	InnerHash [sha256.Size]byte
}

type TxMetadata struct {
	ID       uint64
	PrevAlh  [sha256.Size]byte
	Ts       int64
	NEntries int
	Eh       [sha256.Size]byte
	BlTxID   uint64
	BlRoot   [sha256.Size]byte
}

func NewTx(nentries int, maxKeyLen int) *Tx {
	entries := make([]*TxEntry, nentries)
	for i := 0; i < nentries; i++ {
		entries[i] = &TxEntry{k: make([]byte, maxKeyLen)}
	}

	return NewTxWithEntries(entries)
}

func NewTxWithEntries(entries []*TxEntry) *Tx {
	htree, _ := htree.New(len(entries))

	return &Tx{
		ID:       0,
		entries:  entries,
		nentries: len(entries),
		htree:    htree,
	}
}

func (tx *Tx) Metadata() *TxMetadata {
	var prevAlh, blRoot [sha256.Size]byte

	copy(prevAlh[:], tx.PrevAlh[:])
	copy(blRoot[:], tx.BlRoot[:])

	return &TxMetadata{
		ID:       tx.ID,
		PrevAlh:  prevAlh,
		Ts:       tx.Ts,
		NEntries: tx.nentries,
		Eh:       tx.Eh(),
		BlTxID:   tx.BlTxID,
		BlRoot:   blRoot,
	}
}

func (md *TxMetadata) serialize() []byte {
	var b [txIDSize + 3*sha256.Size + tsSize + 12]byte
	i := 0

	binary.BigEndian.PutUint64(b[i:], md.ID)
	i += txIDSize

	copy(b[i:], md.PrevAlh[:])
	i += sha256.Size

	binary.BigEndian.PutUint64(b[i:], uint64(md.Ts))
	i += tsSize

	binary.BigEndian.PutUint32(b[i:], uint32(md.NEntries))
	i += 4

	copy(b[i:], md.Eh[:])
	i += sha256.Size

	binary.BigEndian.PutUint64(b[i:], uint64(md.BlTxID))
	i += txIDSize

	copy(b[i:], md.BlRoot[:])
	i += sha256.Size

	return b[:]
}

func (md *TxMetadata) readFrom(b []byte) error {
	if len(b) != txIDSize+3*sha256.Size+tsSize+12 {
		return ErrIllegalArguments
	}

	i := 0

	md.ID = binary.BigEndian.Uint64(b[i:])
	i += txIDSize

	copy(md.PrevAlh[:], b[i:])
	i += sha256.Size

	md.Ts = int64(binary.BigEndian.Uint64(b[i:]))
	i += tsSize

	md.NEntries = int(binary.BigEndian.Uint32(b[i:]))
	i += 4

	copy(md.Eh[:], b[i:])
	i += sha256.Size

	md.BlTxID = binary.BigEndian.Uint64(b[i:])
	i += txIDSize

	copy(md.BlRoot[:], b[i:])
	i += sha256.Size

	return nil
}

func (txMetadata *TxMetadata) Alh() [sha256.Size]byte {
	var bi [txIDSize + 2*sha256.Size]byte

	binary.BigEndian.PutUint64(bi[:], txMetadata.ID)
	copy(bi[txIDSize:], txMetadata.PrevAlh[:])

	var bj [tsSize + 4 + sha256.Size + txIDSize + sha256.Size]byte
	binary.BigEndian.PutUint64(bj[:], uint64(txMetadata.Ts))
	binary.BigEndian.PutUint32(bj[tsSize:], uint32(txMetadata.NEntries))
	copy(bj[tsSize+4:], txMetadata.Eh[:])
	binary.BigEndian.PutUint64(bj[tsSize+4+sha256.Size:], txMetadata.BlTxID)
	copy(bj[tsSize+4+sha256.Size+txIDSize:], txMetadata.BlRoot[:])
	innerHash := sha256.Sum256(bj[:]) // hash(ts + nentries + eH + blTxID + blRoot)

	copy(bi[txIDSize+sha256.Size:], innerHash[:]) // hash(txID + prevAlh + innerHash)

	return sha256.Sum256(bi[:])
}

func (tx *Tx) BuildHashTree() error {
	digests := make([][sha256.Size]byte, tx.nentries)

	for i, e := range tx.Entries() {
		digests[i] = e.Digest()
	}

	return tx.htree.BuildWith(digests)
}

func (tx *Tx) Entries() []*TxEntry {
	return tx.entries[:tx.nentries]
}

// Alh calculates the Accumulative Linear Hash up to this transaction
// Alh is calculated as hash(txID + prevAlh + hash(ts + nentries + eH + blTxID + blRoot))
// Inner hash is calculated so to reduce the length of linear proofs
func (tx *Tx) CalcAlh() {
	tx.calcInnerHash()

	var bi [txIDSize + 2*sha256.Size]byte
	binary.BigEndian.PutUint64(bi[:], tx.ID)
	copy(bi[txIDSize:], tx.PrevAlh[:])
	copy(bi[txIDSize+sha256.Size:], tx.InnerHash[:]) // hash(ts + nentries + eH + blTxID + blRoot)

	tx.Alh = sha256.Sum256(bi[:]) // hash(txID + prevAlh + innerHash)
}

func (tx *Tx) calcInnerHash() {
	var bj [tsSize + 4 + sha256.Size + txIDSize + sha256.Size]byte
	binary.BigEndian.PutUint64(bj[:], uint64(tx.Ts))
	binary.BigEndian.PutUint32(bj[tsSize:], uint32(tx.nentries))
	eh := tx.Eh()
	copy(bj[tsSize+4:], eh[:])
	binary.BigEndian.PutUint64(bj[tsSize+4+sha256.Size:], tx.BlTxID)
	copy(bj[tsSize+4+sha256.Size+txIDSize:], tx.BlRoot[:])

	tx.InnerHash = sha256.Sum256(bj[:]) // hash(ts + nentries + eH + blTxID + blRoot)
}

func (tx *Tx) Eh() [sha256.Size]byte {
	root, _ := tx.htree.Root()
	return root
}

func (tx *Tx) IndexOf(key []byte) (int, error) {
	for i, e := range tx.Entries() {
		if bytes.Equal(e.key(), key) {
			return i, nil
		}
	}
	return 0, ErrKeyNotFound
}

func (tx *Tx) Proof(key []byte) (*htree.InclusionProof, error) {
	kindex, err := tx.IndexOf(key)
	if err != nil {
		return nil, err
	}

	return tx.htree.InclusionProof(kindex)
}

func (tx *Tx) readFrom(r *appendable.Reader) error {
	id, err := r.ReadUint64()
	if err != nil {
		return err
	}
	tx.ID = id

	ts, err := r.ReadUint64()
	if err != nil {
		return err
	}
	tx.Ts = int64(ts)

	blTxID, err := r.ReadUint64()
	if err != nil {
		return err
	}
	tx.BlTxID = blTxID

	_, err = r.Read(tx.BlRoot[:])
	if err != nil {
		return err
	}

	_, err = r.Read(tx.PrevAlh[:])
	if err != nil {
		return err
	}

	nentries, err := r.ReadUint32()
	if err != nil {
		return err
	}
	tx.nentries = int(nentries)

	for i := 0; i < int(nentries); i++ {
		kLen, err := r.ReadUint32()
		if err != nil {
			return err
		}
		tx.entries[i].kLen = int(kLen)

		_, err = r.Read(tx.entries[i].k[:kLen])
		if err != nil {
			return err
		}

		vLen, err := r.ReadUint32()
		if err != nil {
			return err
		}
		tx.entries[i].vLen = int(vLen)

		vOff, err := r.ReadUint64()
		if err != nil {
			return err
		}
		tx.entries[i].vOff = int64(vOff)

		_, err = r.Read(tx.entries[i].hVal[:])
		if err != nil {
			return err
		}
	}

	var alh [sha256.Size]byte
	_, err = r.Read(alh[:])
	if err != nil {
		return err
	}

	tx.BuildHashTree()

	tx.CalcAlh()

	if tx.Alh != alh {
		return ErrorCorruptedTxData
	}

	return nil
}

type TxEntry struct {
	k      []byte
	kLen   int
	vLen   int
	hVal   [sha256.Size]byte
	vOff   int64
	unique bool
}

func NewTxEntry(key []byte, vLen int, hVal [sha256.Size]byte, vOff int64) *TxEntry {
	e := &TxEntry{
		k:    make([]byte, len(key)),
		kLen: len(key),
		vLen: vLen,
		hVal: hVal,
		vOff: vOff,
	}

	copy(e.k, key)

	return e
}

func (e *TxEntry) setKey(key []byte) {
	e.kLen = len(key)
	copy(e.k, key)
}

func (e *TxEntry) key() []byte {
	return e.k[:e.kLen]
}

func (e *TxEntry) Key() []byte {
	k := make([]byte, e.kLen)
	copy(k, e.k[:e.kLen])
	return k
}

func (e *TxEntry) HVal() [sha256.Size]byte {
	return e.hVal
}

func (e *TxEntry) VOff() int64 {
	return e.vOff
}

func (e *TxEntry) VLen() int {
	return e.vLen
}

func (e *TxEntry) Digest() [sha256.Size]byte {
	b := make([]byte, e.kLen+sha256.Size)

	copy(b[:], e.k[:e.kLen])
	copy(b[e.kLen:], e.hVal[:])

	return sha256.Sum256(b)
}
