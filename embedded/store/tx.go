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
	entries  []*Txe

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
	entries := make([]*Txe, nentries)
	for i := 0; i < nentries; i++ {
		entries[i] = &Txe{key: make([]byte, maxKeyLen)}
	}

	return NewTxWithEntries(entries)
}

func NewTxWithEntries(entries []*Txe) *Tx {
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

func (tx *Tx) Entries() []*Txe {
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
		if bytes.Equal(e.Key(), key) {
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
		klen, err := r.ReadUint32()
		if err != nil {
			return err
		}
		tx.entries[i].keyLen = int(klen)

		_, err = r.Read(tx.entries[i].key[:klen])
		if err != nil {
			return err
		}

		vlen, err := r.ReadUint32()
		if err != nil {
			return err
		}
		tx.entries[i].ValueLen = int(vlen)

		voff, err := r.ReadUint64()
		if err != nil {
			return err
		}
		tx.entries[i].VOff = int64(voff)

		_, err = r.Read(tx.entries[i].HValue[:])
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

type Txe struct {
	keyLen   int
	key      []byte
	ValueLen int
	HValue   [sha256.Size]byte
	VOff     int64
}

func (e *Txe) Key() []byte {
	return e.key[:e.keyLen]
}

func (e *Txe) SetKey(key []byte) {
	e.key = make([]byte, len(key))
	copy(e.key, key)
	e.keyLen = len(key)
}

func (e *Txe) Digest() [sha256.Size]byte {
	b := make([]byte, e.keyLen+sha256.Size)

	copy(b[:], e.key[:e.keyLen])
	copy(b[e.keyLen:], e.HValue[:])

	return sha256.Sum256(b)
}
