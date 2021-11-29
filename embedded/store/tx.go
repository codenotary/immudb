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
	header *TxHeader

	entries []*TxEntry

	htree *htree.HTree
}

type TxHeader struct {
	ID      uint64
	Ts      int64
	BlTxID  uint64
	BlRoot  [sha256.Size]byte
	PrevAlh [sha256.Size]byte

	Metadata *TxMetadata

	NEntries int
	Eh       [sha256.Size]byte
}

func newTx(nentries int, maxKeyLen int) *Tx {
	entries := make([]*TxEntry, nentries)
	for i := 0; i < nentries; i++ {
		entries[i] = &TxEntry{
			k: make([]byte, maxKeyLen),
		}
	}

	return NewTxWithEntries(entries)
}

func NewTxWithEntries(entries []*TxEntry) *Tx {
	htree, _ := htree.New(len(entries))

	return &Tx{
		header:  &TxHeader{NEntries: len(entries)},
		entries: entries,
		htree:   htree,
	}
}

func (tx *Tx) Header() *TxHeader {
	return tx.header
}

func (hdr *TxHeader) Bytes() []byte {
	// ID + PrevAlh + Ts + MDLen + MD + NEntries + Eh + BlTxID + BlRoot
	var b [txIDSize + sha256.Size + tsSize + sszSize + maxTxMetadataLen + sszSize + sha256.Size + txIDSize + sha256.Size]byte
	i := 0

	var mdbs []byte

	if hdr.Metadata != nil {
		mdbs = hdr.Metadata.Bytes()
	}

	binary.BigEndian.PutUint64(b[i:], hdr.ID)
	i += txIDSize

	copy(b[i:], hdr.PrevAlh[:])
	i += sha256.Size

	binary.BigEndian.PutUint64(b[i:], uint64(hdr.Ts))
	i += tsSize

	binary.BigEndian.PutUint16(b[i:], uint16(len(mdbs)))
	i += sszSize

	copy(b[i:], mdbs)
	i += len(mdbs)

	binary.BigEndian.PutUint16(b[i:], uint16(hdr.NEntries))
	i += sszSize

	copy(b[i:], hdr.Eh[:])
	i += sha256.Size

	binary.BigEndian.PutUint64(b[i:], hdr.BlTxID)
	i += txIDSize

	copy(b[i:], hdr.BlRoot[:])
	i += sha256.Size

	return b[:i]
}

func (hdr *TxHeader) ReadFrom(b []byte) error {
	// Minimum length with an empty metadata record
	if len(b) < txIDSize+sha256.Size+tsSize+2*sszSize+sha256.Size+txIDSize+sha256.Size {
		return ErrIllegalArguments
	}

	i := 0

	hdr.ID = binary.BigEndian.Uint64(b[i:])
	i += txIDSize

	copy(hdr.PrevAlh[:], b[i:])
	i += sha256.Size

	hdr.Ts = int64(binary.BigEndian.Uint64(b[i:]))
	i += tsSize

	mdLen := int(binary.BigEndian.Uint16(b[i:]))
	i += sszSize

	if mdLen > 0 {
		if len(b) < i+mdLen || mdLen > maxTxMetadataLen {
			return ErrCorruptedData
		}

		hdr.Metadata = &TxMetadata{}

		err := hdr.Metadata.ReadFrom(b[i : i+mdLen])
		if err != nil {
			return err
		}
		i += mdLen
	}

	hdr.NEntries = int(binary.BigEndian.Uint16(b[i:]))
	i += sszSize

	copy(hdr.Eh[:], b[i:])
	i += sha256.Size

	hdr.BlTxID = binary.BigEndian.Uint64(b[i:])
	i += txIDSize

	copy(hdr.BlRoot[:], b[i:])
	i += sha256.Size

	return nil
}

func (hdr *TxHeader) innerHash() [sha256.Size]byte {
	// ts + mdLen + md + nentries + eH + blTxID + blRoot
	var b [tsSize + sszSize + maxTxMetadataLen + sszSize + sha256.Size + txIDSize + sha256.Size]byte
	i := 0

	var mdbs []byte

	if hdr.Metadata != nil {
		mdbs = hdr.Metadata.Bytes()
	}

	binary.BigEndian.PutUint64(b[i:], uint64(hdr.Ts))
	i += tsSize

	binary.BigEndian.PutUint16(b[i:], uint16(len(mdbs)))
	i += sszSize

	copy(b[i:], mdbs)
	i += len(mdbs)

	binary.BigEndian.PutUint16(b[i:], uint16(hdr.NEntries))
	i += sszSize

	copy(b[i:], hdr.Eh[:])
	i += sha256.Size

	binary.BigEndian.PutUint64(b[i:], hdr.BlTxID)
	i += txIDSize

	copy(b[i:], hdr.BlRoot[:])
	i += sha256.Size

	// hash(ts + mdLen + md + nentries + eH + blTxID + blRoot)
	return sha256.Sum256(b[:i])
}

// Alh calculates the Accumulative Linear Hash up to this transaction
// Alh is calculated as hash(txID + prevAlh + hash(ts + nentries + eH + blTxID + blRoot))
// Inner hash is calculated so to reduce the length of linear proofs
func (hdr *TxHeader) Alh() [sha256.Size]byte {
	// txID + prevAlh + innerHash
	var bi [txIDSize + 2*sha256.Size]byte
	binary.BigEndian.PutUint64(bi[:], hdr.ID)
	copy(bi[txIDSize:], hdr.PrevAlh[:])

	// hash(ts + mdLen + md + nentries + eH + blTxID + blRoot)
	innerHash := hdr.innerHash()
	copy(bi[txIDSize+sha256.Size:], innerHash[:])

	// hash(txID + prevAlh + innerHash)
	return sha256.Sum256(bi[:])
}

func (tx *Tx) BuildHashTree() error {
	digests := make([][sha256.Size]byte, tx.header.NEntries)

	for i, e := range tx.Entries() {
		digests[i] = e.Digest()
	}

	err := tx.htree.BuildWith(digests)
	if err != nil {
		return err
	}

	root, err := tx.htree.Root()
	if err != nil {
		return err
	}

	tx.header.Eh = root

	return nil
}

func (tx *Tx) Entries() []*TxEntry {
	return tx.entries[:tx.header.NEntries]
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
	tx.header = &TxHeader{}

	id, err := r.ReadUint64()
	if err != nil {
		return err
	}
	tx.header.ID = id

	ts, err := r.ReadUint64()
	if err != nil {
		return err
	}
	tx.header.Ts = int64(ts)

	blTxID, err := r.ReadUint64()
	if err != nil {
		return err
	}
	tx.header.BlTxID = blTxID

	_, err = r.Read(tx.header.BlRoot[:])
	if err != nil {
		return err
	}

	_, err = r.Read(tx.header.PrevAlh[:])
	if err != nil {
		return err
	}

	mdLen, err := r.ReadUint16()
	if err != nil {
		return err
	}

	var txmd *TxMetadata

	if mdLen > 0 {
		if mdLen > maxTxMetadataLen {
			return ErrCorruptedData
		}

		var mdBs [maxTxMetadataLen]byte

		_, err = r.Read(mdBs[:mdLen])
		if err != nil {
			return err
		}

		txmd = &TxMetadata{}

		err = txmd.ReadFrom(mdBs[:mdLen])
		if err != nil {
			return err
		}
	}

	tx.header.Metadata = txmd

	nentries, err := r.ReadUint16()
	if err != nil {
		return err
	}
	tx.header.NEntries = int(nentries)

	for i := 0; i < int(nentries); i++ {
		// md is stored before key to ensure backward compatibility
		mdLen, err := r.ReadUint16()
		if err != nil {
			return err
		}

		var kvmd *KVMetadata

		if mdLen > 0 {
			var mdbs [maxKVMetadataLen]byte

			_, err = r.Read(mdbs[:mdLen])
			if err != nil {
				return err
			}

			kvmd = &KVMetadata{}

			err = kvmd.ReadFrom(mdbs[:mdLen])
			if err != nil {
				return err
			}
		}

		tx.entries[i].md = kvmd

		kLen, err := r.ReadUint16()
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

	err = tx.BuildHashTree()
	if err != nil {
		return err
	}

	if tx.header.Alh() != alh {
		return ErrorCorruptedTxData
	}

	return nil
}

type TxEntry struct {
	k    []byte
	kLen int
	md   *KVMetadata
	vLen int
	hVal [sha256.Size]byte
	vOff int64
}

func NewTxEntry(key []byte, md *KVMetadata, vLen int, hVal [sha256.Size]byte, vOff int64) *TxEntry {
	e := &TxEntry{
		k:    make([]byte, len(key)),
		kLen: len(key),
		md:   md,
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

func (e *TxEntry) Metadata() *KVMetadata {
	return e.md
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
	var mdbs []byte

	if e.md != nil {
		mdbs = e.md.Bytes()
	}

	mdLen := len(mdbs)

	b := make([]byte, sszSize+mdLen+e.kLen+sha256.Size)
	i := 0

	if mdLen > 0 {
		// md is only written if present for backward-compatibility
		binary.BigEndian.PutUint16(b[i:], uint16(mdLen))
		i += sszSize

		copy(b[i:], mdbs)
		i += mdLen
	}

	copy(b[i:], e.k[:e.kLen])
	i += e.kLen

	copy(b[i:], e.hVal[:])
	i += sha256.Size

	return sha256.Sum256(b[:i])
}
