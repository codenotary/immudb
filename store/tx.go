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
	"math/bits"

	"codenotary.io/immudb-v2/appendable"
	"github.com/codenotary/merkletree"
)

type Tx struct {
	ID       uint64
	Ts       int64
	PrevAlh  [sha256.Size]byte
	nentries int
	entries  []*Txe
	Txh      [sha256.Size]byte
	htree    [][][sha256.Size]byte
	Eh       [sha256.Size]byte
}

func newTx(nentries int, maxKeyLen int) *Tx {
	entries := make([]*Txe, nentries)
	for i := 0; i < nentries; i++ {
		entries[i] = &Txe{key: make([]byte, maxKeyLen)}
	}

	w := 1
	for w < nentries {
		w = w << 1
	}

	layers := bits.Len64(uint64(nentries-1)) + 1
	htree := make([][][sha256.Size]byte, layers)
	for l := 0; l < layers; l++ {
		htree[l] = make([][sha256.Size]byte, w>>l)
	}

	return &Tx{
		ID:      0,
		entries: entries,
		htree:   htree,
	}
}

const NodePrefix = byte(1)

func (tx *Tx) buildHashTree() {
	l := 0
	w := tx.nentries

	p := [sha256.Size*2 + 1]byte{NodePrefix}

	for w > 1 {
		wn := 0

		for i := 0; i+1 < w; i += 2 {
			copy(p[1:sha256.Size+1], tx.htree[l][i][:])
			copy(p[sha256.Size+1:], tx.htree[l][i+1][:])
			tx.htree[l+1][wn] = sha256.Sum256(p[:])
			wn++
		}

		if w%2 == 1 {
			tx.htree[l+1][wn] = tx.htree[l][w-1]
			wn++
		}

		l++
		w = wn
	}

	tx.Eh = tx.htree[l][0]
}

func (tx *Tx) Width() uint64 {
	return uint64(tx.nentries)
}

func (tx *Tx) Set(layer uint8, index uint64, value [sha256.Size]byte) {
	tx.htree[layer][index] = value
}

func (tx *Tx) Get(layer uint8, index uint64) *[sha256.Size]byte {
	return &tx.htree[layer][index]
}

func (tx *Tx) Entries() []*Txe {
	return tx.entries[:tx.nentries]
}

func (tx *Tx) Alh() [sha256.Size]byte {
	bs := make([]byte, 2*sha256.Size)
	copy(bs, tx.PrevAlh[:])
	copy(bs[sha256.Size:], tx.Txh[:])
	return sha256.Sum256(bs)
}

func (tx *Tx) Proof(kindex int) merkletree.Path {
	return merkletree.InclusionProof(tx, uint64(tx.nentries-1), uint64(kindex))
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

		_, err = r.Read(tx.entries[i].HValue[:])
		if err != nil {
			return err
		}

		voff, err := r.ReadUint64()
		if err != nil {
			return err
		}
		tx.entries[i].VOff = int64(voff)

		tx.htree[0][i] = tx.entries[i].digest()
	}

	_, err = r.Read(tx.Txh[:])

	tx.buildHashTree()

	var b [52]byte
	binary.BigEndian.PutUint64(b[:], tx.ID)
	binary.BigEndian.PutUint64(b[8:], uint64(tx.Ts))
	binary.BigEndian.PutUint32(b[16:], uint32(len(tx.entries)))
	copy(b[20:], tx.Eh[:])

	if tx.Txh != sha256.Sum256(b[:]) {
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

func (e *Txe) digest() [sha256.Size]byte {
	hash := sha256.New()

	hash.Write(e.Key())
	hash.Write(e.HValue[:])

	var eh [sha256.Size]byte
	copy(eh[:], hash.Sum(nil))
	return eh
}
