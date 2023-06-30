/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"fmt"
	"io"

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

	Version int

	Metadata *TxMetadata

	NEntries int
	Eh       [sha256.Size]byte
}

func NewTx(nentries int, maxKeyLen int) *Tx {
	entries := make([]*TxEntry, nentries)

	keyBuffer := make([]byte, maxKeyLen*nentries)
	entriesBuffer := make([]TxEntry, nentries)
	for i := 0; i < nentries; i++ {
		entries[i] = &entriesBuffer[i]
		entries[i].k = keyBuffer[:maxKeyLen]
		keyBuffer = keyBuffer[maxKeyLen:]
	}

	header := &TxHeader{NEntries: len(entries)}

	return NewTxWithEntries(header, entries)
}

func NewTxWithEntries(header *TxHeader, entries []*TxEntry) *Tx {
	htree, _ := htree.New(len(entries))

	return &Tx{
		header:  header,
		entries: entries,
		htree:   htree,
	}
}

func (tx *Tx) Header() *TxHeader {
	var txmd *TxMetadata

	if tx.header.Metadata == nil {
		txmd = NewTxMetadata()
	} else {
		txmd = tx.header.Metadata
	}
	return &TxHeader{
		ID:      tx.header.ID,
		Ts:      tx.header.Ts,
		BlTxID:  tx.header.BlTxID,
		BlRoot:  tx.header.BlRoot,
		PrevAlh: tx.header.PrevAlh,

		Version: tx.header.Version,

		Metadata: txmd,

		NEntries: tx.header.NEntries,
		Eh:       tx.header.Eh,
	}
}

func (hdr *TxHeader) Bytes() ([]byte, error) {
	// ID + PrevAlh + Ts + Version + MDLen + MD + NEntries + Eh + BlTxID + BlRoot
	var b [txIDSize + sha256.Size + tsSize + sszSize + (sszSize + maxTxMetadataLen) + lszSize + sha256.Size + txIDSize + sha256.Size]byte
	i := 0

	binary.BigEndian.PutUint64(b[i:], hdr.ID)
	i += txIDSize

	copy(b[i:], hdr.PrevAlh[:])
	i += sha256.Size

	binary.BigEndian.PutUint64(b[i:], uint64(hdr.Ts))
	i += tsSize

	binary.BigEndian.PutUint16(b[i:], uint16(hdr.Version))
	i += sszSize

	switch hdr.Version {
	case 0:
		{
			if hdr.Metadata != nil && len(hdr.Metadata.Bytes()) > 0 {
				return nil, ErrMetadataUnsupported
			}

			binary.BigEndian.PutUint16(b[i:], uint16(hdr.NEntries))
			i += sszSize
		}
	case 1:
		{
			var mdbs []byte

			if hdr.Metadata != nil {
				mdbs = hdr.Metadata.Bytes()
			}

			binary.BigEndian.PutUint16(b[i:], uint16(len(mdbs)))
			i += sszSize

			copy(b[i:], mdbs)
			i += len(mdbs)

			binary.BigEndian.PutUint32(b[i:], uint32(hdr.NEntries))
			i += lszSize
		}
	default:
		{
			return nil, fmt.Errorf("%w for version %d", ErrUnsupportedTxHeaderVersion, hdr.Version)
		}
	}

	// following records are currently common in versions 0 and 1
	copy(b[i:], hdr.Eh[:])
	i += sha256.Size

	binary.BigEndian.PutUint64(b[i:], hdr.BlTxID)
	i += txIDSize

	copy(b[i:], hdr.BlRoot[:])
	i += sha256.Size

	return b[:i], nil
}

func (hdr *TxHeader) ReadFrom(b []byte) error {
	// Minimum length with version record
	if len(b) < txIDSize+sha256.Size+tsSize+2*sszSize+sha256.Size+txIDSize+sha256.Size {
		return ErrIllegalArguments
	}

	i := 0

	hdr.ID = binary.BigEndian.Uint64(b[i:])
	i += txIDSize

	if hdr.ID < 1 {
		return fmt.Errorf("%w: invalid tx ID", ErrIllegalArguments)
	}

	copy(hdr.PrevAlh[:], b[i:])
	i += sha256.Size

	hdr.Ts = int64(binary.BigEndian.Uint64(b[i:]))
	i += tsSize

	hdr.Version = int(binary.BigEndian.Uint16(b[i:]))
	i += sszSize

	switch hdr.Version {
	case 0:
		{

			hdr.NEntries = int(binary.BigEndian.Uint16(b[i:]))
			i += sszSize
		}
	case 1:
		{
			// version includes metadata record and a greater max number of entries

			mdLen := int(binary.BigEndian.Uint16(b[i:]))
			i += sszSize

			// nentries follows metadata
			if len(b) < i+mdLen+lszSize || mdLen > maxTxMetadataLen {
				return ErrCorruptedData
			}

			if mdLen > 0 {
				hdr.Metadata = NewTxMetadata()

				err := hdr.Metadata.ReadFrom(b[i : i+mdLen])
				if err != nil {
					return err
				}
				i += mdLen
			}

			hdr.NEntries = int(binary.BigEndian.Uint32(b[i:]))
			i += lszSize
		}
	default:
		{
			return ErrNewerVersionOrCorruptedData
		}
	}

	if hdr.NEntries < 1 {
		return fmt.Errorf("%w: invalid number of entries", ErrIllegalArguments)
	}

	// following records are currently common in versions 0 and 1
	copy(hdr.Eh[:], b[i:])
	i += sha256.Size

	hdr.BlTxID = binary.BigEndian.Uint64(b[i:])
	i += txIDSize

	if hdr.BlTxID >= hdr.ID {
		return fmt.Errorf("%w: invalid BlTxID", ErrIllegalArguments)
	}

	copy(hdr.BlRoot[:], b[i:])
	i += sha256.Size

	return nil
}

func (hdr *TxHeader) innerHash() [sha256.Size]byte {
	// ts + version + (mdLen + md)? + nentries + eH + blTxID + blRoot
	var b [tsSize + sszSize + (sszSize + maxTxMetadataLen) + lszSize + sha256.Size + txIDSize + sha256.Size]byte
	i := 0

	binary.BigEndian.PutUint64(b[i:], uint64(hdr.Ts))
	i += tsSize

	binary.BigEndian.PutUint16(b[i:], uint16(hdr.Version))
	i += sszSize

	switch hdr.Version {
	case 0:
		{
			binary.BigEndian.PutUint16(b[i:], uint16(hdr.NEntries))
			i += sszSize
		}
	case 1:
		{
			var mdbs []byte

			if hdr.Metadata != nil {
				mdbs = hdr.Metadata.Bytes()
			}

			binary.BigEndian.PutUint16(b[i:], uint16(len(mdbs)))
			i += sszSize

			copy(b[i:], mdbs)
			i += len(mdbs)

			binary.BigEndian.PutUint32(b[i:], uint32(hdr.NEntries))
			i += lszSize
		}
	default:
		{
			panic(fmt.Errorf("missing tx hash calculation method for version %d", hdr.Version))
		}
	}

	// following records are currently common in versions 0 and 1

	copy(b[i:], hdr.Eh[:])
	i += sha256.Size

	binary.BigEndian.PutUint64(b[i:], hdr.BlTxID)
	i += txIDSize

	copy(b[i:], hdr.BlRoot[:])
	i += sha256.Size

	// hash(ts + version + (mdLen + md) + nentries + eH + blTxID + blRoot)
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

	// hash(ts + version + (mdLen + md)? + nentries + eH + blTxID + blRoot)
	innerHash := hdr.innerHash()
	copy(bi[txIDSize+sha256.Size:], innerHash[:])

	// hash(txID + prevAlh + innerHash)
	return sha256.Sum256(bi[:])
}

func (hdr *TxHeader) TxEntryDigest() (TxEntryDigest, error) {
	switch hdr.Version {
	case 0:
		return TxEntryDigest_v1_1, nil
	case 1:
		return TxEntryDigest_v1_2, nil
	}

	return nil, ErrCorruptedTxDataUnknownHeaderVersion
}

func (tx *Tx) BuildHashTree() error {
	digests := make([][sha256.Size]byte, tx.header.NEntries)

	txEntryDigest, err := tx.header.TxEntryDigest()
	if err != nil {
		return err
	}

	for i, e := range tx.Entries() {
		digests[i], err = txEntryDigest(e)
		if err != nil {
			return err
		}
	}

	err = tx.htree.BuildWith(digests)
	if err != nil {
		return err
	}

	tx.header.Eh = tx.htree.Root()

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

func (tx *Tx) EntryOf(key []byte) (*TxEntry, error) {
	for _, e := range tx.Entries() {
		if bytes.Equal(e.key(), key) {
			return e, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (tx *Tx) Proof(key []byte) (*htree.InclusionProof, error) {
	kindex, err := tx.IndexOf(key)
	if err != nil {
		return nil, err
	}

	return tx.htree.InclusionProof(kindex)
}

func (tx *Tx) readFrom(r *appendable.Reader, skipIntegrityCheck bool) error {
	tdr := &txDataReader{r: r, skipIntegrityCheck: skipIntegrityCheck}

	header, err := tdr.readHeader(len(tx.entries))
	if err != nil {
		return err
	}

	tx.header = header

	for i := 0; i < header.NEntries; i++ {
		err = tdr.readEntry(tx.entries[i])
		if err != nil {
			return err
		}
	}

	err = tdr.buildAndValidateHtree(tx.htree)
	if err != nil {
		return err
	}

	return nil
}

type txDataReader struct {
	r                  *appendable.Reader
	h                  *TxHeader
	digests            [][sha256.Size]byte
	digestFunc         TxEntryDigest
	skipIntegrityCheck bool
}

func (t *txDataReader) readHeader(maxEntries int) (*TxHeader, error) {
	header := &TxHeader{}

	id, err := t.r.ReadUint64()
	if err != nil {
		return nil, err
	}
	if id == 0 {
		// underlying file may be preallocated
		return nil, io.EOF
	}

	header.ID = id

	ts, err := t.r.ReadUint64()
	if err != nil {
		return nil, err
	}
	header.Ts = int64(ts)

	blTxID, err := t.r.ReadUint64()
	if err != nil {
		return nil, err
	}
	header.BlTxID = blTxID

	_, err = t.r.Read(header.BlRoot[:])
	if err != nil {
		return nil, err
	}

	_, err = t.r.Read(header.PrevAlh[:])
	if err != nil {
		return nil, err
	}

	version, err := t.r.ReadUint16()
	if err != nil {
		return nil, err
	}
	header.Version = int(version)

	switch header.Version {
	case 0:
		{
			nentries, err := t.r.ReadUint16()
			if err != nil {
				return nil, err
			}
			header.NEntries = int(nentries)
		}
	case 1:
		{
			mdLen, err := t.r.ReadUint16()
			if err != nil {
				return nil, err
			}

			if mdLen > maxTxMetadataLen {
				return nil, ErrCorruptedData
			}

			var txmd *TxMetadata

			if mdLen > 0 {
				var mdBs [maxTxMetadataLen]byte

				_, err = t.r.Read(mdBs[:mdLen])
				if err != nil {
					return nil, err
				}

				txmd = NewTxMetadata()

				err = txmd.ReadFrom(mdBs[:mdLen])
				if err != nil {
					return nil, err
				}
			}

			header.Metadata = txmd

			nentries, err := t.r.ReadUint32()
			if err != nil {
				return nil, err
			}
			header.NEntries = int(nentries)
		}
	default:
		{
			return nil, fmt.Errorf("%w %d", ErrCorruptedTxDataUnknownHeaderVersion, header.Version)
		}
	}

	if header.NEntries > maxEntries {
		return nil, ErrCorruptedTxDataMaxTxEntriesExceeded
	}

	t.h = header

	if !t.skipIntegrityCheck {
		t.digestFunc, err = header.TxEntryDigest()
		if err != nil {
			return nil, err
		}

		t.digests = make([][sha256.Size]byte, 0, header.NEntries)
	}

	return header, nil
}

func (t *txDataReader) readEntry(entry *TxEntry) error {
	// md is stored before key to ensure backward compatibility
	mdLen, err := t.r.ReadUint16()
	if err != nil {
		return err
	}

	var kvmd *KVMetadata

	if mdLen > 0 {
		mdbs := make([]byte, mdLen)

		_, err = t.r.Read(mdbs)
		if err != nil {
			return err
		}

		kvmd = newReadOnlyKVMetadata()

		err = kvmd.unsafeReadFrom(mdbs)
		if err != nil {
			return err
		}
	}

	entry.md = kvmd

	kLen, err := t.r.ReadUint16()
	if err != nil {
		return err
	}
	entry.kLen = int(kLen)

	if entry.kLen > len(entry.k) {
		return ErrCorruptedTxDataMaxKeyLenExceeded
	}

	_, err = t.r.Read(entry.k[:kLen])
	if err != nil {
		return err
	}

	vLen, err := t.r.ReadUint32()
	if err != nil {
		return err
	}
	entry.vLen = int(vLen)

	vOff, err := t.r.ReadUint64()
	if err != nil {
		return err
	}
	entry.vOff = int64(vOff)

	_, err = t.r.Read(entry.hVal[:])
	if err != nil {
		return err
	}

	entry.readonly = true

	if !t.skipIntegrityCheck {
		digest, err := t.digestFunc(entry)
		if err != nil {
			return err
		}
		t.digests = append(t.digests, digest)
	}

	return nil
}

func (t *txDataReader) buildAndValidateHtree(htree *htree.HTree) error {
	// it's better to consume alh from appendable even if it's not validated
	// as seuqential tx reading can be done
	var alh [sha256.Size]byte
	_, err := t.r.Read(alh[:])
	if err != nil {
		return err
	}

	if t.skipIntegrityCheck {
		return nil
	}

	err = htree.BuildWith(t.digests)
	if err != nil {
		return err
	}

	t.h.Eh = htree.Root()

	if t.h.Alh() != alh {
		return fmt.Errorf("%w: ALH mismatch at tx %d", ErrCorruptedTxData, t.h.ID)
	}

	return nil
}

type TxEntry struct {
	k        []byte
	kLen     int
	md       *KVMetadata
	vLen     int
	hVal     [sha256.Size]byte
	vOff     int64
	readonly bool
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

type TxEntryDigest func(e *TxEntry) ([sha256.Size]byte, error)

func TxEntryDigest_v1_1(e *TxEntry) ([sha256.Size]byte, error) {
	if e.md != nil && len(e.md.Bytes()) > 0 {
		return [sha256.Size]byte{}, ErrMetadataUnsupported
	}

	b := make([]byte, e.kLen+sha256.Size)

	copy(b[:], e.k[:e.kLen])
	copy(b[e.kLen:], e.hVal[:])

	return sha256.Sum256(b), nil
}

func TxEntryDigest_v1_2(e *TxEntry) ([sha256.Size]byte, error) {
	var mdbs []byte

	if e.md != nil {
		mdbs = e.md.Bytes()
	}

	mdLen := len(mdbs)

	b := make([]byte, sszSize+mdLen+sszSize+e.kLen+sha256.Size)
	i := 0

	binary.BigEndian.PutUint16(b[i:], uint16(mdLen))
	i += sszSize

	copy(b[i:], mdbs)
	i += mdLen

	binary.BigEndian.PutUint16(b[i:], uint16(e.kLen))
	i += sszSize

	copy(b[i:], e.k[:e.kLen])
	i += e.kLen

	copy(b[i:], e.hVal[:])
	i += sha256.Size

	return sha256.Sum256(b[:i]), nil
}
