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

package ahtree

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/bits"
	"os"
	"path/filepath"
	"sync"

	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/multiapp"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrCorruptedCLog = errors.New("commit log is corrupted")
var ErrorCorruptedData = errors.New("data log is corrupted")
var ErrorCorruptedDigests = errors.New("hash log is corrupted")
var ErrAlreadyClosed = errors.New("already closed")
var ErrEmptyTree = errors.New("empty tree")

const NodePrefix = byte(1)

const Version = 1

const (
	MetaVersion  = "VERSION"
	MetaFileSize = "FILE_SIZE"
)

const cLogEntrySize = 12 // data offset and size

//AHtree stands for Appendable Hash Tree
type AHtree struct {
	pLog appendable.Appendable // len + payload
	dLog appendable.Appendable
	cLog appendable.Appendable

	pLogSize int64
	dLogSize int64
	cLogSize int64

	readOnly bool

	closed  bool
	rwMutex sync.RWMutex

	_digests [256 * sha256.Size]byte // pre-allocated array for writing digests
}

func Open(path string, opts *Options) (*AHtree, error) {
	if !validOptions(opts) {
		return nil, ErrIllegalArguments
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(path, opts.fileMode)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, ErrorPathIsNotADirectory
	}

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetaVersion, Version)
	metadata.PutInt(MetaFileSize, opts.fileSize)

	appendableOpts := multiapp.DefaultOptions().
		SetReadOnly(opts.readOnly).
		SetSynced(opts.synced).
		SetFileSize(opts.fileSize).
		SetFileMode(opts.fileMode).
		SetMetadata(metadata.Bytes())

	appendableOpts.SetFileExt("dat")
	pLogPath := filepath.Join(path, "data")
	pLog, err := multiapp.Open(pLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.SetFileExt("sha")
	dLogPath := filepath.Join(path, "tree")
	dLog, err := multiapp.Open(dLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.SetFileExt("di")
	cLogPath := filepath.Join(path, "commit")
	cLog, err := multiapp.Open(cLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	return OpenWith(pLog, dLog, cLog, opts)
}

func OpenWith(pLog, dLog, cLog appendable.Appendable, opts *Options) (*AHtree, error) {
	if !validOptions(opts) {
		return nil, ErrIllegalArguments
	}

	metadata := appendable.NewMetadata(cLog.Metadata())

	fileSize, ok := metadata.GetInt(MetaFileSize)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	mapp, ok := pLog.(*multiapp.MultiFileAppendable)
	if ok {
		mapp.SetFileSize(fileSize)
	}

	mapp, ok = dLog.(*multiapp.MultiFileAppendable)
	if ok {
		mapp.SetFileSize(fileSize)
	}

	mapp, ok = cLog.(*multiapp.MultiFileAppendable)
	if ok {
		mapp.SetFileSize(fileSize)
	}

	cLogSize, err := cLog.Size()
	if err != nil {
		return nil, err
	}

	if cLogSize%cLogEntrySize > 0 {
		return nil, ErrCorruptedCLog
	}

	dLogSize, err := dLog.Size()
	if err != nil {
		return nil, err
	}

	t := &AHtree{
		pLog:     pLog,
		dLog:     dLog,
		cLog:     cLog,
		pLogSize: 0,
		dLogSize: dLogSize,
		cLogSize: cLogSize,
		readOnly: opts.readOnly,
	}

	if cLogSize > 0 {
		b := make([]byte, cLogEntrySize)
		_, err := cLog.ReadAt(b, cLogSize-cLogEntrySize)
		if err != nil {
			return nil, err
		}

		payloadOff := int64(binary.BigEndian.Uint64(b))
		payloadSize := int64(binary.BigEndian.Uint64(b[8:]))

		t.pLogSize = payloadOff + payloadSize
	}

	pLogFileSize, err := pLog.Size()
	if err != nil {
		return nil, err
	}

	if pLogFileSize < t.pLogSize {
		return nil, ErrorCorruptedData
	}

	if dLogSize < int64(nodesUpto(uint64(cLogSize/cLogEntrySize))*sha256.Size) {
		return nil, ErrorCorruptedDigests
	}

	return t, nil
}

func (t *AHtree) Append(d []byte) (n uint64, h [sha256.Size]byte, err error) {
	t.rwMutex.Lock()
	defer t.rwMutex.Unlock()

	if t.closed {
		err = ErrAlreadyClosed
		return
	}

	// will overrite partially written and uncommitted data
	t.pLog.SetOffset(t.pLogSize)

	var dLenBs [4]byte
	binary.BigEndian.PutUint32(dLenBs[:], uint32(len(d)))
	poff, _, perr := t.pLog.Append(dLenBs[:])
	if perr != nil {
		err = perr
		return
	}

	_, _, err = t.pLog.Append(d)
	if err != nil {
		return
	}

	n = uint64(t.cLogSize/cLogEntrySize) + 1

	h = sha256.Sum256(d)
	copy(t._digests[:], h[:])
	dCount := 1

	w := n - 1
	l := 0
	k := n - 1

	for w > 0 {
		if w%2 == 1 {
			b := [1 + sha256.Size*2]byte{NodePrefix}

			hkl := t.node(k, l)

			copy(b[1:], hkl[:])
			copy(b[1+sha256.Size:], h[:])

			h = sha256.Sum256(b[:])

			copy(t._digests[dCount*sha256.Size:], h[:])
			dCount++
		}

		k = k &^ uint64(1<<l)
		w = w >> 1
		l++
	}

	err = t.pLog.Flush()
	if err != nil {
		return
	}

	// will overrite partially written and uncommitted data
	t.dLog.SetOffset(t.dLogSize)

	_, _, err = t.dLog.Append(t._digests[:dCount*sha256.Size])
	if err != nil {
		return
	}

	err = t.dLog.Flush()
	if err != nil {
		return
	}

	var cLogEntry [cLogEntrySize]byte
	binary.BigEndian.PutUint64(cLogEntry[:], uint64(poff))
	binary.BigEndian.PutUint32(cLogEntry[8:], uint32(len(d)))

	_, _, err = t.cLog.Append(cLogEntry[:])
	if err != nil {
		return
	}

	err = t.cLog.Flush()
	if err != nil {
		return
	}

	t.pLogSize += int64(4 + len(d))
	t.dLogSize += int64(dCount * sha256.Size)
	t.cLogSize += cLogEntrySize

	return
}

func (t *AHtree) node(n uint64, l int) [sha256.Size]byte {
	hCount := nodesUntil(n) + uint64(l)

	var h [sha256.Size]byte
	t.dLog.ReadAt(h[:], int64(hCount*sha256.Size))

	return h
}

func nodesUntil(n uint64) uint64 {
	if n == 1 {
		return 0
	}
	return nodesUpto(n - 1)
}

func nodesUpto(n uint64) uint64 {
	o := n
	l := 0

	for {
		if n < (1 << l) {
			break
		}

		o += n >> (l + 1) << l

		if (n/(1<<l))%2 == 1 {
			o += n % (1 << l)
		}

		l++
	}

	return o
}

func levelsAt(n uint64) int {
	w := n - 1
	l := 0
	for w > 0 {
		if w%2 == 1 {
			l++
		}
		w = w >> 1
	}
	return l
}

func (t *AHtree) InclusionProof(i, j uint64) ([][sha256.Size]byte, error) {
	if i > j {
		return nil, ErrIllegalArguments
	}
	return t.inclusionProof(i, j, bits.Len64(j-1))
}

func (t *AHtree) inclusionProof(i, j uint64, height int) ([][sha256.Size]byte, error) {
	var proof [][sha256.Size]byte

	for h := height - 1; h >= 0; h-- {
		if (j-1)&(1<<h) > 0 {
			k := (j - 1) >> h << h

			if i <= k {
				proof = append([][sha256.Size]byte{t.highestNode(j, h)}, proof...)

				p, err := t.inclusionProof(i, k, h)
				if err != nil {
					return nil, err
				}

				proof = append(p, proof...)
				return proof, nil
			}

			proof = append([][sha256.Size]byte{t.node(k, h)}, proof...)
		}
	}

	return proof, nil
}

func (t *AHtree) highestNode(i uint64, d int) [sha256.Size]byte {
	l := 0
	for r := d - 1; r >= 0; r-- {
		if (i-1)&(1<<r) > 0 {
			l++
		}
	}
	return t.node(i, l)
}

func (t *AHtree) Size() (uint64, error) {
	return uint64(t.cLogSize / cLogEntrySize), nil
}

func (t *AHtree) DataAt(n uint64) ([]byte, error) {
	// como las data pueden tener distintos len, tengo que usar el cLog como indice
	// cLog tienen tamaño fijo, de ahi obtengo el offset, pero para leer el dato,

	return nil, nil
}

func (t *AHtree) Root() (r [sha256.Size]byte, err error) {
	if t.cLogSize == 0 {
		return r, ErrEmptyTree
	}

	return t.RootAt(uint64(t.cLogSize / cLogEntrySize))
}

func (t *AHtree) RootAt(n uint64) (r [sha256.Size]byte, err error) {
	hCount := nodesUntil(n) + uint64(levelsAt(n))

	_, err = t.dLog.ReadAt(r[:], int64(hCount*sha256.Size))

	return
}

func (t *AHtree) Flush() error {
	return nil
}

func (t *AHtree) Sync() error {
	return nil
}

func (t *AHtree) Close() error {
	return nil
}
