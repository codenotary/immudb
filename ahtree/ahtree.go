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
	"codenotary.io/immudb-v2/cache"
	"codenotary.io/immudb-v2/multierr"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrCorruptedCLog = errors.New("commit log is corrupted")
var ErrorCorruptedData = errors.New("data log is corrupted")
var ErrorCorruptedDigests = errors.New("hash log is corrupted")
var ErrAlreadyClosed = errors.New("already closed")
var ErrEmptyTree = errors.New("empty tree")
var ErrReadOnly = errors.New("cannot append when openned in read-only mode")
var ErrUnexistentData = errors.New("attempt to read unexistent data")

const NodePrefix = byte(1)

const Version = 1

const (
	MetaVersion = "VERSION"
)

const cLogEntrySize = offsetSize + szSize
const offsetSize = 8
const szSize = 4

//AHtree stands for Appendable Hash Tree
type AHtree struct {
	pLog appendable.Appendable
	dLog appendable.Appendable
	cLog appendable.Appendable

	pLogSize int64
	dLogSize int64
	cLogSize int64

	pCache *cache.LRUCache
	dCache *cache.LRUCache

	readOnly bool

	closed bool
	mutex  sync.Mutex

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

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.readOnly).
		WithSynced(opts.synced).
		WithFileSize(opts.fileSize).
		WithFileMode(opts.fileMode).
		WithMetadata(metadata.Bytes())

	appendableOpts.WithFileExt("dat")
	pLogPath := filepath.Join(path, "data")
	pLog, err := multiapp.Open(pLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.WithFileExt("sha")
	dLogPath := filepath.Join(path, "tree")
	dLog, err := multiapp.Open(dLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.WithFileExt("di")
	cLogPath := filepath.Join(path, "commit")
	cLog, err := multiapp.Open(cLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	return OpenWith(pLog, dLog, cLog, opts)
}

func OpenWith(pLog, dLog, cLog appendable.Appendable, opts *Options) (*AHtree, error) {
	if !validOptions(opts) || pLog == nil || dLog == nil || cLog == nil {
		return nil, ErrIllegalArguments
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

	pCache, err := cache.NewLRUCache(opts.dataCacheSlots)
	if err != nil {
		return nil, err
	}

	dCache, err := cache.NewLRUCache(opts.digestsCacheSlots)
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
		pCache:   pCache,
		dCache:   dCache,
		readOnly: opts.readOnly,
	}

	if cLogSize > 0 {
		var b [cLogEntrySize]byte
		_, err := cLog.ReadAt(b[:], cLogSize-cLogEntrySize)
		if err != nil {
			return nil, err
		}

		pOff := binary.BigEndian.Uint64(b[:])
		pSize := binary.BigEndian.Uint32(b[offsetSize:])

		t.pLogSize = int64(pOff) + int64(pSize)
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
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		err = ErrAlreadyClosed
		return
	}

	if t.readOnly {
		err = ErrReadOnly
		return
	}

	// will overrite partially written and uncommitted data
	t.pLog.SetOffset(t.pLogSize)

	var dLenBs [szSize]byte
	binary.BigEndian.PutUint32(dLenBs[:], uint32(len(d)))
	poff, _, perr := t.pLog.Append(dLenBs[:])
	if perr != nil {
		err = perr
		return
	}

	if len(d) > 0 {
		_, _, err = t.pLog.Append(d)
		if err != nil {
			return
		}
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

			hkl, nErr := t.node(k, l)
			if nErr != nil {
				err = nErr
				return
			}

			copy(b[1:], hkl[:])
			copy(b[1+sha256.Size:], h[:])

			h = sha256.Sum256(b[:])

			copy(t._digests[dCount*sha256.Size:], h[:])
			dCount++
		}

		k = k &^ uint64(1<<l)
		w >>= 1
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
	binary.BigEndian.PutUint32(cLogEntry[offsetSize:], uint32(len(d)))

	_, _, err = t.cLog.Append(cLogEntry[:])
	if err != nil {
		return
	}

	err = t.cLog.Flush()
	if err != nil {
		return
	}

	t.pLogSize += int64(szSize + len(d))
	t.dLogSize += int64(dCount * sha256.Size)
	t.cLogSize += cLogEntrySize

	return
}

func (t *AHtree) node(n uint64, l int) (h [sha256.Size]byte, err error) {
	return t.nodeAt(nodesUntil(n) + uint64(l))
}

func (t *AHtree) nodeAt(i uint64) (h [sha256.Size]byte, err error) {
	v, cErr := t.dCache.Get(i)
	if cErr == cache.ErrKeyNotFound {
		_, err = t.dLog.ReadAt(h[:], int64(i*sha256.Size))
		if err != nil {
			return
		}
		t.dCache.Put(i, h)
		return
	}

	return v.([sha256.Size]byte), nil
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
		w >>= 1
	}
	return l
}

func (t *AHtree) InclusionProof(i, j uint64) (p [][sha256.Size]byte, err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		err = ErrAlreadyClosed
		return
	}

	if i > j {
		return nil, ErrIllegalArguments
	}

	if j > uint64(t.cLogSize/cLogEntrySize) {
		return nil, ErrUnexistentData
	}

	return t.inclusionProof(i, j, bits.Len64(j-1))
}

func (t *AHtree) inclusionProof(i, j uint64, height int) ([][sha256.Size]byte, error) {
	var proof [][sha256.Size]byte

	for h := height - 1; h >= 0; h-- {
		if (j-1)&(1<<h) > 0 {
			k := (j - 1) >> h << h

			if i <= k {
				hNode, err := t.highestNode(j, h)
				if err != nil {
					return nil, err
				}
				proof = append([][sha256.Size]byte{hNode}, proof...)

				p, err := t.inclusionProof(i, k, h)
				if err != nil {
					return nil, err
				}

				proof = append(p, proof...)
				return proof, nil
			}

			n, err := t.node(k, h)
			if err != nil {
				return nil, err
			}
			proof = append([][sha256.Size]byte{n}, proof...)
		}
	}

	return proof, nil
}

func (t *AHtree) ConsistencyProof(i, j uint64) (p [][sha256.Size]byte, err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		err = ErrAlreadyClosed
		return
	}

	if i > j {
		return nil, ErrIllegalArguments
	}

	if j > uint64(t.cLogSize/cLogEntrySize) {
		return nil, ErrUnexistentData
	}

	return t.consistencyProof(i, j, bits.Len64(j-1))
}

func (t *AHtree) consistencyProof(i, j uint64, height int) ([][sha256.Size]byte, error) {
	var proof [][sha256.Size]byte

	for h := height - 1; h >= 0; h-- {
		if (j-1)&(1<<h) > 0 {
			k := (j - 1) >> h << h

			if i <= k {
				hNode, err := t.highestNode(j, h)
				if err != nil {
					return nil, err
				}
				proof = append([][sha256.Size]byte{hNode}, proof...)

				if i < k {
					p, err := t.consistencyProof(i, k, h)
					if err != nil {
						return nil, err
					}

					proof = append(p, proof...)
				}

				if i == k {
					hNode, err := t.highestNode(i, h)
					if err != nil {
						return nil, err
					}
					proof = append([][sha256.Size]byte{hNode}, proof...)
				}

				return proof, nil
			}

			n, err := t.node(k, h)
			if err != nil {
				return nil, err
			}
			proof = append([][sha256.Size]byte{n}, proof...)

			if i == j {
				hNode, err := t.highestNode(i, h)
				if err != nil {
					return nil, err
				}
				proof = append([][sha256.Size]byte{hNode}, proof...)
				return proof, nil
			}
		}
	}

	return proof, nil
}

func (t *AHtree) highestNode(i uint64, d int) ([sha256.Size]byte, error) {
	l := 0
	for r := d - 1; r >= 0; r-- {
		if (i-1)&(1<<r) > 0 {
			l++
		}
	}
	return t.node(i, l)
}

func (t *AHtree) Size() uint64 {
	return uint64(t.cLogSize / cLogEntrySize)
}

func (t *AHtree) DataAt(n uint64) ([]byte, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return nil, ErrAlreadyClosed
	}

	if n < 1 {
		return nil, ErrIllegalArguments
	}

	if n > uint64(t.cLogSize/cLogEntrySize) {
		return nil, ErrUnexistentData
	}

	v, err := t.pCache.Get(n)
	if err == cache.ErrKeyNotFound {
		var b [cLogEntrySize]byte
		_, err := t.cLog.ReadAt(b[:], int64((n-1)*cLogEntrySize))
		if err != nil {
			return nil, err
		}

		pOff := binary.BigEndian.Uint64(b[:])
		pSize := binary.BigEndian.Uint32(b[offsetSize:])

		p := make([]byte, pSize)
		_, err = t.pLog.ReadAt(p[:], int64(pOff+szSize))
		if err != nil {
			return nil, err
		}

		t.pCache.Put(n, p)
		return p, nil
	}
	return v.([]byte), nil
}

func (t *AHtree) Root() (n uint64, r [sha256.Size]byte, err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.cLogSize == 0 {
		err = ErrEmptyTree
		return
	}

	n = uint64(t.cLogSize / cLogEntrySize)
	r, err = t.rootAt(n)

	return
}

func (t *AHtree) RootAt(n uint64) (r [sha256.Size]byte, err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.rootAt(n)
}

func (t *AHtree) rootAt(n uint64) (r [sha256.Size]byte, err error) {
	if t.closed {
		err = ErrAlreadyClosed
		return
	}

	if n == 0 {
		err = ErrIllegalArguments
		return
	}

	if t.cLogSize == 0 {
		err = ErrEmptyTree
		return
	}

	if n > uint64(t.cLogSize/cLogEntrySize) {
		err = ErrUnexistentData
		return
	}

	return t.nodeAt(nodesUntil(n) + uint64(levelsAt(n)))
}

func (t *AHtree) Sync() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	if t.readOnly {
		return ErrReadOnly
	}

	if t.cLogSize == 0 {
		return nil
	}

	err := t.pLog.Sync()
	if err != nil {
		return err
	}

	err = t.dLog.Sync()
	if err != nil {
		return err
	}

	err = t.cLog.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (t *AHtree) Close() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	t.closed = true

	errors := make([]error, 0)

	pErr := t.pLog.Close()
	if pErr != nil {
		errors = append(errors, pErr)
	}

	dErr := t.dLog.Close()
	if dErr != nil {
		errors = append(errors, dErr)
	}

	cErr := t.cLog.Close()
	if cErr != nil {
		errors = append(errors, cErr)
	}

	if len(errors) > 0 {
		return &multierr.MultiErr{Errors: errors}
	}

	return nil
}
