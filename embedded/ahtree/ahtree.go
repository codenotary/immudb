/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/multierr"
)

var ErrIllegalArguments = errors.New("ahtree: illegal arguments")
var ErrInvalidOptions = fmt.Errorf("%w: invalid options", ErrIllegalArguments)
var ErrorPathIsNotADirectory = errors.New("ahtree: path is not a directory")
var ErrorCorruptedData = errors.New("ahtree: data log is corrupted")
var ErrorCorruptedDigests = errors.New("ahtree: hash log is corrupted")
var ErrAlreadyClosed = errors.New("ahtree: already closed")
var ErrEmptyTree = errors.New("ahtree: empty tree")
var ErrReadOnly = errors.New("ahtree: read-only mode")
var ErrUnexistentData = errors.New("ahtree: attempt to read unexistent data")
var ErrCannotResetToLargerSize = errors.New("ahtree: can not reset the tree to a larger size")

const LeafPrefix = byte(0)
const NodePrefix = byte(1)

const Version = 1

const (
	MetaVersion = "VERSION"
)

const cLogEntrySize = offsetSize + szSize
const offsetSize = 8
const szSize = 4

// AHtree stands for Appendable Hash Tree
type AHtree struct {
	pLog appendable.Appendable
	dLog appendable.Appendable
	cLog appendable.Appendable

	pLogSize int64
	dLogSize int64
	cLogSize int64

	latestSyncedNode uint64

	cLogBuf      []byte
	cLogBufCount int

	pCache *cache.LRUCache
	dCache *cache.LRUCache

	syncThld int
	readOnly bool

	closed bool
	mutex  sync.Mutex

	_digests [256 * sha256.Size]byte // pre-allocated array for writing digests
}

func Open(path string, opts *Options) (*AHtree, error) {
	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		err := os.Mkdir(path, opts.fileMode)
		if err != nil {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, fmt.Errorf("%w: '%s'", ErrorPathIsNotADirectory, path)
	}

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetaVersion, Version)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.readOnly).
		WithReadBufferSize(opts.readBufferSize).
		WithWriteBufferSize(opts.writeBufferSize).
		WithRetryableSync(opts.retryableSync).
		WithAutoSync(opts.autoSync).
		WithFileSize(opts.fileSize).
		WithFileMode(opts.fileMode).
		WithMetadata(metadata.Bytes())

	appFactory := opts.appFactory
	if appFactory == nil {
		appFactory = func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			path := filepath.Join(rootPath, subPath)
			return multiapp.Open(path, opts)
		}
	}

	appendableOpts.WithFileExt("dat")
	pLog, err := appFactory(path, "data", appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.WithFileExt("sha")
	dLog, err := appFactory(path, "tree", appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.WithFileExt("di")
	cLog, err := appFactory(path, "commit", appendableOpts)
	if err != nil {
		return nil, err
	}

	return OpenWith(pLog, dLog, cLog, opts)
}

func OpenWith(pLog, dLog, cLog appendable.Appendable, opts *Options) (*AHtree, error) {
	if pLog == nil || dLog == nil || cLog == nil {
		return nil, fmt.Errorf("%w: nil appendable", ErrIllegalArguments)
	}

	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	cLogSize, err := cLog.Size()
	if err != nil {
		return nil, err
	}

	rem := cLogSize % cLogEntrySize
	if rem > 0 {
		cLogSize -= rem
		err = cLog.SetOffset(cLogSize)
		if err != nil {
			return nil, err
		}
	}

	latestSyncedNode := uint64(cLogSize / cLogEntrySize)

	pCache, err := cache.NewLRUCache(opts.dataCacheSlots)
	if err != nil {
		return nil, err
	}

	dCache, err := cache.NewLRUCache(opts.digestsCacheSlots)
	if err != nil {
		return nil, err
	}

	var cLogBuf []byte
	if !opts.readOnly {
		cLogBuf = make([]byte, opts.syncThld*cLogEntrySize)
	}

	t := &AHtree{
		pLog:             pLog,
		dLog:             dLog,
		cLog:             cLog,
		pLogSize:         0,
		dLogSize:         0,
		cLogSize:         cLogSize,
		latestSyncedNode: latestSyncedNode,
		pCache:           pCache,
		dCache:           dCache,
		syncThld:         opts.syncThld,
		readOnly:         opts.readOnly,
		cLogBuf:          cLogBuf,
	}

	if cLogSize == 0 {
		return t, nil
	}

	var b [cLogEntrySize]byte
	_, err = cLog.ReadAt(b[:], cLogSize-cLogEntrySize)
	if err != nil {
		return nil, err
	}

	pOff := binary.BigEndian.Uint64(b[:])
	pSize := binary.BigEndian.Uint32(b[offsetSize:])

	// pOff denotes the latest payload
	// pSize denotes the size of the latest payload
	// as payloads are prefixed with the size when written into pLog
	// pLogSize is calculated with the offset, the size description of the payload and the payload itself
	t.pLogSize = int64(pOff) + int64(szSize+pSize)

	pLogFileSize, err := pLog.Size()
	if err != nil {
		return nil, err
	}

	if pLogFileSize < t.pLogSize {
		return nil, ErrorCorruptedData
	}

	t.dLogSize = int64(nodesUpto(t.latestSyncedNode) * sha256.Size)

	dLogSize, err := dLog.Size()
	if err != nil {
		return nil, err
	}

	if dLogSize < t.dLogSize {
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

	if d == nil {
		err = ErrIllegalArguments
		return
	}

	// will overwrite partially written and uncommitted data
	err = t.pLog.SetOffset(t.pLogSize)
	if err != nil {
		return
	}

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

	n = t.size() + 1

	b := make([]byte, 1+len(d))
	b[0] = LeafPrefix
	copy(b[1:], d) // payload

	h = sha256.Sum256(b)
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

	// will overwrite partially written and uncommitted data
	err = t.dLog.SetOffset(t.dLogSize)
	if err != nil {
		return
	}

	_, _, err = t.dLog.Append(t._digests[:dCount*sha256.Size])
	if err != nil {
		return
	}

	_, _, err = t.pCache.Put(n, b[1:])
	if err != nil {
		return
	}

	for i := 0; i < dCount; i++ {
		var hb [sha256.Size]byte

		hbase := i * sha256.Size
		copy(hb[:], t._digests[hbase:hbase+sha256.Size])

		np := uint64(t.dLogSize/sha256.Size) + uint64(i)
		_, _, err = t.dCache.Put(np, hb)
		if err != nil {
			return
		}
	}

	var cLogEntry [cLogEntrySize]byte
	binary.BigEndian.PutUint64(cLogEntry[:], uint64(poff))
	binary.BigEndian.PutUint32(cLogEntry[offsetSize:], uint32(len(d)))

	copy(t.cLogBuf[t.cLogBufCount*cLogEntrySize:], cLogEntry[:])
	t.cLogBufCount++

	if t.cLogBufCount == t.syncThld {
		err = t.sync()
		if err != nil {
			t.cLogBufCount--
			return
		}
	}

	t.pLogSize += int64(szSize + len(d))
	t.dLogSize += int64(dCount * sha256.Size)
	t.cLogSize += cLogEntrySize

	return
}

func (t *AHtree) ResetSize(newSize uint64) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	if t.readOnly {
		return ErrReadOnly
	}

	currentSize := t.size()

	if currentSize < newSize {
		return ErrCannotResetToLargerSize
	}

	if currentSize == newSize {
		return nil
	}

	err := t.sync()
	if err != nil {
		return err
	}

	cLogSize := int64(newSize * cLogEntrySize)
	pLogSize := int64(0)
	dLogSize := int64(0)

	if newSize > 0 {
		var b [cLogEntrySize]byte
		_, err := t.cLog.ReadAt(b[:], cLogSize-cLogEntrySize)
		if err != nil {
			return err
		}

		pOff := binary.BigEndian.Uint64(b[:])
		pSize := binary.BigEndian.Uint32(b[offsetSize:])

		// pOff denotes the latest payload
		// pSize denotes the size of the latest payload
		// as payloads are prefixed with the size when written into pLog
		// pLogSize is calculated with the offset, the size description of the payload and the payload itself
		pLogSize = int64(pOff) + int64(szSize+pSize)

		pLogFileSize, err := t.pLog.Size()
		if err != nil {
			return err
		}

		if pLogFileSize < pLogSize {
			return ErrorCorruptedData
		}

		dLogSize = int64(nodesUpto(uint64(cLogSize/cLogEntrySize)) * sha256.Size)

		dLogFileSize, err := t.dLog.Size()
		if err != nil {
			return err
		}

		if dLogFileSize < dLogSize {
			return ErrorCorruptedDigests
		}
	}

	// Invalidate caches
	for i := cLogSize; i < t.cLogSize; i += cLogEntrySize {
		t.pCache.Pop(uint64(i / cLogEntrySize))
	}
	for i := dLogSize; i < t.dLogSize; i += sha256.Size {
		t.dCache.Pop(uint64(i / sha256.Size))
	}

	t.cLogSize = cLogSize
	t.pLogSize = pLogSize
	t.dLogSize = dLogSize

	t.latestSyncedNode = newSize

	return nil
}

func (t *AHtree) node(n uint64, l int) (h [sha256.Size]byte, err error) {
	return t.nodeAt(nodesUntil(n) + uint64(l))
}

func (t *AHtree) nodeAt(i uint64) (h [sha256.Size]byte, err error) {
	v, err := t.dCache.Get(i)

	if err == nil {
		return v.([sha256.Size]byte), nil
	}

	if err != cache.ErrKeyNotFound {
		return
	}

	_, err = t.dLog.ReadAt(h[:], int64(i*sha256.Size))
	if err != nil {
		return
	}

	_, _, err = t.dCache.Put(i, h)

	return h, err
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

	if j > t.size() {
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

	if j > t.size() {
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
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.size()
}

func (t *AHtree) size() uint64 {
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

	if n > t.size() {
		return nil, ErrUnexistentData
	}

	if n > t.latestSyncedNode {
		err := t.sync()
		if err != nil {
			return nil, err
		}
	}

	v, err := t.pCache.Get(n)

	if err == nil {
		return v.([]byte), nil
	}

	if err != cache.ErrKeyNotFound {
		return nil, err
	}

	var b [cLogEntrySize]byte
	_, err = t.cLog.ReadAt(b[:], int64((n-1)*cLogEntrySize))
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

	_, _, err = t.pCache.Put(n, p)

	return p, err
}

func (t *AHtree) Root() (n uint64, r [sha256.Size]byte, err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.cLogSize == 0 {
		err = ErrEmptyTree
		return
	}

	n = t.size()
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

	if n > t.size() {
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

	return t.sync()
}

func (t *AHtree) sync() error {
	if t.cLogBufCount == 0 {
		return nil
	}

	err := t.pLog.Flush()
	if err != nil {
		return err
	}

	err = t.pLog.Sync()
	if err != nil {
		return err
	}

	err = t.dLog.Flush()
	if err != nil {
		return err
	}

	err = t.dLog.Sync()
	if err != nil {
		return err
	}

	// will overwrite partially written and uncommitted data
	err = t.cLog.SetOffset(int64(t.latestSyncedNode) * cLogEntrySize)
	if err != nil {
		return err
	}

	_, _, err = t.cLog.Append(t.cLogBuf[:t.cLogBufCount*cLogEntrySize])
	if err != nil {
		return err
	}

	err = t.cLog.Flush()
	if err != nil {
		return err
	}

	err = t.cLog.Sync()
	if err != nil {
		return err
	}

	t.latestSyncedNode += uint64(t.cLogBufCount)
	t.cLogBufCount = 0

	return nil
}

func (t *AHtree) Close() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	t.closed = true

	merrors := multierr.NewMultiErr()

	err := t.sync()
	merrors.Append(err)

	err = t.pLog.Close()
	merrors.Append(err)

	err = t.dLog.Close()
	merrors.Append(err)

	err = t.cLog.Close()
	merrors.Append(err)

	return merrors.Reduce()
}
