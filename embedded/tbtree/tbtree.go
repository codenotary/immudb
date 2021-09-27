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
package tbtree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/pkg/logger"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrReadingFileContent = errors.New("error reading required file content")
var ErrKeyNotFound = errors.New("key not found")
var ErrorMaxKVLenExceeded = errors.New("max kv length exceeded")
var ErrOffsetOutOfRange = errors.New("offset out of range")
var ErrIllegalState = errors.New("illegal state")
var ErrAlreadyClosed = errors.New("index already closed")
var ErrSnapshotsNotClosed = errors.New("snapshots not closed")
var ErrorToManyActiveSnapshots = errors.New("max active snapshots limit reached")
var ErrCorruptedFile = errors.New("file is corrupted")
var ErrCorruptedCLog = errors.New("commit log is corrupted")
var ErrCompactAlreadyInProgress = errors.New("compact already in progress")
var ErrCompactionThresholdNotReached = errors.New("compaction threshold not yet reached")

const Version = 1

const cLogEntrySize = 8 // root node offset

const (
	MetaVersion     = "VERSION"
	MetaMaxNodeSize = "MAX_NODE_SIZE"
)

const (
	// actual nodes and commit folders will be suffixed by root logical timestamp except for initial trees so to be backward compatible
	nodesFolderPrefix  = "nodes"
	commitFolderPrefix = "commit"

	historyFolder = "history" // history data is snapshot-agnostic / compaction-agnostic i.e. history(t) = history(compact(t))
)

// TBTree implements a timed-btree
type TBtree struct {
	path string
	log  logger.Logger

	nLog   appendable.Appendable
	cache  *cache.LRUCache
	nmutex sync.Mutex // mutex for cache and file reading

	hLog appendable.Appendable

	cLog appendable.Appendable

	root node

	maxNodeSize           int
	keyHistorySpace       uint64
	insertionCount        int
	flushThld             int
	maxActiveSnapshots    int
	renewSnapRootAfter    time.Duration
	readOnly              bool
	synced                bool
	cacheSize             int
	fileSize              int
	fileMode              os.FileMode
	maxKeyLen             int
	compactionThld        int
	delayDuringCompaction time.Duration

	greatestKey []byte

	snapshots      map[uint64]*Snapshot
	maxSnapshotID  uint64
	lastSnapRoot   node
	lastSnapRootAt time.Time

	committedLogSize  int64
	committedNLogSize int64
	committedHLogSize int64

	compacting bool

	closed  bool
	rwmutex sync.RWMutex
}

type path []*innerNode

type node interface {
	insertAt(key []byte, value []byte, ts uint64) (node, node, error)
	get(key []byte) (value []byte, ts uint64, hc uint64, err error)
	history(key []byte, offset uint64, descOrder bool, limit int) ([]uint64, error)
	findLeafNode(keyPrefix []byte, path path, neqKey []byte, descOrder bool) (path, *leafNode, int, error)
	minKey() []byte
	maxKey() []byte
	ts() uint64
	size() int
	mutated() bool
	offset() int64
	writeTo(nw, hw io.Writer, writeOpts *WriteOpts) (nOff int64, wN, wH int64, err error)
}

type WriteOpts struct {
	OnlyMutated    bool
	BaseNLogOffset int64
	BaseHLogOffset int64
	commitLog      bool
}

type innerNode struct {
	t       *TBtree
	nodes   []node
	_minKey []byte
	_maxKey []byte
	_ts     uint64
	maxSize int
	off     int64
	mut     bool
}

type leafNode struct {
	t       *TBtree
	values  []*leafValue
	_minKey []byte
	_maxKey []byte
	_ts     uint64
	maxSize int
	off     int64
	mut     bool
}

type nodeRef struct {
	t       *TBtree
	_minKey []byte
	_maxKey []byte
	_ts     uint64
	_size   int
	off     int64
}

type leafValue struct {
	key    []byte
	value  []byte
	ts     uint64
	tss    []uint64
	hOff   int64
	hCount uint64
}

func Open(path string, opts *Options) (*TBtree, error) {
	if !validOptions(opts) {
		return nil, ErrIllegalArguments
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		err = os.Mkdir(path, opts.fileMode)
		if err != nil {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, ErrorPathIsNotADirectory
	}

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetaVersion, Version)
	metadata.PutInt(MetaMaxNodeSize, opts.maxNodeSize)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.readOnly).
		WithSynced(opts.synced).
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

	appendableOpts.WithFileExt("hx")
	hLog, err := appFactory(path, historyFolder, appendableOpts)
	if err != nil {
		return nil, err
	}

	// If compaction was not fully completed, a valid or partially written full snapshot may be there
	snapIDs, err := recoverFullSnapshots(path, commitFolderPrefix, opts.log)
	if err != nil {
		return nil, err
	}

	// Try snapshots from newest to older
	for i := len(snapIDs); i > 0; i-- {
		snapID := snapIDs[i-1]

		nFolder := snapFolder(nodesFolderPrefix, snapID)
		cFolder := snapFolder(commitFolderPrefix, snapID)

		snapPath := filepath.Join(path, cFolder)

		opts.log.Infof("Reading snapshot at '%s'...", snapPath)

		appendableOpts.WithFileExt("n")
		nLog, err := appFactory(path, nFolder, appendableOpts)
		if err != nil {
			return nil, err
		}

		appendableOpts.WithFileExt("ri")
		cLog, err := appFactory(path, cFolder, appendableOpts)
		if err != nil {
			return nil, err
		}

		// TODO: semantic validation and further ammendment procedures may be done instead of a full initialization
		t, err := OpenWith(path, nLog, hLog, cLog, opts)
		if err != nil {
			opts.log.Infof("Error reading snapshot at '%s'. %v", snapPath, err)

			err = nLog.Close()
			if err != nil {
				return nil, err
			}

			err = cLog.Close()
			if err != nil {
				return nil, err
			}

			err = discardSnapshots(path, snapIDs[i-1:i], opts.log)
			if err != nil {
				opts.log.Warningf("Error discarding snapshots at '%s'. %v", path, err)
			}

			continue
		}

		opts.log.Infof("Successfully read snapshot at '%s'", snapPath)

		// Discard older snapshots upon sucessful validation
		err = discardSnapshots(path, snapIDs[:i-1], opts.log)
		if err != nil {
			opts.log.Warningf("Error discarding snapshots at '%s'. %v", path, err)
		}

		return t, nil
	}

	// No snapshot present or none was valid, fresh initialization
	opts.log.Infof("Starting with an empty index...")

	err = hLog.SetOffset(0)
	if err != nil {
		return nil, err
	}

	appendableOpts.WithFileExt("n")
	nLog, err := appFactory(path, nodesFolderPrefix, appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.WithFileExt("ri")
	cLog, err := appFactory(path, commitFolderPrefix, appendableOpts)
	if err != nil {
		return nil, err
	}

	return OpenWith(path, nLog, hLog, cLog, opts)
}

func snapFolder(folder string, snapID uint64) string {
	if snapID == 0 {
		return folder
	}

	return fmt.Sprintf("%s%016d", folder, snapID)
}

func recoverFullSnapshots(path, prefix string, log logger.Logger) (snapIDs []uint64, err error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, f := range fis {
		if f.IsDir() && strings.HasPrefix(f.Name(), prefix) {
			if f.Name() == prefix {
				snapIDs = append(snapIDs, 0)
				continue
			}

			id, err := strconv.ParseInt(strings.TrimPrefix(f.Name(), prefix), 10, 64)
			if err != nil {
				log.Warningf("invalid folder found '%s'", f.Name())
				continue
			}

			snapIDs = append(snapIDs, uint64(id))
		}
	}

	return snapIDs, nil
}

func discardSnapshots(path string, snapIDs []uint64, log logger.Logger) error {
	for _, snapID := range snapIDs {
		nFolder := snapFolder(nodesFolderPrefix, snapID)
		cFolder := snapFolder(commitFolderPrefix, snapID)

		nPath := filepath.Join(path, nFolder)
		cPath := filepath.Join(path, cFolder)

		log.Infof("Discarding snapshot at '%s'...", cPath)

		err := os.RemoveAll(nPath) // TODO: nLog.Remove()
		if err != nil {
			return err
		}

		err = os.RemoveAll(cPath) // TODO: cLog.Remove()
		if err != nil {
			return err
		}

		log.Infof("Snapshot at '%s' has been discarded", cPath)
	}

	return nil
}

func OpenWith(path string, nLog, hLog, cLog appendable.Appendable, opts *Options) (*TBtree, error) {
	if nLog == nil || hLog == nil || cLog == nil || !validOptions(opts) {
		return nil, ErrIllegalArguments
	}

	metadata := appendable.NewMetadata(cLog.Metadata())

	maxNodeSize, ok := metadata.GetInt(MetaMaxNodeSize)
	if !ok {
		return nil, ErrCorruptedCLog
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

	cache, err := cache.NewLRUCache(opts.cacheSize)
	if err != nil {
		return nil, err
	}

	t := &TBtree{
		path:                  path,
		log:                   opts.log,
		nLog:                  nLog,
		hLog:                  hLog,
		cLog:                  cLog,
		committedLogSize:      cLogSize,
		committedNLogSize:     0, // If garbage is accepted then t.committedNLogSize should be set to its size during initialization
		committedHLogSize:     hLog.Offset(),
		cache:                 cache,
		maxNodeSize:           maxNodeSize,
		flushThld:             opts.flushThld,
		renewSnapRootAfter:    opts.renewSnapRootAfter,
		maxActiveSnapshots:    opts.maxActiveSnapshots,
		fileSize:              opts.fileSize,
		cacheSize:             opts.cacheSize,
		fileMode:              opts.fileMode,
		maxKeyLen:             opts.maxKeyLen,
		compactionThld:        opts.compactionThld,
		delayDuringCompaction: opts.delayDuringCompaction,
		greatestKey:           greatestKeyOfSize(opts.maxKeyLen),
		readOnly:              opts.readOnly,
		synced:                opts.synced,
		snapshots:             make(map[uint64]*Snapshot),
	}

	var root node

	if cLogSize == 0 {
		root = &leafNode{t: t, maxSize: maxNodeSize, mut: true, _minKey: t.greatestKey}
	} else {
		var b [cLogEntrySize]byte
		_, err := cLog.ReadAt(b[:], cLogSize-cLogEntrySize)
		if err != nil {
			return nil, err
		}

		committedRootOffset := int64(binary.BigEndian.Uint64(b[:]))

		root, err = t.readNodeAt(committedRootOffset)
		if err != nil {
			return nil, err
		}

		t.committedNLogSize = committedRootOffset + int64(root.size())
	}

	t.root = root

	return t, nil
}

func greatestKeyOfSize(size int) []byte {
	k := make([]byte, size)
	for i := 0; i < size; i++ {
		k[i] = 0xFF
	}
	return k
}

func (t *TBtree) GetOptions() *Options {
	return DefaultOptions().
		WithReadOnly(t.readOnly).
		WithFileMode(t.fileMode).
		WithFileSize(t.fileSize).
		WithMaxKeyLen(t.maxKeyLen).
		WithSynced(t.synced).
		WithLog(t.log).
		WithCacheSize(t.cacheSize).
		WithFlushThld(t.flushThld).
		WithMaxActiveSnapshots(t.maxActiveSnapshots).
		WithMaxNodeSize(t.maxNodeSize).
		WithRenewSnapRootAfter(t.renewSnapRootAfter).
		WithCompactionThld(t.compactionThld).
		WithDelayDuringCompaction(t.delayDuringCompaction)
}

func (t *TBtree) cachePut(n node) {
	t.nmutex.Lock()
	defer t.nmutex.Unlock()

	t.cache.Put(n.offset(), n)
}

func (t *TBtree) nodeAt(offset int64) (node, error) {
	t.nmutex.Lock()
	defer t.nmutex.Unlock()

	v, err := t.cache.Get(offset)
	if err == nil {
		return v.(node), nil
	}

	if err == cache.ErrKeyNotFound {
		n, err := t.readNodeAt(offset)

		if err != nil {
			return nil, err
		}

		t.cache.Put(n.offset(), n)

		return n, nil
	}

	return nil, err
}

func (t *TBtree) readNodeAt(off int64) (node, error) {
	r := appendable.NewReaderFrom(t.nLog, off, t.maxNodeSize)
	return t.readNodeFrom(r)
}

func (t *TBtree) readNodeFrom(r *appendable.Reader) (node, error) {
	off := r.Offset()

	nodeType, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	_, err = r.ReadUint32() // size is not currently used
	if err != nil {
		return nil, err
	}

	switch nodeType {
	case InnerNodeType:
		n, err := t.readInnerNodeFrom(r)
		if err != nil {
			return nil, err
		}
		n.off = off
		return n, nil
	case LeafNodeType:
		n, err := t.readLeafNodeFrom(r)
		if err != nil {
			return nil, err
		}
		n.off = off
		return n, nil
	}

	return nil, ErrReadingFileContent
}

func (t *TBtree) readInnerNodeFrom(r *appendable.Reader) (*innerNode, error) {
	childCount, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}

	n := &innerNode{
		t:       t,
		nodes:   make([]node, childCount),
		_minKey: t.greatestKey,
		_maxKey: nil,
		_ts:     0,
		maxSize: t.maxNodeSize,
	}

	for c := 0; c < int(childCount); c++ {
		nref, err := t.readNodeRefFrom(r)
		if err != nil {
			return nil, err
		}

		n.nodes[c] = nref

		if bytes.Compare(n._minKey, nref._minKey) > 0 {
			n._minKey = nref._minKey
		}

		if bytes.Compare(n._maxKey, nref._maxKey) < 0 {
			n._maxKey = nref._maxKey
		}

		if n._ts < nref._ts {
			n._ts = nref._ts
		}
	}

	return n, nil
}

func (t *TBtree) readNodeRefFrom(r *appendable.Reader) (*nodeRef, error) {
	minKeySize, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}

	minKey := make([]byte, minKeySize)
	_, err = r.Read(minKey)
	if err != nil {
		return nil, err
	}

	maxKeySize, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}

	maxKey := make([]byte, maxKeySize)
	_, err = r.Read(maxKey)
	if err != nil {
		return nil, err
	}

	ts, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	size, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}

	off, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	return &nodeRef{
		t:       t,
		_minKey: minKey,
		_maxKey: maxKey,
		_ts:     ts,
		_size:   int(size),
		off:     int64(off),
	}, nil
}

func (t *TBtree) readLeafNodeFrom(r *appendable.Reader) (*leafNode, error) {
	valueCount, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}

	l := &leafNode{
		t:       t,
		values:  make([]*leafValue, valueCount),
		_minKey: t.greatestKey,
		_maxKey: nil,
		_ts:     0,
		maxSize: t.maxNodeSize,
	}

	for c := 0; c < int(valueCount); c++ {
		ksize, err := r.ReadUint32()
		if err != nil {
			return nil, err
		}

		key := make([]byte, ksize)
		_, err = r.Read(key)
		if err != nil {
			return nil, err
		}

		vsize, err := r.ReadUint32()
		if err != nil {
			return nil, err
		}

		value := make([]byte, vsize)
		_, err = r.Read(value)
		if err != nil {
			return nil, err
		}

		ts, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}

		hOff, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}

		hCount, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}

		leafValue := &leafValue{
			key:    key,
			value:  value,
			ts:     ts,
			tss:    nil,
			hOff:   int64(hOff),
			hCount: hCount,
		}

		l.values[c] = leafValue

		if bytes.Compare(l._minKey, leafValue.key) > 0 {
			l._minKey = leafValue.key
		}

		if bytes.Compare(l._maxKey, leafValue.key) < 0 {
			l._maxKey = leafValue.key
		}

		if l._ts < leafValue.ts {
			l._ts = leafValue.ts
		}
	}

	return l, nil
}

func (t *TBtree) Get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	if t.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}

	if key == nil {
		return nil, 0, 0, ErrIllegalArguments
	}

	v, ts, hc, err := t.root.get(key)
	return cp(v), ts, hc, err
}

func (t *TBtree) History(key []byte, offset uint64, descOrder bool, limit int) (tss []uint64, err error) {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	if t.closed {
		return nil, ErrAlreadyClosed
	}

	if key == nil {
		return nil, ErrIllegalArguments
	}

	if limit < 1 {
		return nil, ErrIllegalArguments
	}

	return t.root.history(key, offset, descOrder, limit)
}

func (t *TBtree) ExistKeyWith(prefix []byte, neq []byte, smaller bool) (bool, error) {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	if t.closed {
		return false, ErrAlreadyClosed
	}

	_, leaf, off, err := t.root.findLeafNode(prefix, nil, neq, smaller)
	if err == ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	v := leaf.values[off]

	if len(prefix) > len(v.key) {
		return false, nil
	}

	return bytes.Equal(prefix, v.key[:len(prefix)]), nil
}

func (t *TBtree) Sync() error {
	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	return t.sync()
}

func (t *TBtree) sync() error {
	err := t.nLog.Sync()
	if err != nil {
		return err
	}

	err = t.hLog.Sync()
	if err != nil {
		return err
	}

	return t.cLog.Sync()
}

func (t *TBtree) Flush() (wN, wH int64, err error) {
	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return 0, 0, ErrAlreadyClosed
	}

	return t.flushTree()
}

type appendableWriter struct {
	appendable.Appendable
}

func (aw *appendableWriter) Write(b []byte) (int, error) {
	_, n, err := aw.Append(b)
	return n, err
}

func (t *TBtree) warn(formattedMessage string, err error) error {
	t.log.Warningf(formattedMessage, err)
	return err
}

func (t *TBtree) flushTree() (wN int64, wH int64, err error) {
	t.log.Infof("Flushing index '%s'...", t.path)

	if !t.root.mutated() {
		t.log.Infof("Flushing not needed at '%s'", t.path)
		return 0, 0, nil
	}

	snapshot := t.newSnapshot(0, t.root, false)

	// will overwrite partially written and uncommitted data
	// if garbage is accepted then t.committedNLogSize should be set to its size during initialization
	err = t.nLog.SetOffset(t.committedNLogSize)
	if err != nil {
		return 0, 0, err
	}

	wopts := &WriteOpts{
		OnlyMutated:    true,
		BaseNLogOffset: t.committedNLogSize,
		BaseHLogOffset: t.committedHLogSize,
		commitLog:      true,
	}

	_, wN, wH, err = snapshot.WriteTo(&appendableWriter{t.nLog}, &appendableWriter{t.hLog}, wopts)
	if err != nil {
		return 0, 0, t.warn("Flushing index '%s' returned: %v", err)
	}

	err = t.nLog.Flush()
	if err != nil {
		return 0, 0, t.warn("Flushing index '%s' returned: %v", err)
	}

	err = t.hLog.Flush()
	if err != nil {
		return 0, 0, t.warn("Flushing index '%s' returned: %v", err)
	}

	// will overwrite partially written and uncommitted data
	err = t.cLog.SetOffset(t.committedLogSize)
	if err != nil {
		return 0, 0, err
	}

	var cb [cLogEntrySize]byte
	binary.BigEndian.PutUint64(cb[:], uint64(t.root.offset()))
	_, _, err = t.cLog.Append(cb[:])
	if err != nil {
		return 0, 0, t.warn("Flushing index '%s' returned: %v", err)
	}

	err = t.cLog.Flush()
	if err != nil {
		return 0, 0, t.warn("Flushing index '%s' returned: %v", err)
	}

	t.insertionCount = 0
	t.committedLogSize += cLogEntrySize
	t.committedNLogSize += wN
	t.committedHLogSize += wH

	t.root = &nodeRef{
		t:       t,
		_minKey: t.root.minKey(),
		_maxKey: t.root.maxKey(),
		_ts:     t.root.ts(),
		_size:   t.root.size(),
		off:     t.root.offset(),
	}

	t.log.Infof("Flushing index '%s' successfully completed", t.path)

	return wN, wH, nil
}

func (t *TBtree) currentSnapshot() (*Snapshot, error) {
	_, _, err := t.flushTree()
	if err != nil {
		return nil, err
	}

	return t.newSnapshot(0, t.root, false), nil
}

// SnapshotCount returns the number of stored snapshots
// Note: snapshotCount(compact(t)) = 1
func (t *TBtree) SnapshotCount() (uint64, error) {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	if t.closed {
		return 0, ErrAlreadyClosed
	}

	return t.snapshotCount(), nil
}

func (t *TBtree) snapshotCount() uint64 {
	return uint64(t.committedLogSize / cLogEntrySize)
}

func (t *TBtree) Compact() (uint64, error) {
	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return 0, ErrAlreadyClosed
	}

	if t.compacting {
		return 0, ErrCompactAlreadyInProgress
	}

	if t.snapshotCount() < uint64(t.compactionThld) {
		return 0, ErrCompactionThresholdNotReached
	}

	snapshot, err := t.currentSnapshot()
	if err != nil {
		return 0, err
	}

	err = t.hLog.Sync()
	if err != nil {
		return 0, err
	}

	t.compacting = true

	t.rwmutex.Unlock()
	err = t.fullDump(snapshot)
	t.rwmutex.Lock()

	t.compacting = false

	if err != nil {
		return 0, err
	}

	return snapshot.Ts(), nil
}

func (t *TBtree) fullDump(snapshot *Snapshot) error {
	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetaVersion, Version)
	metadata.PutInt(MetaMaxNodeSize, t.maxNodeSize)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(false).
		WithSynced(false).
		WithFileSize(t.fileSize).
		WithFileMode(t.fileMode).
		WithMetadata(t.cLog.Metadata())

	appendableOpts.WithFileExt("n")
	nLogPath := filepath.Join(t.path, snapFolder(nodesFolderPrefix, snapshot.Ts()))
	nLog, err := multiapp.Open(nLogPath, appendableOpts)
	if err != nil {
		return err
	}
	defer func() {
		nLog.Close()
	}()

	appendableOpts.WithFileExt("ri")
	cLogPath := filepath.Join(t.path, snapFolder(commitFolderPrefix, snapshot.Ts()))
	cLog, err := multiapp.Open(cLogPath, appendableOpts)
	if err != nil {
		return err
	}
	defer func() {
		cLog.Close()
	}()

	return t.fullDumpTo(snapshot, nLog, cLog)
}

func (t *TBtree) fullDumpTo(snapshot *Snapshot, nLog, cLog appendable.Appendable) error {
	wopts := &WriteOpts{
		OnlyMutated:    false,
		BaseNLogOffset: 0,
		BaseHLogOffset: 0,
	}

	offset, _, _, err := snapshot.WriteTo(&appendableWriter{nLog}, nil, wopts)
	if err != nil {
		return err
	}

	var cb [cLogEntrySize]byte
	binary.BigEndian.PutUint64(cb[:], uint64(offset))
	_, _, err = cLog.Append(cb[:])
	if err != nil {
		return err
	}

	err = nLog.Flush()
	if err != nil {
		return err
	}

	err = nLog.Sync()
	if err != nil {
		return err
	}

	err = cLog.Flush()
	if err != nil {
		return err
	}

	err = cLog.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (t *TBtree) Close() error {
	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	if len(t.snapshots) > 0 {
		return ErrSnapshotsNotClosed
	}

	_, _, err := t.flushTree()
	if err != nil {
		return err
	}

	t.closed = true

	merrors := multierr.NewMultiErr()

	err = t.nLog.Close()
	merrors.Append(err)

	err = t.hLog.Close()
	merrors.Append(err)

	err = t.cLog.Close()
	merrors.Append(err)

	if merrors.HasErrors() {
		return merrors
	}

	return nil
}

type KV struct {
	K []byte
	V []byte
}

func (t *TBtree) Insert(key []byte, value []byte) error {
	return t.BulkInsert([]*KV{{K: key, V: value}})
}

func (t *TBtree) BulkInsert(kvs []*KV) error {
	t.rwmutex.Lock()

	defer func() {
		slowDown := false

		if t.compacting && t.delayDuringCompaction > 0 {
			slowDown = true
		}

		t.rwmutex.Unlock()

		if slowDown {
			time.Sleep(t.delayDuringCompaction)
		}
	}()

	if t.closed {
		return ErrAlreadyClosed
	}

	ts := t.root.ts() + 1

	if len(kvs) == 0 {
		return ErrIllegalArguments
	}

	for _, kv := range kvs {
		if kv.K == nil || kv.V == nil {
			return ErrIllegalArguments
		}

		if len(kv.K)+len(kv.V)+45 > t.maxNodeSize {
			return ErrorMaxKVLenExceeded
		}

		k := make([]byte, len(kv.K))
		v := make([]byte, len(kv.V))

		copy(k, kv.K)
		copy(v, kv.V)

		n1, n2, err := t.root.insertAt(k, v, ts)
		if err != nil {
			return err
		}

		if n2 == nil {
			t.root = n1
		} else {
			newRoot := &innerNode{
				t:       t,
				nodes:   []node{n1, n2},
				_minKey: n1.minKey(),
				_maxKey: n2.maxKey(),
				_ts:     ts,
				maxSize: t.maxNodeSize,
				mut:     true,
			}

			t.root = newRoot
		}

		t.insertionCount++
	}

	if t.insertionCount >= t.flushThld {
		_, _, err := t.flushTree()
		return err
	}

	return nil
}

func (t *TBtree) Ts() uint64 {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	return t.root.ts()
}

func (t *TBtree) UnsafeSnashot() *Snapshot {
	t.rwmutex.RLock()
	return t.newSnapshot(0, t.root, true)
}

func (t *TBtree) Snapshot() (*Snapshot, error) {
	return t.SnapshotSince(0)
}

func (t *TBtree) SnapshotSince(ts uint64) (*Snapshot, error) {
	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return nil, ErrAlreadyClosed
	}

	if len(t.snapshots) == t.maxActiveSnapshots {
		return nil, ErrorToManyActiveSnapshots
	}

	if t.lastSnapRoot == nil || t.lastSnapRoot.ts() < ts ||
		(t.renewSnapRootAfter > 0 && time.Since(t.lastSnapRootAt) >= t.renewSnapRootAfter) {

		_, _, err := t.flushTree()
		if err != nil {
			return nil, err
		}
	}

	if !t.root.mutated() {
		t.lastSnapRoot = t.root
		t.lastSnapRootAt = time.Now()
	}

	t.maxSnapshotID++

	snapshot := t.newSnapshot(t.maxSnapshotID, t.lastSnapRoot, false)

	t.snapshots[snapshot.id] = snapshot

	return snapshot, nil
}

func (t *TBtree) newSnapshot(snapshotID uint64, root node, unsafe bool) *Snapshot {
	return &Snapshot{
		t:       t,
		id:      snapshotID,
		root:    root,
		unsafe:  unsafe,
		readers: make(map[int]io.Closer),
	}
}

func (t *TBtree) snapshotClosed(snapshot *Snapshot) error {
	if snapshot.unsafe {
		t.rwmutex.RUnlock()
		return nil
	}

	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	delete(t.snapshots, snapshot.id)

	return nil
}

func (n *innerNode) insertAt(key []byte, value []byte, ts uint64) (n1 node, n2 node, err error) {
	if !n.mut {
		return n.copyOnInsertAt(key, value, ts)
	}
	return n.updateOnInsertAt(key, value, ts)
}

func (n *innerNode) updateOnInsertAt(key []byte, value []byte, ts uint64) (n1 node, n2 node, err error) {
	insertAt := n.indexOf(key)

	c := n.nodes[insertAt]

	c1, c2, err := c.insertAt(key, value, ts)
	if err != nil {
		return nil, nil, err
	}

	n._ts = ts
	n.mut = true

	if c2 == nil {
		if bytes.Compare(n._minKey, c1.minKey()) > 0 {
			n._minKey = c1.minKey()
		}

		if bytes.Compare(n._maxKey, c1.maxKey()) < 0 {
			n._maxKey = c1.maxKey()
		}

		n.nodes[insertAt] = c1

		return n, nil, nil
	}

	if bytes.Compare(n._minKey, c2.minKey()) > 0 {
		n._minKey = c2.minKey()
	}

	if bytes.Compare(n._maxKey, c2.maxKey()) < 0 {
		n._maxKey = c2.maxKey()
	}

	nodes := make([]node, len(n.nodes)+1)

	copy(nodes[:insertAt], n.nodes[:insertAt])

	nodes[insertAt] = c1
	nodes[insertAt+1] = c2

	if insertAt+2 < len(nodes) {
		copy(nodes[insertAt+2:], n.nodes[insertAt+1:])
	}

	n.nodes = nodes

	n2, err = n.split()

	return n, n2, err
}

func (n *innerNode) copyOnInsertAt(key []byte, value []byte, ts uint64) (n1 node, n2 node, err error) {
	insertAt := n.indexOf(key)

	c := n.nodes[insertAt]

	c1, c2, err := c.insertAt(key, value, ts)
	if err != nil {
		return nil, nil, err
	}

	if c2 == nil {
		minKey := n._minKey
		if bytes.Compare(minKey, c1.minKey()) > 0 {
			minKey = c1.minKey()
		}

		maxKey := n._maxKey
		if bytes.Compare(maxKey, c1.maxKey()) < 0 {
			maxKey = c1.maxKey()
		}

		newNode := &innerNode{
			t:       n.t,
			nodes:   make([]node, len(n.nodes)),
			_minKey: minKey,
			_maxKey: maxKey,
			_ts:     ts,
			maxSize: n.maxSize,
			mut:     true,
		}

		copy(newNode.nodes[:insertAt], n.nodes[:insertAt])

		newNode.nodes[insertAt] = c1

		if insertAt+1 < len(newNode.nodes) {
			copy(newNode.nodes[insertAt+1:], n.nodes[insertAt+1:])
		}

		return newNode, nil, nil
	}

	minKey := n._minKey
	if bytes.Compare(minKey, c2.minKey()) > 0 {
		minKey = c2.minKey()
	}

	maxKey := n._maxKey
	if bytes.Compare(maxKey, c2.maxKey()) < 0 {
		maxKey = c2.maxKey()
	}

	newNode := &innerNode{
		t:       n.t,
		nodes:   make([]node, len(n.nodes)+1),
		_minKey: minKey,
		_maxKey: maxKey,
		_ts:     ts,
		maxSize: n.maxSize,
		mut:     true,
	}

	copy(newNode.nodes[:insertAt], n.nodes[:insertAt])

	newNode.nodes[insertAt] = c1
	newNode.nodes[insertAt+1] = c2

	if insertAt+2 < len(newNode.nodes) {
		copy(newNode.nodes[insertAt+2:], n.nodes[insertAt+1:])
	}

	n2, err = newNode.split()

	return newNode, n2, err
}

func (n *innerNode) get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	i := n.indexOf(key)

	if bytes.Compare(key, n.nodes[i].maxKey()) == 1 {
		return nil, 0, 0, ErrKeyNotFound
	}

	return n.nodes[i].get(key)
}

func (n *innerNode) history(key []byte, offset uint64, descOrder bool, limit int) ([]uint64, error) {
	i := n.indexOf(key)

	if bytes.Compare(key, n.nodes[i].maxKey()) == 1 {
		return nil, ErrKeyNotFound
	}

	return n.nodes[i].history(key, offset, descOrder, limit)
}

func (n *innerNode) findLeafNode(keyPrefix []byte, path path, neqKey []byte, descOrder bool) (path, *leafNode, int, error) {
	if descOrder {
		for i := len(n.nodes); i > 0; i-- {
			minKey := n.nodes[i-1].minKey()

			if len(neqKey) > 0 && bytes.Compare(minKey, neqKey) >= 0 {
				continue
			}

			if bytes.Compare(minKey, keyPrefix) < 1 {
				return n.nodes[i-1].findLeafNode(keyPrefix, append(path, n), neqKey, descOrder)
			}
		}

		return nil, nil, 0, ErrKeyNotFound
	}

	for _, c := range n.nodes {
		maxKey := c.maxKey()

		if len(neqKey) > 0 && bytes.Compare(maxKey, neqKey) <= 0 {
			continue
		}

		if bytes.Compare(keyPrefix, maxKey) < 1 {
			return c.findLeafNode(keyPrefix, append(path, n), neqKey, descOrder)
		}
	}

	return nil, nil, 0, ErrKeyNotFound
}

func (n *innerNode) ts() uint64 {
	return n._ts
}

func (n *innerNode) size() int {
	size := 1 // Node type

	size += 4 // Size

	size += 4 // Child count

	for _, c := range n.nodes {
		size += 4               // minKey length
		size += len(c.minKey()) // minKey
		size += 4               // maxKey length
		size += len(c.maxKey()) // maxKey
		size += 8               // Ts
		size += 4               // Size
		size += 8               // Offset
	}

	return size
}

func (n *innerNode) mutated() bool {
	return n.mut
}

func (n *innerNode) offset() int64 {
	return n.off
}

func (n *innerNode) minKey() []byte {
	return n._minKey
}

func (n *innerNode) maxKey() []byte {
	return n._maxKey
}

func (n *innerNode) indexOf(key []byte) int {
	for i := 0; i < len(n.nodes); i++ {
		if bytes.Compare(key, n.nodes[i].maxKey()) < 1 {
			return i
		}
	}
	return len(n.nodes) - 1
}

func (n *innerNode) split() (node, error) {
	if n.size() <= n.maxSize {
		return nil, nil
	}

	splitIndex, _ := n.splitInfo()

	newNode := &innerNode{
		t:       n.t,
		nodes:   n.nodes[splitIndex:],
		_minKey: n.nodes[splitIndex].minKey(),
		_maxKey: n._maxKey,
		maxSize: n.maxSize,
		mut:     true,
	}
	newNode.updateTs()

	n.nodes = n.nodes[:splitIndex]

	n._maxKey = n.nodes[splitIndex-1].maxKey()

	n.updateTs()

	return newNode, nil
}

func (n *innerNode) splitInfo() (splitIndex int, splitSize int) {
	for i := 0; i < len(n.nodes); i++ {
		splitIndex = i
		if splitSize+len(n.nodes[i].maxKey()) > n.maxSize {
			break
		}
		splitSize += len(n.nodes[i].maxKey())
	}
	return
}

func (n *innerNode) updateTs() {
	n._ts = 0
	for i := 0; i < len(n.nodes); i++ {
		if n.ts() < n.nodes[i].ts() {
			n._ts = n.nodes[i].ts()
		}
	}
	return
}

////////////////////////////////////////////////////////////

func (r *nodeRef) insertAt(key []byte, value []byte, ts uint64) (n1 node, n2 node, err error) {
	n, err := r.t.nodeAt(r.off)
	if err != nil {
		return nil, nil, err
	}
	return n.insertAt(key, value, ts)
}

func (r *nodeRef) get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	n, err := r.t.nodeAt(r.off)
	if err != nil {
		return nil, 0, 0, err
	}
	return n.get(key)
}

func (r *nodeRef) history(key []byte, offset uint64, descOrder bool, limit int) ([]uint64, error) {
	n, err := r.t.nodeAt(r.off)
	if err != nil {
		return nil, err
	}
	return n.history(key, offset, descOrder, limit)
}

func (r *nodeRef) findLeafNode(keyPrefix []byte, path path, neqKey []byte, descOrder bool) (path, *leafNode, int, error) {
	n, err := r.t.nodeAt(r.off)
	if err != nil {
		return nil, nil, 0, err
	}
	return n.findLeafNode(keyPrefix, path, neqKey, descOrder)
}

func (r *nodeRef) minKey() []byte {
	return r._minKey
}

func (r *nodeRef) maxKey() []byte {
	return r._maxKey
}

func (r *nodeRef) ts() uint64 {
	return r._ts
}

func (r *nodeRef) size() int {
	return r._size
}

func (r *nodeRef) mutated() bool {
	return false
}

func (r *nodeRef) offset() int64 {
	return r.off
}

////////////////////////////////////////////////////////////

func (l *leafNode) insertAt(key []byte, value []byte, ts uint64) (n1 node, n2 node, err error) {
	if !l.mut {
		return l.copyOnInsertAt(key, value, ts)
	}
	return l.updateOnInsertAt(key, value, ts)
}

func (l *leafNode) updateOnInsertAt(key []byte, value []byte, ts uint64) (n1 node, n2 node, err error) {
	i, found := l.indexOf(key)

	l._ts = ts
	l.mut = true

	if found {
		l.values[i].value = value
		l.values[i].ts = ts
		l.values[i].tss = append([]uint64{ts}, l.values[i].tss...)

		return l, nil, nil
	}

	if bytes.Compare(l._minKey, key) > 0 {
		l._minKey = key
	}

	if bytes.Compare(l._maxKey, key) < 0 {
		l._maxKey = key
	}

	values := make([]*leafValue, len(l.values)+1)

	copy(values[:i], l.values[:i])

	values[i] = &leafValue{
		key:    key,
		value:  value,
		ts:     ts,
		tss:    []uint64{ts},
		hOff:   -1,
		hCount: 0,
	}

	if i+1 < len(values) {
		copy(values[i+1:], l.values[i:])
	}

	l.values = values

	n2, err = l.split()

	return l, n2, err
}

func (l *leafNode) copyOnInsertAt(key []byte, value []byte, ts uint64) (n1 node, n2 node, err error) {
	i, found := l.indexOf(key)

	if found {
		newLeaf := &leafNode{
			t:       l.t,
			values:  make([]*leafValue, len(l.values)),
			_minKey: l._minKey,
			_maxKey: l._maxKey,
			_ts:     ts,
			maxSize: l.maxSize,
			mut:     true,
		}

		for pi := 0; pi < i; pi++ {
			newLeaf.values[pi] = &leafValue{
				key:    l.values[pi].key,
				value:  l.values[pi].value,
				ts:     l.values[pi].ts,
				tss:    l.values[pi].tss,
				hOff:   l.values[pi].hOff,
				hCount: l.values[pi].hCount,
			}
		}

		newLeaf.values[i] = &leafValue{
			key:    key,
			value:  value,
			ts:     ts,
			tss:    append([]uint64{ts}, l.values[i].tss...),
			hOff:   l.values[i].hOff,
			hCount: l.values[i].hCount,
		}

		for pi := i + 1; pi < len(newLeaf.values); pi++ {
			newLeaf.values[pi] = &leafValue{
				key:    l.values[pi].key,
				value:  l.values[pi].value,
				ts:     l.values[pi].ts,
				tss:    l.values[pi].tss,
				hOff:   l.values[pi].hOff,
				hCount: l.values[pi].hCount,
			}
		}

		return newLeaf, nil, nil
	}

	minKey := l._minKey
	if bytes.Compare(minKey, key) > 0 {
		minKey = key
	}

	maxKey := l._maxKey
	if bytes.Compare(maxKey, key) < 0 {
		maxKey = key
	}

	newLeaf := &leafNode{
		t:       l.t,
		values:  make([]*leafValue, len(l.values)+1),
		_minKey: minKey,
		_maxKey: maxKey,
		_ts:     ts,
		maxSize: l.maxSize,
		mut:     true,
	}

	for pi := 0; pi < i; pi++ {
		newLeaf.values[pi] = &leafValue{
			key:    l.values[pi].key,
			value:  l.values[pi].value,
			ts:     l.values[pi].ts,
			tss:    l.values[pi].tss,
			hOff:   l.values[pi].hOff,
			hCount: l.values[pi].hCount,
		}
	}

	newLeaf.values[i] = &leafValue{
		key:    key,
		value:  value,
		ts:     ts,
		tss:    []uint64{ts},
		hOff:   -1,
		hCount: 0,
	}

	for pi := i + 1; pi < len(newLeaf.values); pi++ {
		newLeaf.values[pi] = &leafValue{
			key:    l.values[pi-1].key,
			value:  l.values[pi-1].value,
			ts:     l.values[pi-1].ts,
			tss:    l.values[pi-1].tss,
			hOff:   l.values[pi-1].hOff,
			hCount: l.values[pi-1].hCount,
		}
	}

	n2, err = newLeaf.split()

	return newLeaf, n2, err
}

func (l *leafNode) get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	i, found := l.indexOf(key)

	if !found {
		return nil, 0, 0, ErrKeyNotFound
	}

	leafValue := l.values[i]
	return leafValue.value, leafValue.ts, leafValue.hCount + uint64(len(leafValue.tss)), nil
}

func (l *leafNode) history(key []byte, offset uint64, desc bool, limit int) ([]uint64, error) {
	i, found := l.indexOf(key)

	if !found {
		return nil, ErrKeyNotFound
	}

	leafValue := l.values[i]

	hCount := leafValue.hCount + uint64(len(leafValue.tss))

	if offset == hCount {
		return nil, ErrNoMoreEntries
	}

	if offset > hCount {
		return nil, ErrOffsetOutOfRange
	}

	tssLen := limit
	if uint64(limit) > hCount-offset {
		tssLen = int(hCount - offset)
	}

	tss := make([]uint64, tssLen)

	initAt := offset
	tssOff := 0

	if !desc {
		initAt = hCount - offset - uint64(tssLen)
	}

	if initAt < uint64(len(leafValue.tss)) {
		for i := int(initAt); i < len(leafValue.tss) && tssOff < tssLen; i++ {
			if desc {
				tss[tssOff] = leafValue.tss[i]
			} else {
				tss[tssLen-1-tssOff] = leafValue.tss[i]
			}

			tssOff++
		}
	}

	hOff := leafValue.hOff

	ti := uint64(len(leafValue.tss))

	for tssOff < tssLen {
		r := appendable.NewReaderFrom(l.t.hLog, hOff, DefaultMaxNodeSize)

		hc, err := r.ReadUint32()
		if err != nil {
			return nil, err
		}

		for i := 0; i < int(hc) && tssOff < tssLen; i++ {
			ts, err := r.ReadUint64()
			if err != nil {
				return nil, err
			}

			if ti < initAt {
				ti++
				continue
			}

			if desc {
				tss[tssOff] = ts
			} else {
				tss[tssLen-1-tssOff] = ts
			}

			tssOff++
		}

		prevOff, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}

		hOff = int64(prevOff)
	}

	return tss, nil
}

func (l *leafNode) findLeafNode(keyPrefix []byte, path path, neqKey []byte, descOrder bool) (path, *leafNode, int, error) {
	if descOrder {
		for i := len(l.values); i > 0; i-- {
			key := l.values[i-1].key

			if len(neqKey) > 0 && bytes.Compare(key, neqKey) >= 0 {
				continue
			}

			if bytes.Compare(key, keyPrefix) < 1 {
				return path, l, i - 1, nil
			}
		}

		return nil, nil, 0, ErrKeyNotFound
	}

	for i, v := range l.values {
		if len(neqKey) > 0 && bytes.Compare(v.key, neqKey) <= 0 {
			continue
		}

		if bytes.Compare(keyPrefix, v.key) < 1 {
			return path, l, i, nil
		}
	}

	return nil, nil, 0, ErrKeyNotFound
}

func (l *leafNode) indexOf(key []byte) (index int, found bool) {
	for i := 0; i < len(l.values); i++ {
		if bytes.Equal(l.values[i].key, key) {
			return i, true
		}

		if bytes.Compare(l.values[i].key, key) == 1 {
			return i, false
		}
	}

	return len(l.values), false
}

func (l *leafNode) minKey() []byte {
	return l._minKey
}

func (l *leafNode) maxKey() []byte {
	return l._maxKey
}

func (l *leafNode) ts() uint64 {
	return l._ts
}

func (l *leafNode) size() int {
	size := 1 // Node type

	size += 4 // Size

	size += 4 // kv count

	for _, kv := range l.values {
		size += 4             // Key length
		size += len(kv.key)   // Key
		size += 4             // Value length
		size += len(kv.value) // Value
		size += 8             // Ts
		size += 8             // hOff
		size += 8             // hCount
	}

	return size
}

func (l *leafNode) mutated() bool {
	return l.mut
}

func (l *leafNode) offset() int64 {
	return l.off
}

func (l *leafNode) split() (node, error) {
	if l.size() <= l.maxSize {
		return nil, nil
	}

	splitIndex, _ := l.splitInfo()

	newLeaf := &leafNode{
		t:       l.t,
		values:  l.values[splitIndex:],
		_minKey: l.values[splitIndex].key,
		_maxKey: l._maxKey,
		maxSize: l.maxSize,
		mut:     true,
	}
	newLeaf.updateTs()

	l.values = l.values[:splitIndex]

	l._maxKey = l.values[splitIndex-1].key

	l.updateTs()

	return newLeaf, nil
}

func (l *leafNode) splitInfo() (splitIndex int, splitSize int) {
	for i := 0; i < len(l.values); i++ {
		splitIndex = i
		if splitSize+l.values[i].size() > l.maxSize {
			break
		}
		splitSize += l.values[i].size()
	}

	return
}

func (l *leafNode) updateTs() {
	l._ts = 0

	for i := 0; i < len(l.values); i++ {
		if l._ts < l.values[i].ts {
			l._ts = l.values[i].ts
		}
	}

	return
}

func (lv *leafValue) size() int {
	return 16 + len(lv.key) + len(lv.value)
}

func (lv *leafValue) asBefore(hLog appendable.Appendable, beforeTs uint64) (uint64, error) {
	if lv.ts < beforeTs {
		return lv.ts, nil
	}

	for _, ts := range lv.tss {
		if ts < beforeTs {
			return ts, nil
		}
	}

	hOff := lv.hOff

	for i := uint64(0); i < lv.hCount; i++ {
		r := appendable.NewReaderFrom(hLog, hOff, DefaultMaxNodeSize)

		hc, err := r.ReadUint32()
		if err != nil {
			return 0, err
		}

		for i := 0; i < int(hc); i++ {
			ts, err := r.ReadUint64()
			if err != nil {
				return 0, err
			}

			if ts < beforeTs {
				return ts, nil
			}
		}

		prevOff, err := r.ReadUint64()
		if err != nil {
			return 0, err
		}

		hOff = int64(prevOff)
	}

	return 0, ErrKeyNotFound
}
