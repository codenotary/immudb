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
package tbtree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/multiapp"
	"codenotary.io/immudb-v2/cache"
)

var ErrIllegalArgument = errors.New("illegal arguments")
var ErrReadingFileContent = errors.New("error reading required file content")
var ErrKeyNotFound = errors.New("key not found")
var ErrIllegalState = errors.New("illegal state")
var ErrAlreadyClosed = errors.New("already closed")
var ErrSnapshotsNotClosed = errors.New("snapshots not closed")
var ErrorToManyActiveSnapshots = errors.New("max active snapshots limit reached")
var ErrCorruptedFile = errors.New("file is corrupted")

const MinNodeSize = 96
const MinCacheSize = 1
const DefaultMaxNodeSize = 4096
const DefaultFlushThld = 100_000
const DefaultMaxActiveSnapshots = 100
const DefaultRenewSnapRootAfter = time.Duration(1000) * time.Millisecond
const DefaultCacheSize = 10000
const DefaultFileMode = 0755

const Version = 1

const KeySpace = 32 // ts trace len per key, number of key updates traced within a same key and leaf node

const (
	MetaVersion     = "VERSION"
	MetaMaxNodeSize = "MAX_NODE_SIZE"
	MetaFileSize    = "FILE_SIZE"
)

// TBTree implements a timed-btree
type TBtree struct {
	aof    appendable.Appendable
	cache  *cache.LRUCache
	nmutex sync.Mutex // mutex for cache and file reading

	// bloom filter

	root               node
	maxNodeSize        int
	insertionCount     int
	flushThld          int
	maxActiveSnapshots int
	renewSnapRootAfter time.Duration
	readOnly           bool
	snapshots          map[uint64]*Snapshot
	maxSnapshotID      uint64
	lastSnapRoot       node
	lastSnapRootAt     time.Time
	currentOffset      int64
	closed             bool
	mutex              sync.Mutex
}

type Options struct {
	flushThld          int
	maxActiveSnapshots int
	renewSnapRootAfter time.Duration
	cacheSize          int
	readOnly           bool
	synced             bool
	fileMode           os.FileMode

	// options below are only set during initialization and stored as metadata
	maxNodeSize int
	fileSize    int
}

func DefaultOptions() *Options {
	return &Options{
		flushThld:          DefaultFlushThld,
		maxActiveSnapshots: DefaultMaxActiveSnapshots,
		renewSnapRootAfter: DefaultRenewSnapRootAfter,
		cacheSize:          DefaultCacheSize,
		readOnly:           false,
		synced:             false,
		fileMode:           DefaultFileMode,

		// options below are only set during initialization and stored as metadata
		maxNodeSize: DefaultMaxNodeSize,
		fileSize:    multiapp.DefaultFileSize,
	}
}

func (opt *Options) SetFlushThld(flushThld int) *Options {
	opt.flushThld = flushThld
	return opt
}

func (opt *Options) SetMaxActiveSnapshots(maxActiveSnapshots int) *Options {
	opt.maxActiveSnapshots = maxActiveSnapshots
	return opt
}

func (opt *Options) SetRenewSnapRootAfter(renewSnapRootAfter time.Duration) *Options {
	opt.renewSnapRootAfter = renewSnapRootAfter
	return opt
}

func (opt *Options) SetCacheSize(cacheSize int) *Options {
	opt.cacheSize = cacheSize
	return opt
}

func (opt *Options) SetReadOnly(readOnly bool) *Options {
	opt.readOnly = readOnly
	return opt
}

func (opt *Options) SetSynced(synced bool) *Options {
	opt.synced = synced
	return opt
}

func (opt *Options) SetFileMode(fileMode os.FileMode) *Options {
	opt.fileMode = fileMode
	return opt
}

func (opt *Options) SetMaxNodeSize(maxNodeSize int) *Options {
	opt.maxNodeSize = maxNodeSize
	return opt
}

func (opt *Options) SetFileSize(fileSize int) *Options {
	opt.fileSize = fileSize
	return opt
}

type path []*innerNode

type node interface {
	insertAt(key []byte, value []byte, ts uint64) (node, node, error)
	get(key []byte) (value []byte, ts uint64, err error)
	getTs(key []byte, limit int64) ([]uint64, error)
	findLeafNode(keyPrefix []byte, path path, neqKey []byte, ascOrder bool) (path, *leafNode, int, error)
	maxKey() []byte
	ts() uint64
	size() int
	mutated() bool
	offset() int64
	writeTo(w io.Writer, asRoot bool, writeOpts *WriteOpts) (int64, int64, error)
}

type WriteOpts struct {
	OnlyMutated bool
	BaseOffset  int64
	commitLog   bool
}

type innerNode struct {
	t       *TBtree
	nodes   []node
	_maxKey []byte
	_ts     uint64
	maxSize int
	off     int64
	mut     bool
}

type leafNode struct {
	t        *TBtree
	prevNode node
	values   []*leafValue
	_maxKey  []byte
	_ts      uint64
	maxSize  int
	off      int64
	mut      bool
}

type nodeRef struct {
	t       *TBtree
	_maxKey []byte
	_ts     uint64
	_size   int
	off     int64
}

type leafValue struct {
	key   []byte
	value []byte
	ts    uint64
	tss   []uint64
}

func validOptions(opts *Options) bool {
	return opts != nil &&
		opts.maxNodeSize >= MinNodeSize &&
		opts.flushThld > 0 &&
		opts.maxActiveSnapshots > 0 &&
		opts.renewSnapRootAfter > 0 &&
		opts.cacheSize >= MinCacheSize
}

func Open(path string, opts *Options) (*TBtree, error) {
	if !validOptions(opts) {
		return nil, ErrIllegalArgument
	}

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetaVersion, Version)
	metadata.PutInt(MetaMaxNodeSize, opts.maxNodeSize)
	metadata.PutInt(MetaFileSize, opts.fileSize)

	appendableOpts := multiapp.DefaultOptions().
		SetReadOnly(opts.readOnly).
		SetSynced(opts.synced).
		SetFileSize(opts.fileSize).
		SetFileMode(opts.fileMode).
		SetFileExt("ki").
		SetMetadata(metadata.Bytes())

	aof, err := multiapp.Open(path, appendableOpts)
	if err != nil {
		return nil, err
	}

	return OpenWith(aof, opts)
}

func OpenWith(aof appendable.Appendable, opts *Options) (*TBtree, error) {
	if !validOptions(opts) {
		return nil, ErrIllegalArgument
	}

	metadata := appendable.NewMetadata(aof.Metadata())

	fileSize, ok := metadata.GetInt(MetaFileSize)
	if !ok {
		return nil, ErrCorruptedFile
	}

	maxNodeSize, ok := metadata.GetInt(MetaMaxNodeSize)
	if !ok {
		return nil, ErrCorruptedFile
	}

	mapp, ok := aof.(*multiapp.MultiFileAppendable)
	if ok {
		mapp.SetFileSize(fileSize)
	}

	cache, err := cache.NewLRUCache(opts.cacheSize)
	if err != nil {
		return nil, err
	}

	t := &TBtree{
		aof:                aof,
		cache:              cache,
		maxNodeSize:        maxNodeSize,
		flushThld:          opts.flushThld,
		renewSnapRootAfter: opts.renewSnapRootAfter,
		maxActiveSnapshots: opts.maxActiveSnapshots,
		readOnly:           opts.readOnly,
		snapshots:          make(map[uint64]*Snapshot),
	}

	aofSize, err := aof.Size()
	if err != nil {
		return nil, err
	}

	var root node

	if aofSize == 0 {
		root = &leafNode{t: t, maxSize: maxNodeSize, mut: true}
	} else {
		bs := make([]byte, 4)
		_, err := aof.ReadAt(bs, aofSize-4)
		if err != nil {
			return nil, err
		}

		rootSize := int(binary.BigEndian.Uint32(bs))
		rootOffset := aofSize - 4 - int64(rootSize)

		root, err = t.readNodeAt(rootOffset)
		if err != nil {
			return nil, err
		}

		t.currentOffset = aofSize
	}

	t.root = root

	return t, nil
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
	r := appendable.NewReaderFrom(t.aof, off, t.maxNodeSize)
	return t.readNodeFrom(r)
}

func (t *TBtree) readNodeFrom(r *appendable.Reader) (node, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	_, err = r.ReadUint32() // size is not currently used
	if err != nil {
		return nil, err
	}

	switch b {
	case InnerNodeType:
		return t.readInnerNodeFrom(r, false)
	case RootInnerNodeType:
		return t.readInnerNodeFrom(r, true)
	case LeafNodeType:
		return t.readLeafNodeFrom(r, false)
	case RootLeafNodeType:
		return t.readLeafNodeFrom(r, true)
	}

	return nil, ErrReadingFileContent
}

func (t *TBtree) readInnerNodeFrom(r *appendable.Reader, asRoot bool) (*innerNode, error) {
	off := r.Offset()

	childCount, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}

	n := &innerNode{
		t:       t,
		nodes:   make([]node, childCount),
		_maxKey: nil,
		_ts:     0,
		maxSize: t.maxNodeSize,
		off:     off,
	}

	for c := 0; c < int(childCount); c++ {
		nref, err := t.readNodeRefFrom(r)
		if err != nil {
			return nil, err
		}

		n.nodes[c] = nref

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
	ksize, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}

	key := make([]byte, ksize)
	_, err = r.Read(key)
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
		_maxKey: key,
		_ts:     ts,
		_size:   int(size),
		off:     int64(off),
	}, nil
}

func (t *TBtree) readLeafNodeFrom(r *appendable.Reader, asRoot bool) (*leafNode, error) {
	off := r.Offset()

	prevNodeOff, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	var prevNode *nodeRef
	if int64(prevNodeOff) >= 0 {
		prevNode = &nodeRef{
			t:   t,
			off: int64(prevNodeOff),
		}
	}

	valueCount, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}

	l := &leafNode{
		t:        t,
		prevNode: prevNode,
		values:   make([]*leafValue, valueCount),
		_maxKey:  nil,
		_ts:      0,
		maxSize:  t.maxNodeSize,
		off:      off,
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

		tsLen, err := r.ReadUint32()
		if err != nil {
			return nil, err
		}

		tss := make([]uint64, tsLen)

		for i := 0; i < int(tsLen); i++ {
			t, err := r.ReadUint64()
			if err != nil {
				return nil, err
			}
			tss[i] = t
		}

		leafValue := &leafValue{
			key:   key,
			value: value,
			tss:   tss,
			ts:    ts,
		}

		l.values[c] = leafValue

		if bytes.Compare(l._maxKey, leafValue.key) < 0 {
			l._maxKey = leafValue.key
		}

		if l._ts < leafValue.ts {
			l._ts = leafValue.ts
		}
	}

	return l, nil
}

func (t *TBtree) Sync() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	return t.aof.Sync()
}

func (t *TBtree) Flush() (int64, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return 0, ErrAlreadyClosed
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

func (t *TBtree) flushTree() (int64, error) {
	if !t.root.mutated() {
		return 0, nil
	}

	snapshot := t.newSnapshot(0, t.root)

	wopts := &WriteOpts{
		OnlyMutated: true,
		BaseOffset:  t.currentOffset,
		commitLog:   true,
	}

	n, err := snapshot.WriteTo(&appendableWriter{t.aof}, wopts)
	if err != nil {
		return 0, err
	}

	err = t.aof.Flush()
	if err != nil {
		return 0, err
	}

	t.insertionCount = 0
	t.currentOffset += n

	return n, nil
}

func (t *TBtree) Close() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	if len(t.snapshots) > 0 {
		return ErrSnapshotsNotClosed
	}

	_, err := t.flushTree()
	if err != nil {
		return err
	}

	err = t.aof.Close()
	if err != nil {
		return err
	}

	t.closed = true

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
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	ts := t.root.ts() + 1

	if len(kvs) == 0 {
		return ErrIllegalArgument
	}

	for _, kv := range kvs {
		if kv.K == nil {
			return ErrIllegalArgument
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
				_maxKey: n2.maxKey(),
				_ts:     ts,
				maxSize: t.maxNodeSize,
				mut:     true,
			}

			t.root = newRoot
		}
	}

	t.insertionCount++

	if t.insertionCount == t.flushThld {
		_, err := t.flushTree()
		return err
	}

	return nil
}

func (t *TBtree) Ts() uint64 {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.root.ts()
}

func (t *TBtree) Snapshot() (*Snapshot, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return nil, ErrAlreadyClosed
	}

	if len(t.snapshots) == t.maxActiveSnapshots {
		return nil, ErrorToManyActiveSnapshots
	}

	if t.lastSnapRoot == nil || time.Since(t.lastSnapRootAt) >= t.renewSnapRootAfter {
		_, err := t.flushTree()
		if err != nil {
			return nil, err
		}
	}

	if !t.root.mutated() {
		t.lastSnapRoot = t.root
		t.lastSnapRootAt = time.Now()
	}

	t.maxSnapshotID++

	snapshot := t.newSnapshot(t.maxSnapshotID, t.lastSnapRoot)

	t.snapshots[snapshot.id] = snapshot

	return snapshot, nil
}

func (t *TBtree) newSnapshot(snapshotID uint64, root node) *Snapshot {
	return &Snapshot{
		t:       t,
		id:      snapshotID,
		root:    root,
		readers: make(map[int]*Reader),
	}
}

func (t *TBtree) snapshotClosed(snapshot *Snapshot) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

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
		if bytes.Compare(n._maxKey, c1.maxKey()) < 0 {
			n._maxKey = c1.maxKey()
		}

		n.nodes[insertAt] = c1

		return n, nil, nil
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
		maxKey := n._maxKey
		if bytes.Compare(maxKey, c1.maxKey()) < 0 {
			maxKey = c1.maxKey()
		}

		newNode := &innerNode{
			t:       n.t,
			nodes:   make([]node, len(n.nodes)),
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

	maxKey := n._maxKey
	if bytes.Compare(maxKey, c2.maxKey()) < 0 {
		maxKey = c2.maxKey()
	}

	newNode := &innerNode{
		t:       n.t,
		nodes:   make([]node, len(n.nodes)+1),
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

func (n *innerNode) get(key []byte) (value []byte, ts uint64, err error) {
	i := n.indexOf(key)

	if bytes.Compare(key, n.nodes[i].maxKey()) == 1 {
		return nil, 0, ErrKeyNotFound
	}

	return n.nodes[i].get(key)
}

func (n *innerNode) getTs(key []byte, limit int64) ([]uint64, error) {
	i := n.indexOf(key)

	if bytes.Compare(key, n.nodes[i].maxKey()) == 1 {
		return nil, ErrKeyNotFound
	}

	return n.nodes[i].getTs(key, limit)
}

func (n *innerNode) findLeafNode(keyPrefix []byte, path path, neqKey []byte, ascOrder bool) (path, *leafNode, int, error) {
	if ascOrder || neqKey == nil {
		for i := 0; i < len(n.nodes); i++ {
			if bytes.Compare(keyPrefix, n.nodes[i].maxKey()) < 1 && bytes.Compare(n.nodes[i].maxKey(), neqKey) == 1 {
				return n.nodes[i].findLeafNode(keyPrefix, append(path, n), neqKey, ascOrder)
			}
		}

		if ascOrder {
			return nil, nil, 0, ErrKeyNotFound
		}

		return n.nodes[len(n.nodes)-1].findLeafNode(keyPrefix, append(path, n), neqKey, ascOrder)
	}

	for i := len(n.nodes); i > 0; i-- {
		if bytes.Compare(n.nodes[i-1].maxKey(), keyPrefix) < 1 && bytes.Compare(n.nodes[i-1].maxKey(), neqKey) < 0 {
			return n.nodes[i-1].findLeafNode(keyPrefix, append(path, n), neqKey, ascOrder)
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
		size += 4               // Key length
		size += len(c.maxKey()) // Key
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

func (r *nodeRef) get(key []byte) (value []byte, ts uint64, err error) {
	n, err := r.t.nodeAt(r.off)
	if err != nil {
		return nil, 0, err
	}
	return n.get(key)
}

func (r *nodeRef) getTs(key []byte, limit int64) ([]uint64, error) {
	n, err := r.t.nodeAt(r.off)
	if err != nil {
		return nil, err
	}
	return n.getTs(key, limit)
}

func (r *nodeRef) findLeafNode(keyPrefix []byte, path path, neqKey []byte, ascOrder bool) (path, *leafNode, int, error) {
	n, err := r.t.nodeAt(r.off)
	if err != nil {
		return nil, nil, 0, err
	}
	return n.findLeafNode(keyPrefix, path, neqKey, ascOrder)
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
	i, found := l.indexOf(key) // TODO: avoid calling indexOf twice
	enoughKeySpace := !found || len(l.values[i].tss)+1 < KeySpace

	if !l.mut || !enoughKeySpace {
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
		l.values[i].tss = append(l.values[i].tss, ts)

		return l, nil, nil
	}

	if bytes.Compare(l._maxKey, key) < 0 {
		l._maxKey = key
	}

	values := make([]*leafValue, len(l.values)+1)

	copy(values[:i], l.values[:i])

	values[i] = &leafValue{
		key:   key,
		value: value,
		ts:    ts,
		tss:   []uint64{ts},
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
			t:        l.t,
			prevNode: l,
			values:   make([]*leafValue, len(l.values)),
			_maxKey:  l._maxKey,
			_ts:      ts,
			maxSize:  l.maxSize,
			mut:      true,
		}

		for pi := 0; pi < i; pi++ {
			newLeaf.values[pi] = &leafValue{
				key:   l.values[pi].key,
				value: l.values[pi].value,
				ts:    l.values[pi].ts,
				tss:   nil,
			}
		}

		newLeaf.values[i] = &leafValue{
			key:   key,
			value: value,
			ts:    ts,
			tss:   []uint64{ts},
		}

		for pi := i + 1; pi < len(newLeaf.values); pi++ {
			newLeaf.values[pi] = &leafValue{
				key:   l.values[pi].key,
				value: l.values[pi].value,
				ts:    l.values[pi].ts,
				tss:   nil,
			}
		}

		return newLeaf, nil, nil
	}

	maxKey := l._maxKey
	if bytes.Compare(maxKey, key) < 0 {
		maxKey = key
	}

	newLeaf := &leafNode{
		t:        l.t,
		prevNode: l,
		values:   make([]*leafValue, len(l.values)+1),
		_maxKey:  maxKey,
		_ts:      ts,
		maxSize:  l.maxSize,
		mut:      true,
	}

	for pi := 0; pi < i; pi++ {
		newLeaf.values[pi] = &leafValue{
			key:   l.values[pi].key,
			value: l.values[pi].value,
			ts:    l.values[pi].ts,
			tss:   nil,
		}
	}

	newLeaf.values[i] = &leafValue{
		key:   key,
		value: value,
		ts:    ts,
		tss:   []uint64{ts},
	}

	for pi := i + 1; pi < len(newLeaf.values); pi++ {
		newLeaf.values[pi] = &leafValue{
			key:   l.values[pi-1].key,
			value: l.values[pi-1].value,
			ts:    l.values[pi-1].ts,
			tss:   nil,
		}
	}

	n2, err = newLeaf.split()

	return newLeaf, n2, err
}

func (l *leafNode) get(key []byte) (value []byte, ts uint64, err error) {
	i, found := l.indexOf(key)

	if !found {
		return nil, 0, ErrKeyNotFound
	}

	leafValue := l.values[i]
	return leafValue.value, leafValue.ts, nil
}

func (l *leafNode) getTs(key []byte, limit int64) ([]uint64, error) {
	i, found := l.indexOf(key)

	if !found {
		return nil, ErrKeyNotFound
	}

	leafValue := l.values[i]

	tsLen := len(leafValue.tss)
	if limit < int64(tsLen) {
		tsLen = int(limit)
	}

	tss := make([]uint64, tsLen)
	for i := 0; i < tsLen; i++ {
		tss[i] = leafValue.tss[len(leafValue.tss)-1-i]
	}

	if int64(tsLen) < limit && l.prevNode != nil {
		pts, err := l.prevNode.getTs(key, limit-int64(tsLen))
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		tss = append(tss, pts...)
	}

	return tss, nil
}

func (l *leafNode) findLeafNode(keyPrefix []byte, path path, neqKey []byte, ascOrder bool) (path, *leafNode, int, error) {
	if ascOrder || neqKey == nil {
		for i := 0; i < len(l.values); i++ {
			if bytes.Compare(keyPrefix, l.values[i].key) < 1 && bytes.Compare(l.values[i].key, neqKey) == 1 {
				return path, l, i, nil
			}
		}

		if ascOrder || len(l.values) == 0 {
			return nil, nil, 0, ErrKeyNotFound
		}

		return path, l, len(l.values) - 1, nil
	}

	for i := len(l.values); i > 0; i-- {
		if bytes.Compare(l.values[i-1].key, keyPrefix) < 1 && bytes.Compare(l.values[i-1].key, neqKey) < 0 {
			return path, l, i - 1, nil
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

func (l *leafNode) maxKey() []byte {
	return l._maxKey
}

func (l *leafNode) ts() uint64 {
	return l._ts
}

func (l *leafNode) size() int {
	size := 1 // Node type

	size += 4 // Size

	size += 8 // prevNode offset

	size += 4 // kv count

	for _, kv := range l.values {
		size += 4               // Key length
		size += len(kv.key)     // Key
		size += 4               // Value length
		size += len(kv.value)   // Value
		size += 8               // Ts
		size += 4               // ts length
		size += 8 * len(kv.tss) // Tss
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
		t:        l.t,
		prevNode: l.prevNode,
		values:   l.values[splitIndex:],
		_maxKey:  l._maxKey,
		maxSize:  l.maxSize,
		mut:      true,
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
