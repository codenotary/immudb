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
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"codenotary.io/immudb-v2/cache"
)

var ErrIllegalArgument = errors.New("illegal arguments")
var ErrReadingRequiredFileContent = errors.New("error reading required file content")
var ErrKeyNotFound = errors.New("key not found")
var ErrIllegalState = errors.New("illegal state")
var ErrAlreadyClosed = errors.New("already closed")
var ErrSnapshotsNotClosed = errors.New("snapshots not closed")
var ErrorMaxActiveSnapshotLimitReached = errors.New("max active snapshots limit reached")

const MinNodeSize = 96
const MinCacheSize = 1
const DefaultMaxNodeSize = 4096
const DefaultInsertionCountThreshold = 100_000
const DefaultMaxActiveSnapshots = 100
const DefaultReuseSnapshotThreshold = 1_000_000
const DefaultCacheSize = 10000
const DefaultFileMode = 0644

// TBTree implements a timed-btree
type TBtree struct {
	f      *os.File
	cache  *cache.LRUCache
	nmutex sync.Mutex // mutex for cache and file reading

	// bloom filter

	root                    node
	maxNodeSize             int
	insertionCount          int
	insertionCountThreshold int
	maxActiveSnapshots      int
	reuseSnapshotThreshold  int
	readOnly                bool
	snapshots               map[uint64]*Snapshot
	maxSnapshotID           uint64
	lastSnapshotRoot        node
	currentOffset           int64
	closed                  bool
	mutex                   sync.Mutex
}

type Options struct {
	maxNodeSize             int
	insertionCountThreshold int
	maxActiveSnapshots      int
	reuseSnapshotThreshold  int
	cacheSize               int
	readOnly                bool
	fileMode                os.FileMode
}

func DefaultOptions() *Options {
	return &Options{
		maxNodeSize:             DefaultMaxNodeSize,
		insertionCountThreshold: DefaultInsertionCountThreshold,
		maxActiveSnapshots:      DefaultMaxActiveSnapshots,
		reuseSnapshotThreshold:  DefaultReuseSnapshotThreshold,
		cacheSize:               DefaultCacheSize,
		readOnly:                false,
		fileMode:                DefaultFileMode,
	}
}

func (opt *Options) SetMaxNodeSize(maxNodeSize int) *Options {
	opt.maxNodeSize = maxNodeSize
	return opt
}

func (opt *Options) SetInsertionCountThreshold(insertionCountThreshold int) *Options {
	opt.insertionCountThreshold = insertionCountThreshold
	return opt
}

func (opt *Options) SetMaxActiveSnapshots(maxActiveSnapshots int) *Options {
	opt.maxActiveSnapshots = maxActiveSnapshots
	return opt
}

func (opt *Options) SetReuseSnapshotThreshold(reuseSnapshotThreshold int) *Options {
	opt.reuseSnapshotThreshold = reuseSnapshotThreshold
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

func (opt *Options) SetFileMode(fileMode os.FileMode) *Options {
	opt.fileMode = fileMode
	return opt
}

type path []*innerNode

type node interface {
	insertAt(key []byte, value []byte, ts uint64) (node, node, error)
	get(key []byte) (value []byte, ts uint64, err error)
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
}

type leafNode struct {
	t       *TBtree
	values  []*leafValue
	_maxKey []byte
	_ts     uint64
	maxSize int
	off     int64
}

type nodeRef struct {
	t       *TBtree
	_maxKey []byte
	_ts     uint64
	_size   int
	off     int64
}

type leafValue struct {
	key    []byte
	ts     uint64
	prevTs uint64
	value  []byte
}

func Open(fileName string, opt *Options) (*TBtree, error) {
	if opt == nil ||
		opt.maxNodeSize < MinNodeSize ||
		opt.insertionCountThreshold < 1 ||
		opt.maxActiveSnapshots < 1 ||
		opt.reuseSnapshotThreshold < 0 ||
		opt.cacheSize < MinCacheSize {
		return nil, ErrIllegalArgument
	}

	var flag int

	if opt.readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR | os.O_APPEND
	}

	f, err := os.OpenFile(fileName, flag, opt.fileMode)
	if err != nil {
		return nil, err
	}

	// TODO: read maxNodeSize and other immutable settings from file

	cache, err := cache.NewLRUCache(opt.cacheSize)
	if err != nil {
		return nil, err
	}

	t := &TBtree{
		f:                       f,
		cache:                   cache,
		maxNodeSize:             opt.maxNodeSize,
		insertionCountThreshold: opt.insertionCountThreshold,
		maxActiveSnapshots:      opt.maxActiveSnapshots,
		readOnly:                opt.readOnly,
		snapshots:               make(map[uint64]*Snapshot),
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	var root node

	if stat.Size() == 0 {
		root = &leafNode{t: t, maxSize: opt.maxNodeSize}
	} else {
		bs := make([]byte, 4)
		_, err := f.ReadAt(bs, stat.Size()-4)
		if err != nil {
			return nil, err
		}

		rootSize := int(binary.BigEndian.Uint32(bs))
		rootOffset := stat.Size() - 4 - int64(rootSize)

		root, err = t.readNodeAt(rootOffset)
		if err != nil {
			return nil, err
		}

		t.currentOffset = stat.Size()
	}

	t.root = root

	return t, nil
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

func (t *TBtree) readNodeAt(offset int64) (node, error) {
	_, err := t.f.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, t.maxNodeSize)
	n, err := t.f.Read(buf)
	if err != nil {
		return nil, err
	}
	if n < 4 {
		return nil, ErrReadingRequiredFileContent
	}

	i := 1
	size := int(binary.BigEndian.Uint32(buf[i:]))
	i += 4

	if n < size-i {
		return nil, ErrReadingRequiredFileContent
	}

	switch buf[0] {
	case InnerNodeType:
		return t.readInnerNodeFrom(buf[i:], false, offset)
	case RootInnerNodeType:
		return t.readInnerNodeFrom(buf[i:], true, offset)
	case LeafNodeType:
		return t.readLeafNodeFrom(buf[i:], false, offset)
	case RootLeafNodeType:
		return t.readLeafNodeFrom(buf[i:], true, offset)
	}

	return nil, ErrReadingRequiredFileContent
}

func (t *TBtree) readInnerNodeFrom(buf []byte, asRoot bool, offset int64) (*innerNode, error) {
	i := 0

	childCount := int(binary.BigEndian.Uint32(buf[i:]))
	i += 4

	n := &innerNode{
		t:       t,
		nodes:   make([]node, childCount),
		_maxKey: nil,
		_ts:     0,
		maxSize: t.maxNodeSize,
		off:     offset,
	}

	for c := 0; c < childCount; c++ {
		nref, sz := t.readNodeRefFrom(buf[i:])
		n.nodes[c] = nref
		i += sz

		if bytes.Compare(n._maxKey, nref._maxKey) < 0 {
			n._maxKey = nref._maxKey
		}

		if n._ts < nref._ts {
			n._ts = nref._ts
		}
	}

	return n, nil
}

func (t *TBtree) readNodeRefFrom(buf []byte) (*nodeRef, int) {
	i := 0
	ksize := int(binary.BigEndian.Uint32(buf[i:]))
	i += 4

	key := make([]byte, ksize)
	copy(key, buf[i:i+ksize])
	i += ksize

	ts := binary.BigEndian.Uint64(buf[i:])
	i += 8

	size := int(binary.BigEndian.Uint32(buf[i:]))
	i += 4

	off := int64(binary.BigEndian.Uint64(buf[i:]))
	i += 8

	return &nodeRef{
		t:       t,
		_maxKey: key,
		_ts:     ts,
		_size:   size,
		off:     off,
	}, i
}

func (t *TBtree) readLeafNodeFrom(buf []byte, asRoot bool, offset int64) (*leafNode, error) {
	i := 0

	valueCount := int(binary.BigEndian.Uint32(buf[i:]))
	i += 4

	l := &leafNode{
		t:       t,
		values:  make([]*leafValue, valueCount),
		_maxKey: nil,
		_ts:     0,
		maxSize: t.maxNodeSize,
		off:     offset,
	}

	for c := 0; c < valueCount; c++ {
		ksize := int(binary.BigEndian.Uint32(buf[i:]))
		i += 4

		key := make([]byte, ksize)
		copy(key, buf[i:i+ksize])
		i += ksize

		vsize := int(binary.BigEndian.Uint32(buf[i:]))
		i += 4

		value := make([]byte, vsize)
		copy(value, buf[i:i+vsize])
		i += vsize

		ts := binary.BigEndian.Uint64(buf[i:])
		i += 8

		prevTs := binary.BigEndian.Uint64(buf[i:])
		i += 8

		leafValue := &leafValue{
			key:    key,
			value:  value,
			ts:     ts,
			prevTs: prevTs,
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

func (t *TBtree) Flush() (int64, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return 0, ErrAlreadyClosed
	}

	return t.flushTree()
}

func (t *TBtree) flushTree() (int64, error) {
	if t.insertionCount == 0 {
		return 0, nil
	}

	snapshot := t.newSnapshot(0, t.root)

	wopts := &WriteOpts{
		OnlyMutated: true,
		BaseOffset:  t.currentOffset,
		commitLog:   true,
	}

	w := bufio.NewWriter(t.f)

	n, err := snapshot.WriteTo(w, wopts)
	if err != nil {
		return 0, err
	}

	err = w.Flush()
	if err != nil {
		return 0, err
	}

	t.insertionCount = 0
	t.currentOffset += n

	t.root = &nodeRef{
		t:       t,
		_maxKey: t.root.maxKey(),
		_ts:     t.root.ts(),
		_size:   t.root.size(),
		off:     t.root.offset(),
	}

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

	err = t.f.Close()
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
			}

			t.root = newRoot
		}
	}

	t.insertionCount++

	if t.insertionCount == t.insertionCountThreshold {
		_, err := t.flushTree()
		return err
	}

	return nil
}

func (t *TBtree) Snapshot() (*Snapshot, error) {
	return t.snapshot(true)
}

func (t *TBtree) FreshSnapshot() (*Snapshot, error) {
	return t.snapshot(false)
}

func (t *TBtree) snapshot(attemptReuse bool) (*Snapshot, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.closed {
		return nil, ErrAlreadyClosed
	}

	if len(t.snapshots) == t.maxActiveSnapshots {
		return nil, ErrorMaxActiveSnapshotLimitReached
	}

	if !attemptReuse || t.lastSnapshotRoot == nil ||
		t.root.ts()-t.lastSnapshotRoot.ts() >= uint64(t.reuseSnapshotThreshold) {
		t.lastSnapshotRoot = t.root
	}

	t.maxSnapshotID++

	snapshot := t.newSnapshot(t.maxSnapshotID, t.lastSnapshotRoot)

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

	if t.lastSnapshotRoot != nil && t.lastSnapshotRoot.ts() == snapshot.Ts() {
		t.lastSnapshotRoot = nil
	}

	return nil
}

func (n *innerNode) insertAt(key []byte, value []byte, ts uint64) (n1 node, n2 node, err error) {
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
	return n.off == 0
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
	i, found := l.indexOf(key)

	if found {
		newLeaf := &leafNode{
			t:       l.t,
			values:  make([]*leafValue, len(l.values)),
			_maxKey: l._maxKey,
			_ts:     ts,
			maxSize: l.maxSize,
		}

		copy(newLeaf.values[:i], l.values[:i])

		newLeaf.values[i] = &leafValue{
			key:    key,
			ts:     ts,
			prevTs: l.values[i].ts,
			value:  value,
		}

		if i+1 < len(newLeaf.values) {
			copy(newLeaf.values[i+1:], l.values[i+1:])
		}

		return newLeaf, nil, nil
	}

	lv := &leafValue{
		key:    key,
		ts:     ts,
		prevTs: 0,
		value:  value,
	}

	maxKey := l._maxKey
	if bytes.Compare(maxKey, key) < 0 {
		maxKey = key
	}

	newLeaf := &leafNode{
		t:       l.t,
		values:  make([]*leafValue, len(l.values)+1),
		_maxKey: maxKey,
		_ts:     ts,
		maxSize: l.maxSize,
	}

	copy(newLeaf.values[:i], l.values[:i])

	newLeaf.values[i] = lv

	if i+1 < len(newLeaf.values) {
		copy(newLeaf.values[i+1:], l.values[i:])
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

	size += 4 // kv count

	for _, kv := range l.values {
		size += 4             // Key length
		size += len(kv.key)   // Key
		size += 4             // Value length
		size += len(kv.value) // Value
		size += 8             // Ts
		size += 8             // Prev Ts
	}

	return size
}

func (l *leafNode) mutated() bool {
	return l.off == 0
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
		_maxKey: l._maxKey,
		maxSize: l.maxSize,
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
