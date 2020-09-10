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
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/codenotary/immudb/pkg/api/schema"

	"github.com/codenotary/immudb/pkg/logger"

	"github.com/codenotary/immudb/pkg/api"

	"github.com/codenotary/immudb/pkg/ring"
	"github.com/codenotary/merkletree"

	"github.com/dgraph-io/badger/v2"
)

const tsPrefix = byte(0)

const bitReferenceEntry = byte(1)
const bitTreeEntry = byte(255)

const lastFlushedMetaKey = "IMMUDB.METADATA.LAST_FLUSHED_LEAF"

func isReservedKey(key []byte) bool {
	return len(key) > 0 && (key[0] == tsPrefix || bytes.Equal(key, []byte(lastFlushedMetaKey)))
}

func treeKey(layer uint8, index uint64) []byte {
	k := make([]byte, 1+1+8)
	k[0] = tsPrefix
	k[1] = layer
	binary.BigEndian.PutUint64(k[2:], index)
	return k
}

func decodeTreeKey(k []byte) (layer uint8, index uint64) {
	layer = k[1]
	index = binary.BigEndian.Uint64(k[2:])
	return
}

// refTreeKey appends a key of a badger value to an hash
func refTreeKey(hash [sha256.Size]byte, reference []byte) []byte {
	kl := len(reference)
	c := make([]byte, sha256.Size+kl)
	copy(c[:sha256.Size], hash[:])
	copy(c[sha256.Size:], reference[:])
	return c
}

// refTreeKey split a value of a badger item in an the hash array and slice reference key
func decodeRefTreeKey(rtk []byte) ([sha256.Size]byte, []byte, error) {
	lrtk := len(rtk)

	if lrtk < sha256.Size {
		// this should not happen
		return [sha256.Size]byte{}, nil, ErrInconsistentState
	}
	hash := make([]byte, sha256.Size)
	reference := make([]byte, lrtk-sha256.Size)
	copy(hash, rtk[:sha256.Size])
	copy(reference, rtk[sha256.Size:][:])

	var hArray [sha256.Size]byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&hash))
	hArray = *(*[sha256.Size]byte)(unsafe.Pointer(hdr.Data))
	if lrtk == sha256.Size {
		return hArray, nil, ErrObsoleteDataFormat
	}
	return hArray, reference, nil
}

func wrapValueWithTS(v []byte, ts uint64) []byte {
	tsv := make([]byte, len(v)+8)
	binary.BigEndian.PutUint64(tsv, ts)
	copy(tsv[8:], v)
	return tsv
}

func unwrapValueWithTS(tsv []byte) ([]byte, uint64) {
	v := make([]byte, len(tsv)-8)
	ts := binary.BigEndian.Uint64(tsv[:8])
	copy(v, tsv[8:])
	return v, ts
}

func treeLayerWidth(layer uint8, txn *badger.Txn) uint64 {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()

	maxKey := []byte{tsPrefix, layer, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	for it.Seek(maxKey); it.ValidForPrefix(maxKey[:2]); it.Next() {
		return binary.BigEndian.Uint64(it.Item().Key()[2:]) + 1
	}
	return 0
}

type treeStoreEntry struct {
	ts uint64
	h  *[sha256.Size]byte
	r  *[]byte
}

func (t treeStoreEntry) Index() uint64 {
	return t.ts - 1
}

func (t treeStoreEntry) HashCopy() []byte {
	return append([]byte{}, t.h[:]...)
}

type treeStore struct {
	// A 64-bit integer must be at the top for memory alignment
	ts           uint64 // badger timestamp
	w            uint64 // width of computed tree
	c            chan *treeStoreEntry
	quit         chan struct{}
	lastFlushed  uint64
	flushLeaves  bool
	flushOnClose bool
	db           *badger.DB
	log          logger.Logger
	caches       [256]ring.Buffer
	rcache       ring.Buffer
	cPos         [256]uint64
	cSize        uint64
	sync.RWMutex
	closeOnce sync.Once
}

func newTreeStore(db *badger.DB, cacheSize uint64, flushLeaves bool, log logger.Logger) (*treeStore, error) {

	t := &treeStore{
		db:          db,
		log:         log,
		c:           make(chan *treeStoreEntry, cacheSize),
		quit:        make(chan struct{}),
		caches:      [256]ring.Buffer{},
		flushLeaves: flushLeaves,
		cPos:        [256]uint64{},
		cSize:       cacheSize,
	}

	t.makeCaches()

	// load tree state
	err := t.loadTreeState()
	if err != nil {
		return nil, err
	}

	go t.worker()

	t.log.Debugf("Tree of width %d ready with root %x", t.w, merkletree.Root(t))

	return t, nil
}

func (t *treeStore) loadTreeState() error {
	return t.db.View(func(txn *badger.Txn) error {
		for l := 0; l < 256; l++ {
			w := treeLayerWidth(uint8(l), txn)
			if w == 0 {
				break
			}
			t.cPos[l] = w
		}
		t.w = t.cPos[0]
		t.ts = t.w

		i, err := txn.Get([]byte(lastFlushedMetaKey))

		if err == nil {
			bs, err := i.ValueCopy(nil)
			if err != nil {
				return err
			}
			t.lastFlushed = binary.BigEndian.Uint64(bs)
			return nil
		}

		if err == badger.ErrKeyNotFound {
			t.lastFlushed = 0
			return nil
		}

		return err
	})
}

func (t *treeStore) makeCaches() {
	size := t.cSize + 2
	if size < 64 {
		size = 64
	}
	for i := 0; i < 256; i++ {
		t.caches[i] = ring.NewRingBuffer(size)
		if size/2 > 64 {
			size /= 2
		} else {
			size = 64
		}
	}
	t.rcache = ring.NewRingBuffer(t.cSize + 2)
}

// Close closes a treeStore. All pending items will be processed and flushed.
// Calling treeStore.Close() multiple times would still only close the treeStore once.
func (t *treeStore) Close() {
	t.close(true)
}

func (t *treeStore) close(flushPending bool) {
	t.closeOnce.Do(func() {
		if t.quit != nil {
			t.flushOnClose = flushPending
			close(t.c)
			<-t.quit
			t.quit = nil
			t.log.Debugf("Tree of width %d closed with root %x", t.w, merkletree.Root(t))
		}
	})
}

// WaitUntil waits until the given _index_ has been added into the merkletree.
// If the given _index_ cannot be reached, it will never return.
// It's thread-safe.
func (t *treeStore) WaitUntil(index uint64) {
	for {
		t.RLock()
		if t.w >= index+1 {
			t.RUnlock()
			return
		}
		t.RUnlock()
		time.Sleep(time.Microsecond)
	}
}

// LastIndex returns the index of last tree commitment.
// It's thread-safe.
func (t *treeStore) LastIndex() uint64 {
	return atomic.LoadUint64(&t.ts) - 1
}

// NewEntry acquires a lease for a new entry and returns it. The entry must be used with Commit() or Discard().
// It's thread-safe.
func (t *treeStore) NewEntry(key []byte, value []byte) *treeStoreEntry {
	ts := atomic.AddUint64(&t.ts, 1)
	h := api.Digest(ts-1, key, value)
	return &treeStoreEntry{
		ts: ts,
		h:  &h,
		r:  &key,
	}
}

// NewBatch is similar to NewEntry but accept a slice of key-value pairs.
// It's thread-safe.
func (t *treeStore) NewBatch(kvPairs *schema.KVList) []*treeStoreEntry {
	size := uint64(len(kvPairs.KVs))
	batch := make([]*treeStoreEntry, 0, size)
	lease := atomic.AddUint64(&t.ts, size)
	for i, kv := range kvPairs.KVs {
		ts := lease - size + uint64(i) + 1
		h := api.Digest(ts-1, kv.Key, kv.Value)
		batch = append(batch, &treeStoreEntry{ts, &h, &kv.Key})
	}
	return batch
}

// Commit enqueues the given entry to be included in the merkletree.
// The value of _entry_ might change once Discard() or Commit() is called,
// so it must not be used later.
// It's thread-safe. Commit will fail if called after Close().
func (t *treeStore) Commit(entry *treeStoreEntry) {
	t.c <- entry
}

// Discard enqueues the given entry to be included in the tree as discarded item.
// The value of _entry_ might change once Discard() or Commit() is called,
// so it must not be used later.
// It's thread-safe. Discard will fail if called after Close().
func (t *treeStore) Discard(entry *treeStoreEntry) {
	h := api.Digest(entry.ts, []byte{}, []byte{})
	entry.h = &h
	t.c <- entry
}

func (t *treeStore) worker() {
	//Priority Queue
	pq := make(treeStorePQ, 0, t.cSize)
	for item := range t.c {
		heap.Push(&pq, item)

		t.Lock()
		for min := pq.Min(); min == t.w+1; min = pq.Min() {

			item := heap.Pop(&pq).(*treeStoreEntry)

			// insertion order index reference creation
			c := refTreeKey(*item.h, *item.r)
			// insertion order index cache save
			t.rcache.Set(item.ts-1, c)

			merkletree.AppendHash(t, item.h)
			if t.w%2 == 0 && (t.w-t.lastFlushed) >= t.cSize/2 {
				t.flush()
			}
		}
		t.Unlock()
	}

	if t.w > 0 && t.flushOnClose {
		t.Lock()
		t.flush()
		t.Unlock()
	}
	t.quit <- struct{}{}
}

// flush should be only called when the tree is in a consistent state and _t_ is locked.
// It always flushes the last portion (ie. items not yet flushed) of buffers in batch,
// in case of failure previous stored state will be preserved and cache indexes will be not advanced.
func (t *treeStore) flush() {
	t.log.Infof("Flushing tree caches at index %d", t.w-1)
	var cancel bool
	var emptyCaches bool = true
	wb := t.db.NewWriteBatchAt(t.w)
	defer func() {
		if cancel {
			wb.Cancel()
			return
		}
		advance := func() {
			// advance cache indexes iff flushing has succeeded
			for l, c := range t.caches {
				t.cPos[l] = c.Tail()
			}
			t.lastFlushed = t.w
		}
		//workaround possible badger bug
		//Commit cannot be called with managedDB=true. Use CommitAt.
		if !emptyCaches {
			err := wb.Flush()
			if err != nil {
				t.log.Errorf("Tree flush error: %s", err)
				return
			}
			advance()
		}
	}()
	for l, c := range t.caches {
		tail := c.Tail()
		if tail == 0 {
			continue
		}
		emptyCaches = false
		// fmt.Printf("Flushing [l=%d, head=%d, tail=%d] from %d to (%d-1)\n", l, c.Head(), c.Tail(), t.cPos[l], tail)
		if !t.flushLeaves && l == 0 {
			continue
		}
		for i := t.cPos[l]; i < tail; i++ {
			if h := c.Get(i); h != nil {
				var value []byte
				value = h.(*[sha256.Size]byte)[:]
				// retrieving insertion order index reference from buffer ring
				if l == 0 {
					value = t.rcache.Get(i).([]byte)
				}
				// fmt.Printf("Storing [l=%d, i=%d]\n", l, i)
				entry := badger.Entry{
					Key:      treeKey(uint8(l), i),
					Value:    value,
					UserMeta: bitTreeEntry,
				}
				// it's safe to discard, only non-frozen nodes could be overwritten
				entry.WithDiscard()

				if err := wb.SetEntry(&entry); err != nil {
					// fixme(leogr): that should never happen, use panic() ?
					t.log.Errorf("Cannot flush tree item (l=%d, i=%d): %s", err)
					t.log.Warningf("Tree flush canceled")
					cancel = true
					return
				}
			}
		}
	}

	if !cancel && !emptyCaches {
		sw := make([]byte, 8)
		binary.BigEndian.PutUint64(sw, t.w)

		entry := badger.Entry{
			Key:   []byte(lastFlushedMetaKey),
			Value: sw,
		}
		// only latest value is needed to replay entries
		entry.WithDiscard()

		if err := wb.SetEntry(&entry); err != nil {
			t.log.Errorf("Cannot flush tree item: %v", err)
			t.log.Warningf("Tree flush canceled")
			cancel = true
			return
		}
	}
}

func (t *treeStore) Width() uint64 {
	return t.w
}

func (t *treeStore) Set(layer uint8, index uint64, value [sha256.Size]byte) {
	t.caches[layer].Set(index, &value)
	// invalidate from `index+1` (included), if needed
	// note that `index` is zero-indexed and `t.cPos` is not
	if index < t.cPos[layer] {
		t.cPos[layer] = index
	}
	if layer == 0 && t.w <= index {
		t.w = index + 1
	}
}

func (t *treeStore) Get(layer uint8, index uint64) *[sha256.Size]byte {

	if v := t.caches[layer].Get(index); v != nil {
		return v.(*[sha256.Size]byte)
	}
	var ret [sha256.Size]byte
	if err := t.db.View(func(txn *badger.Txn) error {
		var temp []byte
		// fmt.Printf("CACHE MISS (ts=%d, w=%d, d=%d): [%d,%d]\n", t.ts, t.w, merkletree.Depth(t), layer, index)
		item, err := txn.Get(treeKey(layer, index))
		if err != nil {
			return err
		}
		// in layer 0 are stored leaves with value composed by hash concatenated with insertion order index key
		if layer == 0 {
			if temp, err = item.ValueCopy(nil); err != nil {
				return err
			}
			if ret, _, err = decodeRefTreeKey(temp); err != nil {
				// here ErrObsoleteDataFormat is suppressed in order to reduce breaking changes
				return nil
			}
		} else {
			// if layer > 0, value of an element is ever an array of 32 bytes
			if _, err = item.ValueCopy(ret[:]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil
	}

	return &ret
}
