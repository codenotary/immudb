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
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/dgraph-io/badger/v2/pb"

	"github.com/codenotary/immudb/pkg/api/schema"

	"github.com/codenotary/merkletree"

	"github.com/stretchr/testify/assert"
)

var root64th = [sha256.Size]byte{0xb1, 0xbe, 0x73, 0xef, 0x38, 0x8e, 0x7e, 0xd3, 0x79, 0x71, 0x7, 0x26, 0xd1, 0x19, 0xa5, 0x35, 0xb8, 0x67, 0x24, 0x12, 0x48, 0x25, 0x7a, 0x7e, 0x2e, 0x34, 0x32, 0x29, 0x65, 0x60, 0xdf, 0xf9}

func makeStore() (*Store, func()) {
	return makeStoreAt(tmpDir())
}

func makeStoreAt(dir string) (*Store, func()) {
	slog := logger.NewSimpleLoggerWithLevel("bm(immudb)", os.Stderr, logger.LogDebug)
	opts, badgerOpts := DefaultOptions(dir, slog)

	st, err := Open(opts, badgerOpts)
	if err != nil {
		log.Fatal(err)
	}

	return st, func() {
		if err := st.Close(); err != nil {
			log.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}
	}
}

func tmpDir() string {
	dir, err := ioutil.TempDir("", "immu")
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func TestStore(t *testing.T) {
	st, closer := makeStore()

	assert.True(t, st.HealthCheck())

	assert.Equal(t, uint64(0), st.CountAll())

	count, err := st.Count(schema.KeyPrefix{Prefix: nil})
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count.GetCount())

	defer closer()

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		kv := schema.KeyValue{
			Key:   key,
			Value: key,
		}
		index, err := st.Set(kv)
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, index.Index, "n=%d", n)
	}

	assert.True(t, uint64(64) <= st.CountAll())

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		item, err := st.Get(schema.Key{Key: key})
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, item.Index, "n=%d", n)
		assert.Equal(t, key, item.Value, "n=%d", n)
		assert.Equal(t, key, item.Key, "n=%d", n)

		count, err = st.Count(schema.KeyPrefix{Prefix: key})
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count.GetCount())
	}

	st.tree.WaitUntil(64)

	st.FlushToDisk()

	assert.True(t, st.tree.w == st.tree.lastFlushed)

	assert.Equal(t, root64th, merkletree.Root(st.tree))

	st.tree.Close()
	assert.Equal(t, root64th, merkletree.Root(st.tree))

	st.tree.makeCaches() // with empty cache, next call should fetch from DB
	assert.Equal(t, root64th, merkletree.Root(st.tree))

	assert.True(t, st.HealthCheck())
}

func TestStoreMissingEntriesReplay(t *testing.T) {
	dbDir := tmpDir()

	st, _ := makeStoreAt(dbDir)

	assert.True(t, st.HealthCheck())

	assert.Equal(t, uint64(0), st.CountAll())

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		kv := schema.KeyValue{
			Key:   key,
			Value: key,
		}
		index, err := st.Set(kv)
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, index.Index, "n=%d", n)
	}

	st.tree.close(false)

	st.Close()

	st, closer := makeStoreAt(dbDir)
	defer closer()

	st.tree.WaitUntil(64)

	assert.Equal(t, root64th, merkletree.Root(st.tree))

	st.FlushToDisk()

	assert.True(t, st.tree.w == st.tree.lastFlushed)

	assert.Equal(t, root64th, merkletree.Root(st.tree))

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		item, err := st.Get(schema.Key{Key: key})
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, item.Index, "n=%d", n)
		assert.Equal(t, key, item.Value, "n=%d", n)
		assert.Equal(t, key, item.Key, "n=%d", n)
	}
}

func TestStoreAsyncCommit(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		kv := schema.KeyValue{
			Key:   key,
			Value: key,
		}
		index, err := st.Set(kv, WithAsyncCommit(true))
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, index.Index, "n=%d", n)
	}

	st.Wait()

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		item, err := st.Get(schema.Key{Key: key})
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, item.Index, "n=%d", n)
		assert.Equal(t, key, item.Value, "n=%d", n)
		assert.Equal(t, key, item.Key, "n=%d", n)
	}

	st.tree.WaitUntil(64)
	assert.Equal(t, root64th, merkletree.Root(st.tree))

	st.tree.Close()
	assert.Equal(t, root64th, merkletree.Root(st.tree))

	st.tree.makeCaches() // with empty cache, next call should fetch from DB
	assert.Equal(t, root64th, merkletree.Root(st.tree))
}

func TestDump(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	for n := uint64(0); n <= 4; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		kv := schema.KeyValue{
			Key:   key,
			Value: largeItem,
		}
		index, err := st.Set(kv)
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, index.Index, "n=%d", n)
	}

	kv2 := schema.KeyValue{
		Key:   []byte(strconv.FormatUint(1, 10)),
		Value: []byte(`secondval`),
	}
	index, _ := st.Set(kv2)

	st.tree.WaitUntil(5)

	assert.Equal(t, uint64(5), index.Index)

	done := make(chan bool)
	kvChan := make(chan *pb.KVList, 1)

	var lists []*pb.KVList
	retrieveLists := func() {
		for {
			list, more := <-kvChan
			if more {
				lists = append(lists, list)
				fmt.Println("send kv:" + strconv.Itoa(len(lists)))
				fmt.Println("len list kv:" + strconv.Itoa(len(list.Kv)))
			} else {
				fmt.Println("received all lists")
				done <- true
				return
			}
		}
	}

	go retrieveLists()
	err := st.Dump(kvChan)
	<-done

	assert.NoError(t, err)
	assert.Equal(t, 1, len(lists))
	assert.Equal(t, 18, len(lists[0].Kv), "All keys was retrieved")
}

func TestLargeDump(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	done := make(chan bool)
	for n := uint64(0); n <= 6500; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		kv := schema.KeyValue{
			Key:   key,
			Value: largeItem,
		}
		index, err := st.Set(kv)
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, index.Index, "n=%d", n)
	}

	kv2 := schema.KeyValue{
		Key:   []byte(strconv.FormatUint(13, 10)),
		Value: []byte(`secondval`),
	}
	st.Set(kv2)

	kv3 := schema.KeyValue{
		Key:   []byte(strconv.FormatUint(13, 10)),
		Value: []byte(`lastversion`),
	}
	index, _ := st.Set(kv3)

	st.tree.WaitUntil(6502)

	assert.Equal(t, uint64(6502), index.Index)

	kvChan := make(chan *pb.KVList, 1)

	var lists []*pb.KVList
	retrieveLists := func() {
		for {
			list, more := <-kvChan
			if more {
				lists = append(lists, list)
				fmt.Println("send kv:" + strconv.Itoa(len(lists)))
				fmt.Println("len list kv:" + strconv.Itoa(len(list.Kv)))
			} else {
				fmt.Println("received all lists")
				done <- true
				return
			}
		}
	}

	go retrieveLists()
	err := st.Dump(kvChan)
	<-done

	assert.NoError(t, err)
}

func TestRestore(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	done := make(chan bool)
	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		kv := schema.KeyValue{
			Key:   key,
			Value: key,
		}
		st.Set(kv)
	}

	st.tree.WaitUntil(64)

	kvChan := make(chan *pb.KVList, 1)

	var lists []*pb.KVList
	retrieveLists := func() {
		for {
			list, more := <-kvChan
			if more {
				lists = append(lists, list)
			} else {
				done <- true
				return
			}
		}
	}
	go retrieveLists()
	st.Dump(kvChan)
	<-done
	closer()

	st2, closer2 := makeStore()
	defer closer2()

	kvChan2 := make(chan *pb.KVList)
	done2 := make(chan bool)

	sendLists := func() {
		for _, list := range lists {
			kvChan2 <- list
		}
		done2 <- true
		return
	}
	go sendLists()

	i, err := st2.Restore(kvChan2)
	<-done2
	assert.NoError(t, err)
	assert.Equal(t, uint64(65), i)
	st2.tree.WaitUntil(64)
	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		item, err := st2.Get(schema.Key{Key: key})

		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, item.Index, "n=%d", n)
		assert.Equal(t, key, item.Value, "n=%d", n)
		assert.Equal(t, key, item.Key, "n=%d", n)
		itemByIndex, err := st2.ByIndex(schema.Index{Index: n})
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, key, itemByIndex.Value, "n=%d", n)
		assert.Equal(t, key, itemByIndex.Key, "n=%d", n)

	}

	st2.tree.WaitUntil(64)
	assert.Equal(t, root64th, merkletree.Root(st2.tree))

	st2.tree.Close()
	assert.Equal(t, root64th, merkletree.Root(st2.tree))

	st2.tree.makeCaches() // with empty cache, next call should fetch from DB
	assert.Equal(t, root64th, merkletree.Root(st2.tree))
}

func TestRestoreHistoryCheck(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	done := make(chan bool)
	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		kv := schema.KeyValue{
			Key:   key,
			Value: key,
		}
		st.Set(kv)
	}

	kv2 := schema.KeyValue{
		Key:   []byte(strconv.FormatUint(13, 10)),
		Value: []byte(`secondval`),
	}
	st.Set(kv2)

	st.tree.WaitUntil(65)

	kvChan := make(chan *pb.KVList, 1)

	var lists []*pb.KVList
	retrieveLists := func() {
		for {
			list, more := <-kvChan
			if more {
				lists = append(lists, list)
			} else {
				done <- true
				return
			}
		}
	}
	go retrieveLists()
	st.Dump(kvChan)
	<-done
	closer()

	st2, closer2 := makeStore()
	defer closer2()

	kvChan2 := make(chan *pb.KVList)
	done2 := make(chan bool)

	sendLists := func() {
		for _, list := range lists {
			kvChan2 <- list
		}
		done2 <- true
		return
	}
	go sendLists()

	i, err := st2.Restore(kvChan2)
	<-done2
	assert.NoError(t, err)
	assert.Equal(t, uint64(66), i)

	st2.tree.WaitUntil(64)
	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		item, err := st2.Get(schema.Key{Key: key})
		i := n
		v := key
		k := key
		if n == 13 {
			v = []byte(`secondval`) // history check
			i = uint64(65)          // history check
		}
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, i, item.Index, "n=%d", n)
		assert.Equal(t, v, item.Value, "n=%d", n)
		assert.Equal(t, k, item.Key, "n=%d", n)
	}

	options := &schema.HistoryOptions{
		Key: []byte(strconv.FormatUint(13, 10)),
	}

	il, err := st2.History(options)
	assert.NoError(t, err)
	assert.Equal(t, il.Items[0].Value, []byte(`secondval`))
	assert.Equal(t, il.Items[1].Value, []byte(strconv.FormatUint(13, 10)))

}

func TestStore_HistoryPagination(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`key`), Value: []byte(`val1`)})
	st.Set(schema.KeyValue{Key: []byte(`key`), Value: []byte(`val2`)})
	st.Set(schema.KeyValue{Key: []byte(`key`), Value: []byte(`val3`)})
	st.Set(schema.KeyValue{Key: []byte(`key`), Value: []byte(`val4`)})
	st.Set(schema.KeyValue{Key: []byte(`key`), Value: []byte(`val5`)})

	hOpts1 := &schema.HistoryOptions{
		Key:   []byte(`key`),
		Limit: 2,
	}
	list1, err := st.History(hOpts1)
	assert.NoError(t, err)
	assert.Len(t, list1.Items, 2)
	assert.Equal(t, list1.Items[0].Value, []byte(`val5`))
	assert.Equal(t, list1.Items[1].Value, []byte(`val4`))

	hOpts2 := &schema.HistoryOptions{
		Key:    []byte(`key`),
		Offset: list1.Items[len(list1.Items)-1].Index,
		Limit:  2,
	}
	list2, err := st.History(hOpts2)
	assert.NoError(t, err)
	assert.Len(t, list2.Items, 2)
	assert.Equal(t, list2.Items[0].Value, []byte(`val3`))
	assert.Equal(t, list2.Items[1].Value, []byte(`val2`))

	hOpts3 := &schema.HistoryOptions{
		Key:    []byte(`key`),
		Offset: list2.Items[len(list2.Items)-1].Index,
		Limit:  2,
	}
	list3, err := st.History(hOpts3)
	assert.NoError(t, err)
	assert.Len(t, list3.Items, 1)
	assert.Equal(t, list3.Items[0].Value, []byte(`val1`))

}

func TestStore_HistoryReversePagination(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`key`), Value: []byte(`val1`)})
	st.Set(schema.KeyValue{Key: []byte(`key`), Value: []byte(`val2`)})
	st.Set(schema.KeyValue{Key: []byte(`key`), Value: []byte(`val3`)})
	st.Set(schema.KeyValue{Key: []byte(`key`), Value: []byte(`val4`)})
	st.Set(schema.KeyValue{Key: []byte(`key`), Value: []byte(`val5`)})

	hOpts1 := &schema.HistoryOptions{
		Key:     []byte(`key`),
		Limit:   2,
		Reverse: true,
	}
	list1, err := st.History(hOpts1)
	assert.NoError(t, err)
	assert.Len(t, list1.Items, 2)
	assert.Equal(t, list1.Items[0].Value, []byte(`val1`))
	assert.Equal(t, list1.Items[1].Value, []byte(`val2`))

	hOpts2 := &schema.HistoryOptions{
		Key:     []byte(`key`),
		Offset:  list1.Items[len(list1.Items)-1].Index,
		Limit:   2,
		Reverse: true,
	}
	list2, err := st.History(hOpts2)
	assert.NoError(t, err)
	assert.Len(t, list2.Items, 2)
	assert.Equal(t, list2.Items[0].Value, []byte(`val3`))
	assert.Equal(t, list2.Items[1].Value, []byte(`val4`))

	hOpts3 := &schema.HistoryOptions{
		Key:     []byte(`key`),
		Offset:  list2.Items[len(list2.Items)-1].Index,
		Limit:   2,
		Reverse: true,
	}
	list3, err := st.History(hOpts3)
	assert.NoError(t, err)
	assert.Len(t, list3.Items, 1)
	assert.Equal(t, list3.Items[0].Value, []byte(`val5`))

}

func TestStore_HistoryInvalidKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	hOpts1 := &schema.HistoryOptions{
		Key: []byte{tsPrefix},
	}
	_, err := st.History(hOpts1)
	assert.Error(t, err, ErrInvalidKey)
}

func TestInsertionOrderIndex(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	key1 := []byte(`myFirstElementKey`)
	val1 := []byte(`firstValue`)
	key2 := []byte(`mySecondElementKey`)
	val2 := []byte(`secondValue`)
	key3 := []byte(`myThirdElementKey`)
	val3 := []byte(`thirdValue`)

	index1, _ := st.Set(schema.KeyValue{Key: key1, Value: val1})
	index2, _ := st.Set(schema.KeyValue{Key: key2, Value: val2})
	index3, _ := st.Set(schema.KeyValue{Key: key3, Value: val3})

	st.tree.WaitUntil(2)
	item1, err := st.ByIndex(*index1)
	assert.NoError(t, err)
	assert.Equal(t, item1.Index, index1.Index)
	assert.Equal(t, key1, item1.Key)
	assert.Equal(t, val1, item1.Value)

	item2, err := st.ByIndex(*index2)
	assert.NoError(t, err)
	assert.Equal(t, item2.Index, item2.Index)
	assert.Equal(t, key2, item2.Key)
	assert.Equal(t, val2, item2.Value)

	item3, err := st.ByIndex(*index3)
	assert.NoError(t, err)
	assert.Equal(t, item3.Index, item3.Index)
	assert.Equal(t, key3, item3.Key)
	assert.Equal(t, val3, item3.Value)

	//flushing and empty cache
	st.tree.Close()
	st.tree.makeCaches()

	item1, err = st.ByIndex(*index1)
	assert.NoError(t, err)
	assert.Equal(t, item1.Index, index1.Index)
	assert.Equal(t, key1, item1.Key)
	assert.Equal(t, val1, item1.Value)

	item2, err = st.ByIndex(*index2)
	assert.NoError(t, err)
	assert.Equal(t, item2.Index, item2.Index)
	assert.Equal(t, key2, item2.Key)
	assert.Equal(t, val2, item2.Value)

	item3, err = st.ByIndex(*index3)
	assert.NoError(t, err)
	assert.Equal(t, item3.Index, item3.Index)
	assert.Equal(t, key3, item3.Key)
	assert.Equal(t, val3, item3.Value)

	_, err = st.ByIndex(schema.Index{
		Index: 4,
	})
	assert.Error(t, err, ErrIndexNotFound)
}

func TestInsertionOrderIndexTamperGuard(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	key1 := []byte(`myFirstElementKey`)
	val1 := []byte(`firstValue`)
	key2 := []byte(`mySecondElementKey`)
	val2 := []byte(`secondValue`)

	_, _ = st.Set(schema.KeyValue{Key: key1, Value: val1})
	index2, _ := st.Set(schema.KeyValue{Key: key2, Value: val2})
	st.tree.Close()
	st.tree.makeCaches()

	// TAMPER: here we try to modify insertion sorted index of element 2 making him pointing to element 1
	txn := st.tree.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	// retrieving the leaf of element 2
	leaf, _ := txn.Get(treeKey(0, index2.Index))
	ts := leaf.Version()
	leafkey := leaf.KeyCopy(nil)
	refKey, _ := leaf.ValueCopy(nil)
	// extract the hash ef element 2
	hash, _, _ := decodeRefTreeKey(refKey)
	// creation of a fake reference to element 1
	fakeReference := refTreeKey(hash, key1)
	// override the leaf
	_ = txn.Set(leafkey, fakeReference)
	_ = txn.CommitAt(ts, nil)

	_, err := st.ByIndex(*index2)
	assert.Errorf(t, err, fmt.Sprintf("insertion order index %d was tampered", ts))
}

func TestInsertionOrderIndexMix(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	key1 := []byte(`myFirstElementKey`)
	val1 := []byte(`firstValue`)
	key2 := []byte(`mySecondElementKey`)
	val2 := []byte(`secondValue`)
	key3 := []byte(`myThirdElementKey`)
	val3 := []byte(`thirdValue`)

	val4 := []byte(`444`)
	val5 := []byte(`555`)
	val6 := []byte(`666`)

	firstTag := []byte(`firstTag`)
	secondTag := []byte(`secondTag`)

	index1, _ := st.Set(schema.KeyValue{Key: key1, Value: val1})
	index2, _ := st.Set(schema.KeyValue{Key: key2, Value: val2})
	index3, _ := st.Set(schema.KeyValue{Key: key3, Value: val3})

	index4, _ := st.Set(schema.KeyValue{Key: key3, Value: val4})
	index5, _ := st.Set(schema.KeyValue{Key: key3, Value: val5})
	index6, _ := st.Set(schema.KeyValue{Key: key3, Value: val6})

	ref1 := schema.SafeReferenceOptions{
		Ro: &schema.ReferenceOptions{
			Reference: firstTag,
			Key:       key1,
		},
	}

	proof, err := st.SafeReference(ref1)
	assert.NoError(t, err)

	ref2 := schema.SafeReferenceOptions{
		Ro: &schema.ReferenceOptions{
			Reference: secondTag,
			Key:       key3,
		},
		RootIndex: &schema.Index{
			Index: proof.Index,
		},
	}

	st.SafeReference(ref2)

	st.tree.WaitUntil(7)
	item1, err := st.ByIndex(*index1)
	assert.NoError(t, err)
	assert.Equal(t, item1.Index, index1.Index)
	assert.Equal(t, key1, item1.Key)
	assert.Equal(t, val1, item1.Value)

	item2, err := st.ByIndex(*index2)
	assert.NoError(t, err)
	assert.Equal(t, item2.Index, item2.Index)
	assert.Equal(t, key2, item2.Key)
	assert.Equal(t, val2, item2.Value)

	item3, err := st.ByIndex(*index3)
	assert.NoError(t, err)
	assert.Equal(t, index3.Index, item3.Index)
	assert.Equal(t, key3, item3.Key)
	assert.Equal(t, val3, item3.Value)

	item3v2, err := st.ByIndex(*index4)
	assert.NoError(t, err)
	assert.Equal(t, index4.Index, item3v2.Index)
	assert.Equal(t, key3, item3v2.Key)
	assert.Equal(t, val4, item3v2.Value)
	item3v4, err := st.ByIndex(*index5)
	assert.NoError(t, err)
	assert.Equal(t, index5.Index, item3v4.Index)
	assert.Equal(t, key3, item3v4.Key)
	assert.Equal(t, val5, item3v4.Value)
	item3v5, err := st.ByIndex(*index6)
	assert.NoError(t, err)
	assert.Equal(t, index6.Index, item3v5.Index)
	assert.Equal(t, key3, item3v5.Key)
	assert.Equal(t, val6, item3v5.Value)
	//flushing and empty cache
	st.tree.Close()
	st.tree.makeCaches()

	item1, err = st.ByIndex(*index1)
	assert.NoError(t, err)
	assert.Equal(t, item1.Index, index1.Index)
	assert.Equal(t, key1, item1.Key)
	assert.Equal(t, val1, item1.Value)

	item2, err = st.ByIndex(*index2)
	assert.NoError(t, err)
	assert.Equal(t, item2.Index, item2.Index)
	assert.Equal(t, key2, item2.Key)
	assert.Equal(t, val2, item2.Value)

	item3, err = st.ByIndex(*index3)
	assert.NoError(t, err)
	assert.Equal(t, item3.Index, item3.Index)
	assert.Equal(t, key3, item3.Key)
	assert.Equal(t, val3, item3.Value)

	item3v2, err = st.ByIndex(*index4)
	assert.NoError(t, err)
	assert.Equal(t, index4.Index, item3v2.Index)
	assert.Equal(t, key3, item3v2.Key)
	assert.Equal(t, val4, item3v2.Value)
	item3v4, err = st.ByIndex(*index5)
	assert.NoError(t, err)
	assert.Equal(t, index5.Index, item3v4.Index)
	assert.Equal(t, key3, item3v4.Key)
	assert.Equal(t, val5, item3v4.Value)
	item3v5, err = st.ByIndex(*index6)
	assert.NoError(t, err)
	assert.Equal(t, index6.Index, item3v5.Index)
	assert.Equal(t, key3, item3v5.Key)
	assert.Equal(t, val6, item3v5.Value)

	_, err = st.ByIndex(schema.Index{
		Index: 99,
	})
	assert.Error(t, err, ErrIndexNotFound)
}

func TestGetTree(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	tree := st.GetTree()
	assert.NotNil(t, tree)
	assert.Nil(t, tree.T)

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		kv := schema.KeyValue{
			Key:   key,
			Value: key,
		}
		index, err := st.Set(kv)
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, index.Index, "n=%d", n)
	}

	st.tree.WaitUntil(64)

	tree1 := st.GetTree()
	assert.NotNil(t, tree1)
	assert.Equal(t, 8, len(tree1.T))
	assert.Equal(t, 65, len(tree1.T[0].L))
	root, err := st.CurrentRoot()
	assert.NoError(t, err)
	assert.Equal(t, root.Payload.Root, tree1.T[7].L[0].H)
}

func BenchmarkStoreSet(b *testing.B) {
	st, closer := makeStore()
	defer closer()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		kv := schema.KeyValue{
			Key:   []byte(strconv.FormatUint(uint64(i), 10)),
			Value: []byte{0, 1, 3, 4, 5, 6, 7},
		}
		st.Set(kv)
	}
	b.StopTimer()
}

func TestStore_ZAddWrongKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	i1, _ := st.Set(schema.KeyValue{Key: []byte(`val1`), Value: []byte(`val2`)})

	zaddOpts1 := schema.ZAddOptions{
		Set:   []byte(`set`),
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte{tsPrefix},
		Index: i1,
	}
	_, err := st.ZAdd(zaddOpts1)

	assert.Error(t, err)
	assert.Equal(t, err, ErrInvalidKey)
}

func TestStore_ZAddWrongSet(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	i1, _ := st.Set(schema.KeyValue{Key: []byte(`val1`), Value: []byte(`val2`)})

	zaddOpts1 := schema.ZAddOptions{
		Set:   []byte{tsPrefix},
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`key`),
		Index: i1,
	}
	_, err := st.ZAdd(zaddOpts1)

	assert.Error(t, err)
	assert.Equal(t, err, ErrInvalidSet)
}

var largeItem = []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce non odio sed tellus rutrum suscipit. Aliquam nisl libero, porta in augue at, laoreet fringilla mi. Aliquam metus dolor, tincidunt eget nibh at, tempus malesuada odio. Suspendisse ex nisl, pretium ac lacinia et, efficitur sed ipsum. In porttitor cursus sem, ac aliquet ante tristique nec. Donec interdum nulla enim, ac maximus urna molestie ut. Ut ornare mi et dolor pretium semper. Donec facilisis vitae massa in faucibus. Suspendisse potenti.
Etiam fringilla risus quam, vel ornare ligula vestibulum ac. In tristique ut diam ut dictum. Nulla vitae condimentum ante. Curabitur elementum lacus nibh. Donec vitae tortor porttitor, efficitur felis eu, facilisis libero. Nulla sit amet egestas neque. Nullam eleifend lobortis erat. Donec vehicula erat vitae mi rutrum, vel tempus sem volutpat. Aenean sem urna, dapibus sit amet justo vitae, consequat molestie enim. Maecenas venenatis, risus eget semper faucibus, erat risus feugiat nulla, id tincidunt risus tellus ut elit.
Cras elementum ipsum ullamcorper, blandit magna sit amet, scelerisque lectus. Pellentesque ut egestas libero. Sed aliquet scelerisque tortor, ut consequat mi vehicula id. Aliquam gravida, quam eget pharetra commodo, elit nisi aliquam ante, non egestas ex lectus at lorem. Suspendisse semper, enim eget semper vehicula, sapien ipsum lacinia eros, eget ullamcorper elit nunc sed leo. Nam consequat ligula non mi iaculis, vitae dignissim augue posuere. Nunc varius risus non libero ultricies venenatis eu et ex. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi mattis tristique nunc, pharetra aliquet mauris eleifend vitae.
Interdum et malesuada fames ac ante ipsum primis in faucibus. Vivamus imperdiet, mauris sed ullamcorper suscipit, velit libero bibendum est, ut pretium eros urna sit amet urna. Duis eleifend rutrum odio at vestibulum. Phasellus eget neque tempor, posuere leo ac, dictum lorem. Vestibulum cursus lorem et augue rhoncus scelerisque. Pellentesque maximus libero sed nisl efficitur suscipit. Vivamus at urna volutpat, ultrices arcu sed, faucibus augue. Quisque ut lacinia ligula, eu accumsan tortor. Curabitur elementum, mauris imperdiet faucibus sodales, tellus sapien tincidunt risus, non scelerisque lacus lacus eu risus. Ut auctor quam ipsum, quis fringilla enim interdum vel. Duis consequat vehicula neque. Donec sagittis nunc ac nisi lobortis ornare.
Maecenas sit amet nibh velit. Pellentesque viverra rutrum arcu quis pretium. Quisque volutpat cursus iaculis. Curabitur tempor varius tortor a pharetra. Pellentesque vel odio vel elit congue commodo eu nec mi. Pellentesque molestie non risus vitae feugiat. Proin lobortis leo dolor, vel tincidunt purus fermentum vitae. Aenean eget tortor in turpis maximus ornare vel in nulla. Donec et commodo lorem. Ut gravida porta eros ac vehicula. Aliquam odio ante, tristique ut ipsum id, accumsan consectetur massa.
Maecenas non ullamcorper erat. Pellentesque lacus sem, rhoncus sed accumsan et, aliquam at diam. Praesent eu interdum erat. Integer lacinia, purus ut suscipit congue, enim dolor lobortis libero, ac volutpat ligula risus sed tortor. Nam sollicitudin massa eget varius rhoncus. Donec non magna vitae odio placerat ultrices. Nulla et porta mi, sit amet tincidunt justo. Proin ut iaculis velit. Quisque tempus et lectus vel molestie. Mauris efficitur neque quam, eu gravida augue tempus sodales. Aenean eget fermentum mauris, sed feugiat lorem. Vestibulum sodales ante et sodales congue.
Pellentesque rhoncus risus et tortor tempor euismod eu a dui. Nam consectetur elementum ligula, non bibendum ipsum facilisis sit amet. Sed fringilla justo mauris, at semper nisi dapibus ut. Nulla vulputate massa nibh, a tristique nunc ornare eu. Nunc vitae erat sollicitudin, posuere tellus ac, pharetra ante. Proin suscipit tellus tortor, nec luctus nunc iaculis sed. Donec ut tincidunt velit. Interdum et malesuada fames ac ante ipsum primis in faucibus. Nam semper lorem vitae condimentum pharetra.
Fusce elementum id dui a ullamcorper. Suspendisse vulputate justo augue, vel laoreet justo pretium nec. Quisque commodo nisl et rutrum tempor. Nam semper enim condimentum enim laoreet vulputate. Nullam condimentum, metus ut lacinia sollicitudin, sem tellus tempus metus, ac sollicitudin nulla augue et velit. Morbi non tincidunt mi. Duis ut magna eu dolor varius rutrum. Duis elementum, orci nec sollicitudin tincidunt, sapien sapien aliquam augue, nec viverra mauris est quis erat. Mauris elit dui, aliquet in lacus eget, facilisis tincidunt risus.
Etiam eget lorem augue. Sed placerat tempor dolor, nec commodo mi fermentum at. Sed dapibus vel urna in malesuada. Mauris in porta lorem. Quisque euismod condimentum libero, vitae condimentum quam fermentum eu. Ut vitae tempus risus. Fusce semper eu quam quis aliquet. Donec tempus sapien neque, a luctus velit eleifend sed. Aliquam sit amet pharetra odio. Etiam sagittis cursus rhoncus. Aliquam gravida at urna ut porta. Aenean auctor neque vitae mi porta volutpat. Vestibulum egestas ex at urna hendrerit commodo. Aenean fermentum nec elit quis fringilla.
Etiam mattis, diam at ornare aliquam, lectus sem interdum lectus, eu euismod magna tortor ut ex. Suspendisse vel magna blandit, consequat nisl at, convallis purus. Donec vitae arcu odio. Sed ut tortor lacus. Maecenas vehicula porta facilisis. Suspendisse nec consectetur turpis, id vulputate sapien. Nam vitae urna sodales sem vulputate cursus id id diam. Aenean efficitur orci sed varius porta. Integer eu malesuada elit. Curabitur faucibus tempor imperdiet. Aenean semper finibus augue, nec ullamcorper ante pretium vel. Donec nec lacus id ligula dignissim egestas.
In ultrices at mi id tristique. Maecenas id purus a tellus sagittis hendrerit non in ligula. Vestibulum iaculis justo in finibus tempor. Sed cursus elit at massa sollicitudin, eu ultrices nunc tristique. Proin dapibus sit amet tellus et auctor. Curabitur imperdiet lacus mattis euismod porta. Nullam et nulla maximus, ultricies eros et, volutpat libero. Sed eu sollicitudin purus. Mauris pretium dui eget tempor ullamcorper. Quisque sed dignissim est, non bibendum massa. Etiam eleifend porta nunc, dapibus pharetra ex accumsan quis. Pellentesque aliquet ac ligula non aliquet. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce hendrerit tempor quam, eget malesuada magna. Vivamus condimentum urna et libero dictum hendrerit. Mauris accumsan euismod mauris, id iaculis metus.
Nullam nec consectetur nisi. Cras et diam risus. Nullam sollicitudin tellus a elementum tincidunt. Duis sagittis sit amet est nec pellentesque. Etiam efficitur, mauris eu molestie vestibulum, mauris erat consequat turpis, elementum convallis nisi dolor commodo nunc. Vivamus non hendrerit ante. Pellentesque imperdiet posuere odio sed viverra. Nunc accumsan vestibulum interdum. Ut magna nibh, scelerisque vel ex eu, facilisis porta ligula.
Phasellus egestas rhoncus sem non venenatis. Ut facilisis ac nibh vitae ullamcorper. Cras dictum nisl et dolor vulputate, at mollis nulla semper. Suspendisse dictum pellentesque purus vel porttitor. Phasellus eu nunc venenatis libero ornare consectetur. Mauris luctus justo erat, at ultricies erat euismod sed. Vestibulum non felis vel neque maximus sollicitudin a at tortor. Fusce ornare vestibulum ex, vitae lacinia eros. Phasellus imperdiet arcu massa, a ullamcorper odio condimentum id. Vestibulum libero leo, posuere ut consequat id, accumsan ac orci. Donec pretium auctor ipsum, id lobortis neque finibus eu.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Curabitur efficitur hendrerit feugiat. Cras et placerat sem. Mauris ut accumsan dolor, vel tincidunt augue. Vestibulum tempor convallis sem, id molestie mauris pulvinar at. Nulla convallis, lorem vel mollis laoreet, odio risus maximus neque, ac molestie mauris neque non lectus. Phasellus a neque at nulla laoreet maximus. Nulla in risus velit. Suspendisse vitae lacus porta, iaculis dolor convallis, gravida ipsum.
Fusce mollis nisi eu nibh lacinia ullamcorper. Aliquam gravida massa eu mauris pellentesque, ut rhoncus ante porta. Nam a massa ex. Sed libero ante, condimentum eget vulputate at, blandit at eros. Curabitur ullamcorper malesuada dolor vitae ultrices. In lacinia libero ac urna rutrum porta. Mauris non neque hendrerit tortor ultricies euismod. Cras id odio ac risus suscipit euismod. Nullam et ante tempus, vulputate est luctus, viverra urna. Nulla auctor, metus quis vulputate mattis, lorem velit consequat arcu, et lacinia tortor odio vel nibh. Etiam vitae dui at eros condimentum faucibus at in metus.
Pellentesque congue sapien a nisi eleifend luctus. Nulla placerat, massa in lobortis interdum, quam metus rhoncus odio, quis commodo quam justo id elit. Suspendisse lobortis, velit fermentum tempus faucibus, lacus augue iaculis nulla, non pulvinar eros nisl vel ante. Nam ac ipsum venenatis, maximus diam sed, malesuada ipsum. Mauris leo lacus, commodo vitae consequat eu, pulvinar ullamcorper ex. Pellentesque leo dui, gravida in maximus nec, eleifend quis elit. In hac habitasse platea dictumst. In nec maximus nisi. Mauris sit amet purus ultricies, pulvinar mi eget, commodo nunc. Integer tristique vel nunc nec tristique. Mauris vehicula lacinia mauris, a porta neque lacinia ut. Curabitur ut mattis neque. Nullam vitae nulla interdum enim malesuada lobortis. Aenean lorem elit, dapibus at neque quis, porttitor dictum augue. Duis convallis feugiat purus, at faucibus augue scelerisque vel. Suspendisse eu placerat orci.
Donec ligula ipsum, aliquam sit amet euismod convallis, commodo vel mauris. Fusce interdum dolor in nisl tempor, eget condimentum enim dapibus. Duis fermentum felis vitae dolor feugiat consequat. Praesent tincidunt accumsan purus quis blandit. Suspendisse aliquet, augue sit amet eleifend lobortis, nulla arcu maximus tellus, ac gravida nisi sapien id mi. Ut cursus orci sed vulputate elementum. Duis tempor mauris magna, ac tempus metus auctor ut.
Vivamus ultricies lobortis sapien sed vehicula. Curabitur in faucibus velit. Donec quis magna sit amet leo pulvinar rutrum. Praesent a nisi non diam feugiat eleifend nec vitae massa. Duis blandit vulputate nibh in aliquam. Nam at ante viverra, tempor ipsum nec, consectetur arcu. Suspendisse elementum vulputate nisi et cursus. Aenean sit amet dolor in ex posuere cursus in a mauris. Nam et nisl massa. Praesent in nulla a lacus sagittis suscipit sed interdum diam.
Suspendisse sed sapien convallis, luctus velit ac, ornare ipsum. Morbi a ipsum facilisis, egestas ex eu, sodales elit. Curabitur faucibus sollicitudin nunc sed rhoncus. Suspendisse scelerisque nisi vitae mi consequat, eget tincidunt nisi lobortis. Aenean in urna in dolor tincidunt egestas eu id orci. Mauris laoreet lectus nec mollis tincidunt. Etiam feugiat est gravida commodo volutpat. Sed id nunc eleifend, condimentum quam sed, tincidunt tortor.
Ut et vehicula lorem. Curabitur vitae mauris libero. Vestibulum vitae velit dapibus, aliquam dolor in, posuere nisl. Aenean at libero vehicula, auctor dui eu, tempus dolor. In purus ex, fermentum id dolor id, convallis suscipit justo. In nec purus tristique, auctor eros vitae, mattis magna. Sed suscipit, turpis sed porta ullamcorper, metus nibh dignissim nisi, at facilisis elit est et ipsum. Donec elit velit, lacinia a risus a, vestibulum tincidunt erat. Vivamus sit amet fermentum elit. Aliquam accumsan tortor quis risus auctor, sed sodales est interdum. Nullam eget dictum erat. Curabitur nec augue convallis, mollis erat a, scelerisque magna. Interdum et malesuada fames ac ante ipsum primis in faucibus. Vestibulum eu lectus tortor. Praesent molestie augue sit amet libero auctor consequat. Pellentesque eu commodo elit.
Nullam vestibulum sapien in leo ultricies, nec tempor tellus molestie. Integer imperdiet, est ac vulputate eleifend, diam leo eleifend metus, finibus viverra lacus augue ac leo. In tristique neque sed varius rhoncus. Vestibulum sit amet arcu at mauris tincidunt tincidunt ac non diam. Fusce vitae porta urna. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Donec suscipit felis ac lectus pulvinar, consectetur iaculis nibh pharetra. Phasellus elit ligula, auctor quis libero a, faucibus commodo erat.
Aenean non sapien non tortor aliquet cursus ut sit amet lectus. Quisque maximus vestibulum arcu, id pellentesque velit viverra non. Etiam et ornare enim. Nam in quam auctor, condimentum metus eu, scelerisque lorem. Maecenas quis lacus quis enim dignissim viverra. Mauris risus dolor, euismod in mauris in, dictum molestie purus. Sed cursus risus fringilla dui euismod porttitor. Mauris vel suscipit mauris, sed porta ex. Nulla sem mauris, laoreet faucibus ex et, porta faucibus tortor. Pellentesque egestas cursus vulputate. Sed a nibh ut ante tincidunt suscipit sed luctus mauris. Sed sodales et nibh eget egestas.
Suspendisse vitae dolor quis nibh lobortis vulputate. Nulla sed neque vitae odio efficitur posuere quis at justo. Proin tristique sodales lacus, at tincidunt metus tempor eget. Vivamus auctor eleifend sapien nec feugiat. Pellentesque mollis consectetur iaculis. Donec hendrerit cursus velit, ut vulputate ante sodales quis. Fusce interdum egestas turpis quis vestibulum. Vivamus magna dolor, tempor ut consectetur non, lacinia a nibh. Ut vehicula faucibus nulla, et tempus lorem consequat vitae. In hac habitasse platea dictumst.
Ut egestas condimentum nisi nec varius. Pellentesque fermentum elit et blandit molestie. Nunc dictum velit in augue sollicitudin lacinia. Duis ligula purus, finibus in enim at, bibendum dignissim turpis. Suspendisse a ligula feugiat, consectetur dui id, bibendum odio. Morbi auctor turpis imperdiet tortor tristique dictum. Aliquam erat volutpat. Etiam feugiat ex justo, eget venenatis turpis sagittis sed. Maecenas ut venenatis massa, ut consectetur magna. Vivamus et nunc ac sapien eleifend aliquam.
Nam et consectetur lorem. Cras interdum pellentesque odio, id accumsan est porta vel. Fusce vel aliquet ligula, quis convallis nulla. Phasellus quis dolor lorem. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In ultricies, ipsum eu faucibus blandit, justo magna varius justo, ut scelerisque diam risus sit amet purus. Donec at malesuada mauris.
Nullam a odio dapibus, mollis est id, dapibus orci. Donec leo risus, commodo non diam at, elementum semper lacus. Maecenas dignissim massa nec urna consectetur consequat. Nullam vel risus auctor, suscipit libero et, faucibus lorem. Donec feugiat magna ut diam auctor, eget dictum massa auctor. Aliquam eros nisi, vestibulum nec tellus non, varius venenatis turpis. Nulla ultricies tempor libero, in dapibus neque semper sit amet. Integer orci elit, volutpat eu mollis nec, posuere vel dolor. Vestibulum a consequat tortor. Nam iaculis diam in mollis finibus.
Donec et facilisis tellus. Morbi molestie porta consequat. Vivamus porta purus at sapien venenatis congue. Vivamus a blandit nunc. Quisque scelerisque augue sit amet ultrices vestibulum. Suspendisse ullamcorper augue eu magna pretium porttitor. Morbi in odio sed ipsum auctor facilisis in vitae nibh. Nam ipsum ligula, sollicitudin nec sem imperdiet, pretium facilisis dolor. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Quisque tristique lectus eu placerat consectetur. Mauris vestibulum venenatis lorem. Praesent odio sem, pretium id finibus quis, ullamcorper in lectus.
Morbi nec elementum nisi. Vestibulum vehicula metus id pretium maximus. Praesent et odio neque. Nunc porta accumsan mi sit amet vestibulum. Sed ut tempus ipsum. Donec ante ipsum, commodo eget nibh at, bibendum congue lectus. Cras placerat in lorem sed tincidunt. Ut et pellentesque nibh. Nullam accumsan tempus diam, non bibendum enim malesuada non. In lacus felis, pulvinar in accumsan vel, mollis eu metus. Morbi sed varius magna. Donec sodales dui nec mauris egestas, commodo pulvinar libero malesuada. Nulla aliquet arcu quis odio dapibus tincidunt.
Curabitur sed lacinia sem. Sed vulputate lorem id malesuada aliquet. Phasellus non blandit lectus. Sed consectetur leo non diam molestie condimentum. Nullam interdum enim ac tellus feugiat, a iaculis justo vehicula. Nullam id placerat odio, vel interdum enim. Phasellus condimentum sapien non est iaculis cursus. Suspendisse a risus nisl. Nam id imperdiet justo. Cras nec odio ut lorem posuere sodales. Aliquam erat volutpat.
Vestibulum bibendum lacinia urna sed malesuada. Aenean eros dolor, condimentum id condimentum at, feugiat nec mi. Vivamus et ligula in mauris rhoncus vestibulum. Aenean id mi velit. Phasellus eu augue a ligula interdum ornare vitae sed ipsum. Aliquam fringilla eu lacus in mollis. Etiam venenatis porta lacus in egestas. Donec porttitor commodo gravida. Phasellus porta odio dignissim, gravida nibh vel, scelerisque dui. Maecenas pretium, elit eu efficitur ultrices, sapien felis tristique nibh, ut vestibulum libero massa fringilla quam. Nulla euismod ligula sit amet massa suscipit vehicula. Pellentesque ante velit, gravida ut volutpat vitae, molestie id orci. Mauris dignissim aliquam lorem faucibus viverra. Duis felis tellus, volutpat id tellus vitae, condimentum fermentum odio. Nulla non odio massa.
Duis venenatis, sapien id pulvinar cursus, nibh ex molestie diam, id cursus enim massa in metus. Mauris tempor gravida cursus. Fusce fringilla orci massa, et iaculis orci viverra in. Aliquam hendrerit magna metus. Integer vehicula, eros interdum dignissim molestie, enim est scelerisque erat, vehicula dignissim quam turpis id felis. Vestibulum vel posuere justo. Sed eu pellentesque ligula. Donec in magna maximus, pretium eros pellentesque, finibus elit. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Praesent accumsan volutpat est ac sollicitudin. Curabitur pharetra interdum dolor vitae hendrerit.
Nam gravida tortor et felis facilisis, sed ultricies felis convallis. Quisque a pharetra lectus. Aliquam consequat consectetur eros, et tincidunt libero accumsan nec. Praesent auctor tincidunt nulla et ultrices. Morbi posuere est et volutpat pharetra. Sed rutrum dictum neque eget ultricies. Sed mi felis, porta ac pharetra nec, laoreet eget nulla. In malesuada viverra odio lacinia porttitor. Sed augue enim, cursus quis turpis vitae, congue vehicula libero.
Integer ornare euismod tortor eget dapibus. Donec luctus, ipsum ac facilisis cursus, nisl augue viverra nisi, a malesuada nisi lectus quis libero. Praesent euismod lacus in velit elementum, nec interdum velit dictum. Fusce nisl ipsum, faucibus ac ipsum id, lobortis dictum tortor. Duis non sollicitudin dui. Quisque nunc ipsum, suscipit a tellus sed, eleifend tempus eros. Praesent pulvinar mauris sed tortor feugiat, ut interdum mauris eleifend. Integer iaculis, justo vitae tincidunt consequat, mauris turpis fringilla mauris, mattis sollicitudin tellus justo sit amet sem. Morbi eu suscipit lacus. Proin tempus ex in lectus auctor tincidunt. Vivamus sem justo, placerat a volutpat sed, vulputate a neque.
Suspendisse tempus interdum metus, eu malesuada elit cursus dictum. Proin in tortor eu massa porta molestie. Vivamus ac nulla aliquam, imperdiet mi sit amet, tincidunt ante. Vivamus at lacinia sapien. Proin nec nisi ante. Integer sem orci, finibus a risus at, auctor vulputate arcu. Cras vestibulum, ex et fermentum suscipit, magna odio viverra orci, in sodales purus tellus id mauris. Nullam volutpat et mauris sit amet tincidunt. Nam accumsan ligula sed diam euismod, ultricies vulputate leo bibendum.
Vestibulum sem arcu, consectetur vitae nulla in, sagittis viverra ipsum. Morbi eleifend turpis eget luctus semper. Mauris ipsum ligula, finibus et dapibus pharetra, mollis rutrum nisi. Nulla vel sapien felis. Integer consequat ex non nisi ullamcorper, sit amet dignissim nulla volutpat. Proin volutpat aliquam dolor, nec imperdiet mauris tempor id. In congue dapibus mauris ac vehicula. Nunc fringilla eu sapien at venenatis. Ut quam nunc, scelerisque ullamcorper est non, feugiat lacinia massa. Integer in diam quis ipsum molestie placerat.
Integer posuere neque massa, at tristique orci gravida et. Sed sed ullamcorper turpis. Phasellus auctor est sed lectus vehicula dictum. Fusce a luctus est. Nullam congue aliquet lorem, vel pellentesque urna rhoncus eget. Sed egestas felis nulla, in dictum mauris vulputate consectetur. Maecenas cursus efficitur ante sit amet venenatis. Duis id lacus imperdiet, varius erat ut, posuere orci. Donec condimentum id velit ac elementum. Praesent vehicula lobortis ante. Nulla in bibendum erat. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Nullam ut ultricies ante. Proin rutrum neque nec velit luctus, nec gravida quam mollis.
Nunc aliquet viverra massa, sed pharetra est cursus in. Vestibulum lobortis, leo eget ullamcorper finibus, sem sem maximus magna, et feugiat mi libero sed erat. Aliquam sit amet tristique odio. Duis luctus commodo ex. Integer est leo, viverra id condimentum id, lacinia quis tellus. Curabitur hendrerit dignissim fermentum. Mauris a neque malesuada turpis ultrices molestie. Nulla metus dui, tempor sed erat vitae, hendrerit lobortis risus. Aliquam molestie sapien nisl, quis dignissim diam rutrum laoreet. Donec eros mi, pharetra sed arcu at, semper venenatis magna. Aliquam id quam et metus facilisis tincidunt. In et ante mi. Suspendisse consectetur purus ac ullamcorper congue. Sed convallis neque vitae leo molestie, a rutrum eros facilisis. Duis id dui molestie, gravida nibh sed, facilisis quam.
Maecenas suscipit, nulla vitae laoreet rutrum, justo urna dapibus nibh, ac vestibulum nulla sem et velit. Aliquam commodo arcu molestie arcu lobortis lobortis consequat a tellus. Nam fermentum, quam ac elementum laoreet, ex lorem laoreet libero, eget tempor justo lorem nec sapien. Sed rhoncus posuere fermentum. Integer tincidunt finibus quam, non mollis nibh viverra eget. Cras hendrerit eros in lectus elementum vehicula. Nullam id blandit dui. Mauris rhoncus velit ac volutpat pharetra. Donec vitae risus faucibus, mollis magna vel, semper arcu. Cras sit amet erat cursus, faucibus est in, tempus nisi. Ut orci velit, aliquam at maximus in, volutpat at orci. Nunc lacinia eros sed neque vestibulum, eu placerat massa iaculis. Proin tempor est ac lorem tempor sodales.
Integer volutpat feugiat mi, et efficitur massa pretium eget. Quisque facilisis semper bibendum. Curabitur ac interdum risus. Nulla id dui dictum, condimentum ipsum id, varius massa. In ut metus viverra, posuere tellus sed, vehicula metus. Vestibulum consectetur interdum tortor non egestas. Morbi a posuere sapien, a luctus nibh. Vestibulum congue dapibus urna, ac feugiat tortor semper nec. Etiam id augue metus. Maecenas id pretium elit. Pellentesque sit amet consectetur velit.
Pellentesque sed vulputate nibh, id tincidunt ligula. Mauris pulvinar a arcu in sollicitudin. Morbi vestibulum massa accumsan maximus pulvinar. Praesent iaculis scelerisque lectus, ac placerat metus maximus ac. Cras ipsum neque, eleifend nec ornare non, interdum eget quam. Cras laoreet ex quis cursus lobortis. Morbi tellus odio, posuere eu molestie porta, ultrices ut elit. Nulla ullamcorper, orci eget lacinia congue, metus nunc euismod elit, eu efficitur magna velit et nisi. Ut convallis lacus id nulla egestas, condimentum faucibus lectus placerat. Quisque a metus risus.
Morbi sit amet laoreet ligula, gravida consectetur odio. Nunc et posuere eros. Praesent ut nulla facilisis, suscipit purus fringilla, convallis nibh. Proin consequat, ex vel vulputate sagittis, mi quam feugiat metus, ac tincidunt augue leo sit amet arcu. Fusce a mi a nunc dapibus congue. Nullam nec convallis sapien. Praesent tempus maximus pretium. Cras vel nulla quis arcu congue pharetra.
Vestibulum a fringilla massa. In hac habitasse platea dictumst. Fusce sit amet justo condimentum, aliquet augue convallis, elementum nunc. Sed posuere sit amet dolor at gravida. Praesent vitae erat sed quam ullamcorper sollicitudin. Nulla tincidunt tempus turpis, at commodo lacus ornare nec. Aenean consectetur justo at bibendum blandit.
Nunc fringilla arcu eu pellentesque sollicitudin. Nulla facilisi. Suspendisse vel mattis justo. Vestibulum nec purus vitae nisl porta auctor. Donec molestie aliquet libero, vel ornare justo scelerisque consectetur. In at odio aliquet, gravida neque eget, porta velit. Etiam congue facilisis nibh non egestas.
Praesent posuere turpis non neque gravida maximus. Mauris nunc enim, hendrerit at ex ut, consequat iaculis neque. Morbi ex turpis, volutpat sed consequat vitae, ornare non eros. Suspendisse potenti. Fusce nisi justo, sodales eu quam et, gravida bibendum urna. Integer convallis fringilla felis, a iaculis tortor consequat a. Duis rhoncus ligula diam, vel dictum purus ornare vel. Aenean placerat mollis quam, eget hendrerit turpis vulputate sed. Sed ut hendrerit nunc. Donec scelerisque sodales turpis ut sagittis. Morbi vel orci id libero lacinia porttitor. Duis aliquet eget turpis sed venenatis.
Cras laoreet eleifend ex ut facilisis. Fusce nec lacinia felis, vel aliquet eros. Suspendisse nec venenatis ante. Duis ac enim eget urna tempus tristique varius eget odio. Morbi et neque non urna laoreet pulvinar vel nec tellus. Cras venenatis lacus sit amet massa vulputate ornare. Aenean posuere vehicula ligula, vitae luctus lectus pretium nec. Donec aliquet consectetur magna, nec tincidunt sem varius in. Nunc aliquet sit amet mauris a posuere. Nunc varius massa id mauris egestas varius.
Aenean laoreet odio vel urna volutpat luctus. Maecenas ac condimentum metus. Cras laoreet ultricies magna at tristique. Aenean ante ante, fermentum eget vestibulum in, finibus nec arcu. Aenean pulvinar leo at lectus interdum ornare. Ut nec eros ex. Quisque at ullamcorper risus, ut viverra dui. Mauris imperdiet blandit cursus. Donec eget ante nec libero efficitur rhoncus. Nullam lectus lorem, vehicula in mollis a, fermentum eget lacus. Mauris ipsum odio, dictum at felis eu, tincidunt consectetur libero. Cras tempus finibus nunc non tincidunt. Quisque viverra volutpat metus. Morbi fringilla venenatis efficitur.
Cras ut magna at tortor vehicula ultrices. Sed lacus metus, tristique sit amet dictum et, porttitor ac purus. Phasellus pellentesque, dui auctor luctus vehicula, erat mauris vehicula ipsum, vel suscipit neque tortor vel elit. Cras ac nisi sit amet sem volutpat venenatis nec et mi. Nullam sit amet purus non ante iaculis porttitor. Aenean viverra, tellus eget eleifend scelerisque, est lacus blandit urna, sodales eleifend quam lorem sit amet magna. Vivamus mi purus, suscipit ut tempus bibendum, mattis vitae sapien. Maecenas ut finibus lectus. Donec enim justo, rutrum a felis vitae, aliquet venenatis ex. Donec ultrices lacus et risus vulputate mollis et eu augue. Proin rutrum mattis dolor, nec placerat odio maximus sed. Nunc cursus libero at quam dictum, non placerat enim fringilla. Curabitur a convallis justo.
Nulla a dolor in nibh tincidunt blandit. Donec congue, nisl in dictum semper, nunc lectus accumsan dolor, eu consequat velit erat ac libero. Integer ultricies felis purus, vitae sagittis sapien malesuada a. Quisque sed pretium mi. In accumsan enim at urna suscipit ornare. Nunc rhoncus varius diam, nec finibus nunc congue vel. Sed risus urna, pellentesque ut tortor vel, semper lacinia massa. Sed molestie convallis tristique.
Aenean porta vehicula turpis eget condimentum. Aenean finibus justo vel nisi vestibulum, id placerat leo luctus. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas a risus et mauris luctus vehicula id vitae lectus. Sed molestie bibendum risus non pretium. Sed a posuere mauris, vitae ornare diam. Praesent ac quam egestas, molestie arcu nec, volutpat lacus. Nulla at sagittis mi. Integer id justo ante. Nulla et metus id mauris finibus volutpat eget sed nisi. Maecenas ac gravida lacus, id feugiat neque. Nullam auctor purus ut dolor euismod, nec congue ante placerat. Donec fermentum orci quis aliquam congue.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam tincidunt viverra orci eget ornare. Nam mattis nunc a gravida scelerisque. Phasellus ullamcorper tellus nec tincidunt rhoncus. Nunc ac risus orci. Ut bibendum pharetra neque eu semper. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Etiam convallis lectus non pharetra commodo.`)
