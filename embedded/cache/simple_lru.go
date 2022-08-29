package cache

import (
	"sync"
	"unsafe"
)

const (
	prev = uint16(0)
	next = uint16(1)
)

type sptr struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// hash is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aeshash if aes instruction is available).
func hash(str string) uint64 {
	ss := (*sptr)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func maskOfNextPowOf2(cap uint16) uint16 {
	if cap > 0 && cap&(cap-1) == 0 {
		return cap - 1
	}
	cap |= (cap >> 1)
	cap |= (cap >> 2)
	cap |= (cap >> 4)
	return cap | (cap >> 8)
}

type node struct {
	k string
	v *interface{}
}

type cache struct {
	dlnk [][2]uint16       // double link list, 0 for prev, 1 for next, the first node stands for [tail, head]
	m    []node            // memory pre-allocated
	hmap map[string]uint16 // key -> idx in []node
	last uint16            // last element index when not full
}

func create(cap uint16) *cache {
	return &cache{
		dlnk: make([][2]uint16, uint32(cap)+1),
		m:    make([]node, cap),
		hmap: make(map[string]uint16, cap),
		last: 0,
	}
}

/*
If the element exists in hashmap
    move the accessed element to the tail of the linked list
Otherwise,
    if eviction is needed i.e. cache is already full
        Remove the head element from doubly linked list and delete its hashmap entry
    Add the new element at the tail of linked list and in hashmap
Get from Cache and Return
*/

// put a cache item into lru cache, if added return 1, updated return 0
func (c *cache) put(k string, i *interface{}) int {
	if x, ok := c.hmap[k]; ok {
		c.m[x-1].v = i
		c.move(x, prev, next) // refresh to head
		return 0
	}

	if c.last == uint16(cap(c.m)) {
		tail := &c.m[c.dlnk[0][prev]-1]
		delete(c.hmap, (*tail).k)
		c.hmap[k], (*tail).k, (*tail).v = c.dlnk[0][prev], k, i // reuse to reduce gc
		c.move(c.dlnk[0][prev], prev, next)                     // refresh to head
		return 1
	}

	/*
	   // append the node to the end of the list
	   func (this *LRUCache) append(node *Node) {
	       tailPrev := this.tail.prev
	       node.next = this.tail
	       this.tail.prev = node
	       tailPrev.next = node
	       node.prev = tailPrev
	   }
	*/

	c.last++
	if len(c.hmap) == 0 {
		c.dlnk[0][prev] = c.last
	} else {
		c.dlnk[c.dlnk[0][next]][prev] = c.last
	}

	c.m[c.last-1].k = k
	c.m[c.last-1].v = i
	c.dlnk[c.last] = [2]uint16{0, c.dlnk[0][next]}
	c.hmap[k] = c.last
	c.dlnk[0][next] = c.last
	return 1
}

// get value of key from lru cache with result
func (c *cache) get(k string) (*node, int) {
	if x, ok := c.hmap[k]; ok {
		c.move(x, prev, next) // refresh to head
		return &c.m[x-1], 1
	}
	return nil, 0
}

// delete item by key from lru cache
func (c *cache) del(k string) (_ *node, _ int) {
	if x, ok := c.hmap[k]; ok {
		c.move(x, next, prev) // sink to tail
		return &c.m[x-1], 1
	}
	return nil, 0
}

// calls f sequentially for each valid item in the lru cache
func (c *cache) walk(walker func(key string, iface *interface{}) bool) {
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		if !walker(c.m[idx-1].k, c.m[idx-1].v) {
			return
		}
	}
}

// when f=0, t=1, move to head, otherwise to tail
func (c *cache) move(idx, f, t uint16) {
	if c.dlnk[idx][f] != 0 { // f=0, t=1, not head node, otherwise not tail
		c.dlnk[c.dlnk[idx][t]][f] = c.dlnk[idx][f]
		c.dlnk[c.dlnk[idx][f]][t] = c.dlnk[idx][t]
		c.dlnk[idx][f] = 0
		c.dlnk[idx][t] = c.dlnk[0][t]
		c.dlnk[c.dlnk[0][t]][f] = idx
		c.dlnk[0][t] = idx
	}
}

// Cache - concurrent cache structure
type Cache struct {
	locks []sync.Mutex
	insts []*cache
	mask  uint64
}

// NewLRUECache - create lru cache
// `bucketCnt` is buckets that shard items to reduce lock racing
// `capPerBkt` is length of each bucket, can store `capPerBkt * bucketCnt` count of items in Cache at most
func NewLRUECache(bucketCnt, capPerBkt uint16) *Cache {
	mask := maskOfNextPowOf2(bucketCnt)
	c := &Cache{make([]sync.Mutex, mask+1), make([]*cache, mask+1), uint64(mask)}
	for i := range c.insts {
		c.insts[i] = create(capPerBkt)
	}
	return c
}

// put - put a item into cache
func (c *Cache) put(key string, i *interface{}) {
	idx := hash(key) & c.mask
	c.locks[idx].Lock()
	c.insts[idx].put(key, i)
	c.locks[idx].Unlock()
}

// Put - put an item into cache
func (c *Cache) Put(key string, val interface{}) { c.put(key, &val) }

// Get - get value of key from cache with result
func (c *Cache) Get(key string) (interface{}, bool) {
	if i, ok := c.get(key); ok && i != nil {
		return *i, true
	}
	return nil, false
}

func (c *Cache) get(key string) (i *interface{}, _ bool) {
	idx := hash(key) & c.mask
	c.locks[idx].Lock()
	n, s := c.insts[idx].get(key)
	if s <= 0 {
		c.locks[idx].Unlock()
		return
	}
	c.locks[idx].Unlock()
	return n.v, true
}

// Del - delete item by key from cache
func (c *Cache) Del(key string) {
	idx := hash(key) & c.mask
	c.locks[idx].Lock()
	n, s := c.insts[idx].del(key)
	if s > 0 {
		n.v = nil // release now
	}
	c.locks[idx].Unlock()
}

// Walk - calls f sequentially for each valid item in the lru cache, return false to stop iteration for every bucket
func (c *Cache) Walk(walker func(key string, iface *interface{}) bool) {
	for i := range c.insts {
		c.locks[i].Lock()
		c.insts[i].walk(walker)
		c.locks[i].Unlock()
	}
}
