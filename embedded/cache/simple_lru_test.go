package cache

import (
	"container/list"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkPutTinyObject_ecacheLRU2(b *testing.B) {
	cache := NewLRUECache(128, 16)
	for i := 0; i < b.N; i++ {
		cache.Put(fmt.Sprint(i), "baz")
	}
}

func BenchmarkPutTinyObject_LRU2(b *testing.B) {
	initialCacheSize := 16
	cache, err := NewLRUCache(initialCacheSize)
	require.NoError(b, err)
	require.NotNil(b, cache)
	for i := 0; i < b.N; i++ {
		cache.Put(fmt.Sprint(i), "baz")
	}
}

func iface(i interface{}) *interface{} { return &i }

type Elem struct {
	key string
	val string
}

var on = func(int, string, *interface{}, int) {}

func Test_put(t *testing.T) {
	c := create(5)
	c.put("1", iface("1"))
	c.put("2", iface("2"))
	c.put("1", iface("3"))
	if len(c.hmap) != 2 {
		t.Error("case 2.1 failed")
	}

	l := list.New()
	l.PushBack(&Elem{"1", "3"})
	l.PushBack(&Elem{"2", "2"})

	e := l.Front()
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		v := e.Value.(*Elem)
		el := c.m[idx-1]
		if el.k != v.key {
			t.Error("case 2.2 failed: ", el.k, v.key)
		}
		if (*(el.v)).(string) != v.val {
			t.Error("case 2.3 failed: ", (*(el.v)).(string), v.val)
		}
		e = e.Next()
	}

	c.put("3", iface("4"))
	c.put("4", iface("5"))
	c.put("5", iface("6"))
	c.put("2", iface("7"))
	if len(c.hmap) != 5 {
		t.Error("case 3.1 failed")
	}

	l = list.New()
	l.PushBack(&Elem{"2", "7"})
	l.PushBack(&Elem{"5", "6"})
	l.PushBack(&Elem{"4", "5"})
	l.PushBack(&Elem{"3", "4"})
	l.PushBack(&Elem{"1", "3"})

	rl := list.New()
	rl.PushBack(&Elem{"1", "3"})
	rl.PushBack(&Elem{"3", "4"})
	rl.PushBack(&Elem{"4", "5"})
	rl.PushBack(&Elem{"5", "6"})
	rl.PushBack(&Elem{"2", "7"})

	e = l.Front()
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		v := e.Value.(*Elem)
		el := c.m[idx-1]
		if el.k != v.key {
			t.Error("case 3.2 failed: ", el.k, v.key)
		}
		if (*(el.v)).(string) != v.val {
			t.Error("case 3.3 failed: ", (*(el.v)).(string), v.val)
		}
		e = e.Next()
	}

	e = rl.Front()
	for idx := c.dlnk[0][prev]; idx != 0; idx = c.dlnk[idx][prev] {
		v := e.Value.(*Elem)
		el := c.m[idx-1]
		if el.k != v.key {
			t.Error("case 3.4 failed: ", el.k, v.key)
		}
		if (*(el.v)).(string) != v.val {
			t.Error("case 3.5 failed: ", (*(el.v)).(string), v.val)
		}
		e = e.Next()
	}

	c.put("6", iface("8"))
	if len(c.hmap) != 5 {
		t.Error("case 4.1 failed")
	}

	l = list.New()
	l.PushBack(&Elem{"6", "8"})
	l.PushBack(&Elem{"2", "7"})
	l.PushBack(&Elem{"5", "6"})
	l.PushBack(&Elem{"4", "5"})
	l.PushBack(&Elem{"3", "4"})

	e = l.Front()
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		v := e.Value.(*Elem)
		el := c.m[idx-1]
		if el.k != v.key {
			t.Error("case 4.2 failed: ", el.k, v.key)
		}
		if (*(el.v)).(string) != v.val {
			t.Error("case 4.3 failed: ", (*(el.v)).(string), v.val)
		}
		e = e.Next()
	}
}

func Test_get(t *testing.T) {
	c := create(2)
	c.put("1", iface("1"))
	c.put("2", iface("2"))
	if v, _ := c.get("1"); *(v.v) != "1" {
		t.Error("case 1.1 failed")
	}
	c.put("3", iface("3"))
	if len(c.hmap) != 2 {
		t.Error("case 1.2 failed")
	}

	l := list.New()
	l.PushBack(&Elem{"3", "3"})
	l.PushBack(&Elem{"1", "1"})

	e := l.Front()
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		v := e.Value.(*Elem)
		el := c.m[idx-1]
		if el.k != v.key {
			t.Error("case 1.3 failed: ", el.k, v.key)
		}
		if (*(el.v)).(string) != v.val {
			t.Error("case 1.4 failed: ", (*(el.v)).(string), v.val)
		}
		e = e.Next()
	}
}

func Test_delete(t *testing.T) {
	c := create(5)
	c.put("3", iface("4"))
	c.put("4", iface("5"))
	c.put("5", iface("6"))
	c.put("2", iface("7"))
	c.put("6", iface("8"))
	c.del("5")

	l := list.New()
	l.PushBack(&Elem{"6", "8"})
	l.PushBack(&Elem{"2", "7"})
	l.PushBack(&Elem{"4", "5"})
	l.PushBack(&Elem{"3", "4"})

	e := l.Front()
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		el := c.m[idx-1]
		fmt.Printf("IDXX %+v\n", idx)
		fmt.Printf("ohhh %+v\n", el)
		fmt.Printf("ahhh %+v\n", e)
		v := e.Value.(*Elem)
		if el.k != v.key {
			t.Error("case 1.2 failed: ", el.k, v.key)
		}
		if (*(el.v)).(string) != v.val {
			t.Error("case 1.3 failed: ", (*(el.v)).(string), v.val)
		}
		e = e.Next()
	}

	c.del("6")

	l = list.New()
	l.PushBack(&Elem{"2", "7"})
	l.PushBack(&Elem{"4", "5"})
	l.PushBack(&Elem{"3", "4"})
	/*if len(c.hmap) != 3 {
		t.Error("case 2.1 failed")
	}*/

	e = l.Front()
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		el := c.m[idx-1]
		v := e.Value.(*Elem)
		if el.k != v.key {
			t.Error("case 2.2 failed: ", el.k, v.key)
		}
		if (*(el.v)).(string) != v.val {
			t.Error("case 2.3 failed: ", (*(el.v)).(string), v.val)
		}
		e = e.Next()
	}

	c.del("3")

	l = list.New()
	l.PushBack(&Elem{"2", "7"})
	l.PushBack(&Elem{"4", "5"})
	/*if len(c.hmap) != 2 {
		t.Error("case 3.1 failed")
	}*/

	e = l.Front()
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		el := c.m[idx-1]
		v := e.Value.(*Elem)
		if el.k != v.key {
			t.Error("case 3.2 failed: ", el.k, v.key)
		}
		if (*(el.v)).(string) != v.val {
			t.Error("case 3.3 failed: ", (*(el.v)).(string), v.val)
		}
		e = e.Next()
	}
}

func Test_walk(t *testing.T) {
	c := create(5)
	c.put("3", iface(4))
	c.put("4", iface(5))
	c.put("5", iface(6))
	c.put("2", iface(7))
	c.put("6", iface(8))

	l := list.New()
	l.PushBack(&Elem{"6", "8"})
	l.PushBack(&Elem{"2", "7"})
	l.PushBack(&Elem{"5", "6"})
	l.PushBack(&Elem{"4", "5"})
	l.PushBack(&Elem{"3", "4"})

	e := l.Front()
	c.walk(
		func(key string, iface *interface{}) bool {
			v := e.Value.(*Elem)
			if key != v.key {
				t.Error("case 1.1 failed: ", key, v.key)
			}
			if fmt.Sprint(*iface) != v.val {
				t.Error("case 1.2 failed: ", *iface, v.val)
			}
			e = e.Next()
			return true
		})

	if e != nil {
		t.Error("case 1.3 failed: ", e.Value)
	}

	e = l.Front()
	c.walk(
		func(key string, iface *interface{}) bool {
			v := e.Value.(*Elem)
			if key != v.key {
				t.Error("case 1.1 failed: ", key, v.key)
			}
			if fmt.Sprint(*iface) != v.val {
				t.Error("case 1.2 failed: ", iface, v.val)
			}
			return false
		})
}

func TestLRUCache(t *testing.T) {
	lc := NewLRUECache(1, 3)
	lc.Put("1", "1")
	lc.Put("2", "2")
	lc.Put("3", "3")

	v, _ := lc.Get("2") // check reuse
	lc.Put("4", "4")
	lc.Put("5", "5")
	lc.Put("6", "6")

	if v != "2" {
		fmt.Println(v)
		t.Error("case 3 failed")
	}
}

func Test_size(t *testing.T) {
	c := create(4)
	c.put("3", iface("3"))
	c.put("4", iface("4"))
	c.put("5", iface("5"))
	c.put("6", iface("6"))

	l := list.New()
	l.PushBack(&Elem{"4", "5"})
	l.PushBack(&Elem{"3", "4"})

	e := l.Front()
	for idx := c.dlnk[0][next]; idx != 0; idx = c.dlnk[idx][next] {
		el := c.m[idx-1]
		fmt.Printf("IDXX %+v\n", idx)
		fmt.Printf("ohhh %+v\n", el)
		fmt.Printf("ahhh %+v\n", e)
		v := e.Value.(*Elem)
		if el.k != v.key {
			t.Error("case 1.2 failed: ", el.k, v.key)
		}
		if (*(el.v)).(string) != v.val {
			t.Error("case 1.3 failed: ", (*(el.v)).(string), v.val)
		}
		e = e.Next()
	}

}

func BenchmarkMemHash(b *testing.B) {
	buf := make([]byte, 64)
	rand.Read(buf)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hash(string(buf))
	}
	b.SetBytes(int64(len(buf)))
}
