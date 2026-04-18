/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

package multiapp

import (
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/cache"
)

// refCountedApp wraps an appendable so that cache eviction's Close()
// is *deferred* until no goroutine still holds the handle. Without
// this, a reader returned by appendableCache.Get() can race with a
// concurrent Put() that evicts and Close()s its handle, surfacing as
// `singleapp: already closed` mid-read. Refcounting makes that race
// impossible: Get/Pop return a handle with an extra ref; the read
// path must Release() when done; Close marks evicted and only
// performs the underlying Close when refs reach zero.
//
// Implements appendable.Appendable by delegating, so callers see no
// interface change beyond the new Release().
type refCountedApp struct {
	appendable.Appendable

	mu      sync.Mutex
	refs    int
	evicted bool
}

// Acquire bumps the refcount. Caller must Release exactly once.
// Returns false if the underlying app has already been closed (i.e.
// evicted with refs==0 since this handle was obtained); the handle
// is unusable in that case.
func (r *refCountedApp) Acquire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.evicted && r.refs == 0 {
		return false
	}
	r.refs++
	return true
}

// Release drops one ref. If the cache previously asked us to close
// (Close called by eviction) and we're now the last holder, perform
// the underlying Close.
func (r *refCountedApp) Release() error {
	r.mu.Lock()
	r.refs--
	doClose := r.evicted && r.refs == 0
	r.mu.Unlock()
	if doClose {
		return r.Appendable.Close()
	}
	return nil
}

// Close is what the cache's eviction path calls. We don't actually
// close the file here unless no reader still holds the handle —
// otherwise we mark it evicted and the last Release will perform the
// real close. On a double-Close we forward to the underlying so the
// caller sees the underlying ErrAlreadyClosed (preserves the
// behaviour expected by TestMultiAppClosedAndDeletedFiles etc).
func (r *refCountedApp) Close() error {
	r.mu.Lock()
	if r.evicted {
		r.mu.Unlock()
		return r.Appendable.Close()
	}
	r.evicted = true
	doClose := r.refs == 0
	r.mu.Unlock()
	if doClose {
		return r.Appendable.Close()
	}
	return nil
}

type appendableCache struct {
	cache *cache.Cache
}

// Put stores value under key. Returns the (possibly nil) evicted
// entry. The evicted entry is a *refCountedApp; the caller closes it
// (which is now eviction-safe via the refcount).
func (c appendableCache) Put(key int64, value appendable.Appendable) (int64, appendable.Appendable, error) {
	wrapped := &refCountedApp{Appendable: value}
	k, v, err := c.cache.Put(key, wrapped)
	rkey, _ := k.(int64)
	rvalue, _ := v.(appendable.Appendable)
	return rkey, rvalue, err
}

// Get returns the cached entry with one extra ref held. Caller MUST
// type-assert to *refCountedApp and call .Release() when done.
// Returns ErrKeyNotFound (via the underlying cache) on miss.
func (c appendableCache) Get(key int64) (appendable.Appendable, error) {
	v, err := c.cache.Get(key)
	if err != nil {
		return nil, err
	}
	r := v.(*refCountedApp)
	if !r.Acquire() {
		// race: between cache.Get and Acquire the entry was evicted
		// and its underlying file already closed. Return as miss so
		// the caller refetches.
		return nil, cache.ErrKeyNotFound
	}
	return r, nil
}

// Pop removes the entry from the cache and returns it. Treated as a
// transfer of ownership: caller controls the (still ref-counted)
// handle and is responsible for closing it.
func (c appendableCache) Pop(key int64) (appendable.Appendable, error) {
	v, err := c.cache.Pop(key)
	rvalue, _ := v.(appendable.Appendable)
	return rvalue, err
}

// Replace swaps the entry for a new one. The old entry's refcount
// continues to govern when its file is actually closed.
func (c appendableCache) Replace(key int64, value appendable.Appendable) (appendable.Appendable, error) {
	wrapped := &refCountedApp{Appendable: value}
	v, err := c.cache.Replace(key, wrapped)
	rvalue, _ := v.(appendable.Appendable)
	return rvalue, err
}

// Apply iterates entries. The callback sees the underlying
// appendable.Appendable (via the wrapper). No refs are taken — the
// callback runs while the cache holds the entry, which is safe for
// short-lived inspection (callers like Sync/Close that already hold
// the multiapp mutex prevent concurrent eviction).
func (c appendableCache) Apply(fun func(k int64, v appendable.Appendable) error) error {
	return c.cache.Apply(func(k, v interface{}) error {
		return fun(k.(int64), v.(appendable.Appendable))
	})
}
