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

package memory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	remotestorage "github.com/codenotary/immudb/embedded/remotestorage"
)

var (
	ErrInvalidArguments = errors.New("invalid arguments")
)

// Storage implements a simple in-memory remote storage
type Storage struct {
	mutex   sync.RWMutex
	objects map[string][]byte

	randomPutDelayMinMs, randomPutDelayMaxMs int
}

func Open() *Storage {
	return &Storage{
		objects: map[string][]byte{},
	}
}

func (r *Storage) Kind() string {
	return "memory"
}

func (r *Storage) String() string {
	return fmt.Sprintf("memory(%p):", r)
}

// Get opens a stream of data for given object
func (r *Storage) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	if offs < 0 || size == 0 {
		return nil, ErrInvalidArguments
	}

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	object, exists := r.objects[name]
	if !exists {
		return nil, remotestorage.ErrNotFound
	}
	objectLen := int64(len(object))

	if offs > objectLen {
		offs = objectLen
	}

	if size < 0 || offs+size > objectLen {
		size = objectLen - offs
	}

	return ioutil.NopCloser(bytes.NewReader(object[offs : offs+size])), nil
}

func (r *Storage) randomPutDelay() time.Duration {
	delayMs := r.randomPutDelayMinMs + rand.Intn(r.randomPutDelayMaxMs-r.randomPutDelayMinMs+1)
	return time.Millisecond * time.Duration(delayMs)
}

// Put writes a remote s3 resource
func (r *Storage) Put(ctx context.Context, name string, fileName string) error {
	object, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}

	store := func() {
		r.mutex.Lock()
		r.objects[name] = object
		r.mutex.Unlock()
	}

	delay := r.randomPutDelay()
	if delay > 0 {
		// Asynchronous store
		go func() {
			time.Sleep(delay)
			store()
		}()
	} else {
		// Synchronous store
		store()
	}

	return nil
}

// Exists checks if a remove resource exists and can be read
// Note that due to an asynchronous nature of cluod storage,
// a resource stored with the Put method may not be immediately accessible
func (r *Storage) Exists(ctx context.Context, name string) (bool, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	_, exists := r.objects[name]
	return exists, nil
}

func (r *Storage) ListEntries(ctx context.Context, path string) ([]remotestorage.EntryInfo, []string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if path != "" {
		if !strings.HasSuffix(path, "/") ||
			strings.Contains(path, "//") ||
			path == "/" {
			return nil, nil, ErrInvalidArguments
		}
	}

	subPathSet := map[string]struct{}{}
	entries := []remotestorage.EntryInfo{}

	for k := range r.objects {
		if !strings.HasPrefix(k, path) {
			continue
		}

		kWithoutPath := k[len(path):]
		nextSlash := strings.IndexRune(kWithoutPath, '/')
		if nextSlash == -1 {
			entries = append(entries, remotestorage.EntryInfo{
				Name: kWithoutPath,
				Size: int64(len(r.objects[k])),
			})
		} else {
			subPathSet[kWithoutPath[:nextSlash]] = struct{}{}
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })

	subPaths := []string{}
	for k := range subPathSet {
		subPaths = append(subPaths, k)
	}
	sort.Strings(subPaths)

	return entries, subPaths, nil
}

func (r *Storage) SetRandomPutDelays(minMs, maxMs int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.randomPutDelayMinMs = minMs
	r.randomPutDelayMaxMs = maxMs
}

var _ remotestorage.Storage = (*Storage)(nil)
