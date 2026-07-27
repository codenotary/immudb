//go:build wasip1

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

package main

import (
	"context"
	"errors"
	"io"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
)

// engine is one open immudb store (plus a lazily-created SQL engine), addressed
// by an integer handle from the host.
type engine struct {
	st        *store.ImmuStore
	sqlEngine *sql.Engine
	txPool    store.TxPool
}

var (
	engines    = map[int32]*engine{}
	nextHandle int32
	ctx        = context.Background()
)

// Reserved key-space prefixes. Multi-indexing (required by the SQL engine)
// means each key space must have its own index, so plain KV entries live under
// kvPrefix and the SQL engine manages its own data under sqlPrefix.
var (
	kvPrefix  = []byte{0x01}
	sqlPrefix = []byte{0x00, 's', 'q', 'l'}
)

// kvKey namespaces a user key into the KV index space.
func kvKey(key []byte) []byte {
	return append(append([]byte{}, kvPrefix...), key...)
}

// maxValueLen is the per-value limit the store is created with. immudb's default
// is 4 KB, small enough that an ordinary document is rejected with an opaque
// "max value length exceeded". The limit is written into store metadata at
// creation and is authoritative on every later open, so raising it here only
// affects newly created stores; existing ones keep the limit they were created
// with.
//
// Keys are not raised: store.MaxKeyLen (1024) is a hard ceiling in the engine,
// and kvKey prepends one prefix byte, so a user key is capped at 1023 bytes.
// Both limits are documented in README.md ("Notes, limits, and trade-offs").
const maxValueLen = 1 << 20

// openStore opens (creating if needed) the immudb store at path with the
// multi-indexing required by the SQL engine, initializes the KV index, and
// registers a handle.
func openStore(path string) (int32, error) {
	opts := store.DefaultOptions().
		WithMultiIndexing(true).
		WithMaxValueLen(maxValueLen).
		WithLogger(logger.NewSimpleLoggerWithLevel("immudb-wasm", io.Discard, logger.LogError))

	st, err := store.Open(path, opts)
	if err != nil {
		return 0, err
	}

	// Index the KV key space so Get/Scan/VerifiedGet can resolve plain keys.
	if err := st.InitIndexing(&store.IndexSpec{SourcePrefix: kvPrefix, TargetPrefix: kvPrefix}); err != nil {
		st.Close()
		return 0, err
	}

	pool, err := st.NewTxHolderPool(1, false)
	if err != nil {
		st.Close()
		return 0, err
	}

	nextHandle++
	h := nextHandle
	engines[h] = &engine{st: st, txPool: pool}
	return h, nil
}

func lookup(handle int32) (*engine, error) {
	e, ok := engines[handle]
	if !ok {
		return nil, errBadHandleErr
	}
	return e, nil
}

func closeStore(handle int32) error {
	e, ok := engines[handle]
	if !ok {
		return errBadHandleErr
	}
	delete(engines, handle)
	return e.st.Close()
}

// set writes a single key/value in its own transaction and returns the tx id.
func (e *engine) set(key, value []byte) (uint64, error) {
	tx, err := e.st.NewWriteOnlyTx(ctx)
	if err != nil {
		return 0, err
	}
	if err := tx.Set(kvKey(key), nil, value); err != nil {
		tx.Cancel()
		return 0, err
	}
	hdr, err := tx.Commit(ctx)
	if err != nil {
		return 0, err
	}
	// Indexing is asynchronous; wait so a subsequent Get/Scan sees this write
	// (the ABI presents a synchronous, read-your-writes API).
	if err := e.st.WaitForIndexingUpto(ctx, hdr.ID); err != nil {
		return 0, err
	}
	return hdr.ID, nil
}

// get resolves the latest value for key. Returns (nil, 0, false, nil) when the
// key does not exist.
func (e *engine) get(key []byte) (value []byte, txID uint64, found bool, err error) {
	valRef, err := e.st.Get(ctx, kvKey(key))
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	v, err := valRef.Resolve()
	if err != nil {
		return nil, 0, false, err
	}
	return v, valRef.Tx(), true, nil
}

type scanEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
	Tx    uint64 `json:"tx"`
}

// scan returns up to limit entries whose key starts with prefix, in ascending
// key order.
func (e *engine) scan(prefix []byte, limit int) ([]scanEntry, error) {
	tx, err := e.st.NewTx(ctx, store.DefaultTxOptions().WithMode(store.ReadOnlyTx))
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	reader, err := tx.NewKeyReader(store.KeyReaderSpec{Prefix: kvKey(prefix)})
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var out []scanEntry
	for limit <= 0 || len(out) < limit {
		key, valRef, rerr := reader.Read(ctx)
		if errors.Is(rerr, store.ErrNoMoreEntries) {
			break
		}
		if rerr != nil {
			return nil, rerr
		}
		v, verr := valRef.Resolve()
		if verr != nil {
			return nil, verr
		}
		// Strip the internal KV prefix and copy (the reader reuses the buffer).
		userKey := key[len(kvPrefix):]
		k := make([]byte, len(userKey))
		copy(k, userKey)
		out = append(out, scanEntry{Key: k, Value: v, Tx: valRef.Tx()})
	}
	return out, nil
}
