/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"sync"

	"github.com/codenotary/immudb/embedded/htree"
)

type txPoolOptions struct {
	PoolSize     int
	MaxTxEntries int
	MaxKeyLen    int
	Preallocated bool
}

type TxPool interface {
	Alloc() (*Tx, error)
	Release(*Tx)
	Stats() (used, free, max int)
}

type txPool struct {
	pool []*Tx
	used int
	m    sync.Mutex
	opts txPoolOptions
}

func newTxPool(opts txPoolOptions) (TxPool, error) {

	if opts.PoolSize <= 0 || opts.MaxTxEntries <= 0 || opts.MaxKeyLen <= 0 {
		return nil, ErrIllegalArguments
	}

	ret := &txPool{
		pool: make([]*Tx, 0, opts.PoolSize),
		opts: opts,
	}

	if opts.Preallocated {
		// The pool uses only 5 allocations in total
		// instead of allocating data separately for each tx and then each
		// entry, we instead allocate single large arrays large enough
		// to fulfill requirements of all internal objects
		//
		// Keeping the number of allocated objects small is essential
		// for reducing the CPU time spent in GC cycles.

		txBuffer := make([]Tx, opts.PoolSize)
		txEntryPtrBuffer := make([]*TxEntry, opts.PoolSize*opts.MaxTxEntries)
		txEntryBuffer := make([]TxEntry, opts.PoolSize*opts.MaxTxEntries)
		headerBuffer := make([]TxHeader, opts.PoolSize)
		keyBuffer := make([]byte, opts.PoolSize*opts.MaxTxEntries*opts.MaxKeyLen)

		for i := 0; i < opts.PoolSize; i++ {
			tx := &txBuffer[i]
			ret.pool = append(ret.pool, tx)

			tx.entries = txEntryPtrBuffer[:opts.MaxTxEntries]
			tx.htree, _ = htree.New(opts.MaxTxEntries)
			tx.header = &headerBuffer[i]

			for j := 0; j < opts.MaxTxEntries; j++ {
				entry := &txEntryBuffer[j]
				tx.entries[j] = entry

				entry.k = keyBuffer[:opts.MaxKeyLen]
				keyBuffer = keyBuffer[opts.MaxKeyLen:]
			}

			txEntryPtrBuffer = txEntryPtrBuffer[opts.MaxTxEntries:]
			txEntryBuffer = txEntryBuffer[opts.MaxTxEntries:]
		}

	}

	return ret, nil
}

func (p *txPool) Alloc() (*Tx, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.used == len(p.pool) {
		if p.used >= p.opts.PoolSize {
			return nil, ErrMaxConcurrencyLimitExceeded
		}

		p.pool = append(p.pool, newTx(p.opts.MaxTxEntries, p.opts.MaxKeyLen))
	}

	tx := p.pool[p.used]
	p.used++

	return tx, nil
}

func (p *txPool) Release(tx *Tx) {
	p.m.Lock()
	defer p.m.Unlock()

	p.used--
	p.pool[p.used] = tx
}

func (p *txPool) Stats() (used, free, max int) {
	p.m.Lock()
	defer p.m.Unlock()

	return p.used, len(p.pool) - p.used, p.opts.PoolSize
}
