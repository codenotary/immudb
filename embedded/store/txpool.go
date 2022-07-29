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
	poolSize     int
	maxTxEntries int
	maxKeyLen    int
	preallocated bool
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

	if opts.poolSize <= 0 || opts.maxTxEntries <= 0 || opts.maxKeyLen <= 0 {
		return nil, ErrIllegalArguments
	}

	ret := &txPool{
		pool: make([]*Tx, 0, opts.poolSize),
		opts: opts,
	}

	if opts.preallocated {
		// The pool uses only 5 allocations in total
		// instead of allocating data separately for each tx and then each
		// entry, we instead allocate single large arrays large enough
		// to fulfill requirements of all internal objects
		//
		// Keeping the number of allocated objects small is essential
		// for reducing the CPU time spent in GC cycles.

		txBuffer := make([]Tx, opts.poolSize)
		txEntryPtrBuffer := make([]*TxEntry, opts.poolSize*opts.maxTxEntries)
		txEntryBuffer := make([]TxEntry, opts.poolSize*opts.maxTxEntries)
		headerBuffer := make([]TxHeader, opts.poolSize)
		keyBuffer := make([]byte, opts.poolSize*opts.maxTxEntries*opts.maxKeyLen)

		for i := 0; i < opts.poolSize; i++ {
			tx := &txBuffer[i]
			ret.pool = append(ret.pool, tx)

			tx.entries = txEntryPtrBuffer[:opts.maxTxEntries]
			tx.htree, _ = htree.New(opts.maxTxEntries)
			tx.header = &headerBuffer[i]

			for j := 0; j < opts.maxTxEntries; j++ {
				entry := &txEntryBuffer[j]
				tx.entries[j] = entry

				entry.k = keyBuffer[:opts.maxKeyLen]
				keyBuffer = keyBuffer[opts.maxKeyLen:]
			}

			txEntryPtrBuffer = txEntryPtrBuffer[opts.maxTxEntries:]
			txEntryBuffer = txEntryBuffer[opts.maxTxEntries:]
		}

	}

	return ret, nil
}

func (p *txPool) Alloc() (*Tx, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.used == len(p.pool) {
		if p.used >= p.opts.poolSize {
			return nil, ErrMaxConcurrencyLimitExceeded
		}

		p.pool = append(p.pool, newTx(p.opts.maxTxEntries, p.opts.maxKeyLen))
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

	return p.used, len(p.pool) - p.used, p.opts.poolSize
}
