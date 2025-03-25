/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package store

import (
	"sync"
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
		for i := 0; i < opts.poolSize; i++ {
			ret.pool = append(ret.pool, NewTx(opts.maxTxEntries, opts.maxKeyLen))
		}
	}

	return ret, nil
}

func (p *txPool) Alloc() (*Tx, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.used == len(p.pool) {
		if p.used >= p.opts.poolSize {
			return nil, ErrTxPoolExhausted
		}

		p.pool = append(p.pool, NewTx(p.opts.maxTxEntries, p.opts.maxKeyLen))
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
