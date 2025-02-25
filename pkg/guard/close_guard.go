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

package guard

import (
	"errors"
	"runtime"
	"sync/atomic"
)

var ErrAlreadyClosed = errors.New("already closed")

type CloseGuard struct {
	w atomic.Uint64
}

// Acquire attempts to acquire the guard. Returns true if successful, false if closed.
func (g *CloseGuard) Acquire() bool {
	v := g.w.Add(0x2)
	if v&0x1 != 0 {
		g.Release()
		return false
	}
	return true
}

// Releases a previously acquired guard. Releasing a non acquired guard will result in non defined behavior
func (g *CloseGuard) Release() {
	g.w.Add(^uint64(1))
}

// Close attempts to close the guard. Returns nil if successful or an error if active acquisitions exist.
// Once Close() returns nil, further Acquire() calls will fail.
func (g *CloseGuard) Close(run func() error) error {
	if !g.markClosed() {
		return ErrAlreadyClosed
	}

	if run != nil {
		if err := run(); err != nil {
			g.unmarkClosed()
			return err
		}
	}

	for (g.w.Load() >> 1) != 0 {
		runtime.Gosched()
	}
	return nil
}

func (g *CloseGuard) markClosed() bool {
	for {
		v := g.w.Load()
		if v&0x1 != 0 {
			return false
		}

		if g.w.CompareAndSwap(v, v|0x1) {
			return true
		}
	}
}

func (g *CloseGuard) unmarkClosed() {
	for {
		v := g.w.Load()
		if v&0x1 == 0 {
			panic("close mark not set")
		}

		if g.w.CompareAndSwap(v, v^1) {
			return
		}
	}
}

func (g *CloseGuard) Closed() bool {
	return g.w.Load()&0x1 != 0
}
