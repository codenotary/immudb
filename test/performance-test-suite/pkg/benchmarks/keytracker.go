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

package benchmarks

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
)

type KeyTracker struct {
	start uint64
	max   uint64
}

func NewKeyTracker(start uint64) *KeyTracker {
	return &KeyTracker{
		start: start,
	}
}

func (kt *KeyTracker) GetWKey() string {
	max := atomic.AddUint64(&kt.max, 1)
	return fmt.Sprintf("KEY:%010d", max+kt.start-1)
}

// GetRKey returns a uniformly-random key over [start, start+max). Use
// GetRKeyZipf when a workload with locality (hot keys accessed often) is
// more realistic — real traffic is almost never uniform and benchmarking
// with uniform-only reads masks cache and index behaviour.
func (kt *KeyTracker) GetRKey() string {
	max := atomic.LoadUint64(&kt.max)
	k := kt.start
	if max > 0 {
		k += rand.Uint64() % max
	}
	return fmt.Sprintf("KEY:%010d", k)
}

// ZipfKeyTracker wraps KeyTracker with a Zipf-distributed read selector so
// a small fraction of keys receive a disproportionate share of reads —
// the "80/20 rule" that characterises most real production workloads.
// Writes still go through GetWKey on the embedded KeyTracker, so put and
// get load can be paired in the same benchmark.
//
// s (>1) controls skew: larger s = heavier concentration on low-indexed
// (hot) keys. A typical setting is s=1.1 for mild skew and s=2.0 for
// aggressive. v is the Zipf shift; keep at 1 for standard Zipf.
type ZipfKeyTracker struct {
	*KeyTracker

	mu   sync.Mutex
	rng  *rand.Rand
	zipf *rand.Zipf
	imax uint64
	s, v float64
}

// NewZipfKeyTracker builds a tracker that generates Zipf-distributed read
// keys. The Zipf needs a finite imax, so callers must provide an expected
// maximum key count; reads beyond the current written max simply wrap via
// modulo, preserving the distribution shape.
func NewZipfKeyTracker(start uint64, imax uint64, s, v float64, seed int64) *ZipfKeyTracker {
	rng := rand.New(rand.NewSource(seed))
	return &ZipfKeyTracker{
		KeyTracker: NewKeyTracker(start),
		rng:        rng,
		zipf:       rand.NewZipf(rng, s, v, imax),
		imax:       imax,
		s:          s,
		v:          v,
	}
}

// GetRKeyZipf returns a Zipf-distributed key. rand.Zipf is not safe for
// concurrent use so we guard access with a mutex; for a benchmark that
// needs lock-free reads, create one tracker per worker goroutine.
func (kt *ZipfKeyTracker) GetRKeyZipf() string {
	max := atomic.LoadUint64(&kt.KeyTracker.max)
	if max == 0 {
		return fmt.Sprintf("KEY:%010d", kt.start)
	}

	kt.mu.Lock()
	raw := kt.zipf.Uint64()
	kt.mu.Unlock()

	return fmt.Sprintf("KEY:%010d", kt.start+(raw%max))
}
