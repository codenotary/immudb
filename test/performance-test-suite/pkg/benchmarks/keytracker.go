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

package benchmarks

import (
	"fmt"
	"math/rand"
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

func (kt *KeyTracker) GetRKey() string {
	max := atomic.LoadUint64(&kt.max)
	k := kt.start
	if max > 0 {
		k += rand.Uint64() % max
	}
	return fmt.Sprintf("KEY:%010d", k)
}
