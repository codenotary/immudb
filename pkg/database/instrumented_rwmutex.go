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

package database

import (
	"sync"
	"time"
)

type instrumentedRWMutex struct {
	rwmutex sync.RWMutex

	trwmutex      sync.RWMutex
	waitingCount  int
	lastReleaseAt time.Time
}

func (imux *instrumentedRWMutex) State() (waitingCount int, lastReleaseAt time.Time) {
	imux.trwmutex.RLock()
	defer imux.trwmutex.RUnlock()

	return imux.waitingCount, imux.lastReleaseAt
}

func (imux *instrumentedRWMutex) Lock() {
	imux.trwmutex.Lock()
	imux.waitingCount++
	imux.trwmutex.Unlock()

	imux.rwmutex.Lock()

	imux.trwmutex.Lock()
	imux.waitingCount--
	imux.trwmutex.Unlock()
}

func (imux *instrumentedRWMutex) Unlock() {
	imux.trwmutex.Lock()

	imux.rwmutex.Unlock()
	imux.lastReleaseAt = time.Now()

	imux.trwmutex.Unlock()
}

func (imux *instrumentedRWMutex) RLock() {
	imux.trwmutex.Lock()
	imux.waitingCount++
	imux.trwmutex.Unlock()

	imux.rwmutex.RLock()

	imux.trwmutex.Lock()
	imux.waitingCount--
	imux.trwmutex.Unlock()
}

func (imux *instrumentedRWMutex) RUnlock() {
	imux.trwmutex.Lock()

	imux.rwmutex.RUnlock()
	imux.lastReleaseAt = time.Now()

	imux.trwmutex.Unlock()
}
