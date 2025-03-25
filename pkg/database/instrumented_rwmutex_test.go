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

package database

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInstrumentedMutex(t *testing.T) {
	mutex := &instrumentedRWMutex{}

	waitingCount, _ := mutex.State()
	require.Equal(t, 0, waitingCount)

	mutex.Lock()

	waitingCount, _ = mutex.State()
	require.Equal(t, 0, waitingCount)

	justBeforeRelease := time.Now()

	time.Sleep(1 * time.Millisecond)

	mutex.Unlock()

	waitingCount, lastReleaseAt := mutex.State()
	require.Equal(t, 0, waitingCount)
	require.True(t, lastReleaseAt.After(justBeforeRelease))

	mutex.Lock()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		wg.Done()

		mutex.RLock()

		time.Sleep(1 * time.Millisecond)

		justBeforeRelease = time.Now()

		time.Sleep(1 * time.Millisecond)

		mutex.RUnlock()

		wg.Done()
	}()

	wg.Wait()

	wg.Add(1)

	waitingCount, _ = mutex.State()
	require.Equal(t, 1, waitingCount)

	mutex.Unlock()

	wg.Wait()

	waitingCount, lastReleaseAt = mutex.State()
	require.Equal(t, 0, waitingCount)
	require.True(t, lastReleaseAt.After(justBeforeRelease))
}
