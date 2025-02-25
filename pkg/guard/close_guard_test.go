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

package guard_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/guard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloseGuard(t *testing.T) {
	var g guard.CloseGuard

	require.True(t, g.Acquire(), "First acquire should succeed")
	require.True(t, g.Acquire(), "Second acquire should succeed")

	g.Release()
	g.Release()

	require.NoError(t, g.Close(nil), "Close should succeed with no active acquisitions")
	assert.False(t, g.Acquire(), "Acquire should fail after Close")
}

func TestCloseGuardConcurrent(t *testing.T) {
	var g guard.CloseGuard

	n := 1000

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			acquired := g.Acquire()
			require.True(t, acquired)

			time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))

			wg.Done()
			g.Release()
		}()
	}
	wg.Wait()

	assert.NoError(t, g.Close(nil), "Close should succeed when no outstanding acquisitions remain")
	assert.ErrorIs(t, g.Close(nil), guard.ErrAlreadyClosed, "Close should fail")
	assert.False(t, g.Acquire(), "Acquire should fail after Close")
}

func TestCloseGuardWhileActive(t *testing.T) {
	var g guard.CloseGuard

	require.True(t, g.Acquire(), "Initial acquire should succeed")

	go func() {
		time.Sleep(time.Millisecond * 100)
		g.Release()
	}()

	assert.NoError(t, g.Close(nil), "Close should succeed after guard is released")
}
