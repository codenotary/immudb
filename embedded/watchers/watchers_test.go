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

package watchers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWatchersHub(t *testing.T) {
	waitessCount := 1_000

	wHub := New(0, waitessCount*2)

	wHub.DoneUpto(0)

	err := wHub.RecedeTo(1)
	require.ErrorIs(t, err, ErrIllegalState)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = wHub.WaitFor(ctx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	doneUpto, waiting, err := wHub.Status()
	require.NoError(t, err)
	require.Equal(t, uint64(0), doneUpto)
	require.Equal(t, 0, waiting)

	var wg sync.WaitGroup
	wg.Add(waitessCount * 2)

	for it := 0; it < 2; it++ {
		for i := 1; i <= waitessCount; i++ {
			go func(i uint64) {
				defer wg.Done()
				err := wHub.WaitFor(context.Background(), i)
				require.NoError(t, err)
			}(uint64(i))
		}
	}

	time.Sleep(10 * time.Millisecond)

	err = wHub.WaitFor(context.Background(), uint64(waitessCount*2+1))
	require.ErrorIs(t, err, ErrMaxWaitessLimitExceeded)

	done := make(chan struct{})

	go func(done <-chan struct{}) {
		id := uint64(1)

		for {
			select {
			case <-time.Tick(1 * time.Millisecond):
				{
					err := wHub.DoneUpto(id + 2)
					require.NoError(t, err)
					id++
				}
			case <-done:
				{
					return
				}
			}
		}
	}(done)

	wg.Wait()

	done <- struct{}{}

	if t.Failed() {
		t.FailNow()
	}

	err = wHub.WaitFor(context.Background(), 5)
	require.NoError(t, err)

	err = wHub.RecedeTo(5)
	require.NoError(t, err)

	wg.Add(1)

	go func() {
		defer wg.Done()
		err := wHub.WaitFor(context.Background(), uint64(waitessCount)+1)
		if !errors.Is(err, ErrAlreadyClosed) {
			require.NoError(t, err)
		}
	}()

	time.Sleep(1 * time.Millisecond)

	err = wHub.Close()
	require.NoError(t, err)

	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	err = wHub.WaitFor(context.Background(), 0)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = wHub.DoneUpto(0)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = wHub.RecedeTo(0)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, _, err = wHub.Status()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = wHub.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestSimultaneousCancellationAndNotification(t *testing.T) {
	wHub := New(0, 30)

	const maxIterations = 100

	wg := sync.WaitGroup{}
	// Spawn waitees
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			for j := uint64(0); j < maxIterations; j++ {
				func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
					defer cancel()

					doneUpTo, _, err := wHub.Status()
					require.NoError(t, err)

					err = wHub.WaitFor(ctx, j)
					if errors.Is(err, context.DeadlineExceeded) {
						// Check internal invariant of the wHub
						// Since we got cancel request it must only happen
						// as long as we did not already cross the waiting point
						require.Less(t, doneUpTo, j)
					} else {
						require.NoError(t, err)
					}
				}()
			}
		}(i)
	}

	// Producer
	for j := uint64(1); j < maxIterations; j++ {
		wHub.DoneUpto(j)
		time.Sleep(time.Millisecond)
	}

	wg.Wait()

	assert.Zero(t, wHub.waiting)
	assert.Empty(t, wHub.wpoints)
}
