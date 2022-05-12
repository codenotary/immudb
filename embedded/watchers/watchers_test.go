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
package watchers

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWatchersHub(t *testing.T) {
	waitessCount := 1_000

	wHub := New(0, waitessCount*2)

	wHub.DoneUpto(0)

	cancellation := make(chan struct{}, 0)

	go func(cancel chan<- struct{}) {
		time.Sleep(100 * time.Millisecond)
		close(cancel)
	}(cancellation)

	err := wHub.WaitFor(1, cancellation)
	require.ErrorIs(t, err, ErrCancellationRequested)

	doneUpto, waiting, err := wHub.Status()
	require.NoError(t, err)
	require.Equal(t, uint64(0), doneUpto)
	require.Equal(t, 0, waiting)

	var wg sync.WaitGroup
	wg.Add(waitessCount * 2)

	var errHolder error
	var errMutex sync.Mutex

	for it := 0; it < 2; it++ {
		for i := 1; i <= waitessCount; i++ {
			go func(i uint64) {
				err := wHub.WaitFor(i, nil)
				if err != nil {
					errMutex.Lock()
					errHolder = err
					errMutex.Unlock()
				}
				wg.Done()
			}(uint64(i))
		}
	}

	time.Sleep(10 * time.Millisecond)

	err = wHub.WaitFor(uint64(waitessCount*2+1), nil)
	require.Equal(t, ErrMaxWaitessLimitExceeded, err)

	done := make(chan struct{})

	go func(done <-chan struct{}) {
		t := uint64(1)

		for {
			select {
			case <-time.Tick(1 * time.Millisecond):
				{
					err := wHub.DoneUpto(t + 2)
					if err != nil {
						errMutex.Lock()
						errHolder = err
						errMutex.Unlock()
					}
					t++
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

	require.NoError(t, errHolder)

	err = wHub.WaitFor(5, nil)
	require.NoError(t, err)

	wg.Add(1)

	go func() {
		err := wHub.WaitFor(uint64(waitessCount)+1, nil)
		if err != ErrAlreadyClosed {
			errMutex.Lock()
			errHolder = err
			errMutex.Unlock()
		}
		wg.Done()
	}()

	time.Sleep(1 * time.Millisecond)

	err = wHub.Close()
	require.NoError(t, err)

	wg.Wait()

	require.NoError(t, errHolder)

	err = wHub.WaitFor(0, nil)
	require.Equal(t, ErrAlreadyClosed, err)

	err = wHub.DoneUpto(0)
	require.Equal(t, ErrAlreadyClosed, err)

	_, _, err = wHub.Status()
	require.Equal(t, ErrAlreadyClosed, err)

	err = wHub.Close()
	require.Equal(t, ErrAlreadyClosed, err)
}
