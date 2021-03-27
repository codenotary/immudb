/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

func TestWatchersCenter(t *testing.T) {
	waitessCount := 1_000

	wCenter := New(0, waitessCount*2)

	wCenter.DoneUpto(0)

	var wg sync.WaitGroup
	wg.Add(waitessCount * 2)

	var errHolder error
	var errMutex sync.Mutex

	for it := 0; it < 2; it++ {
		for i := 1; i <= waitessCount; i++ {
			go func(i uint64) {
				err := wCenter.WaitFor(i)
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

	err := wCenter.WaitFor(uint64(waitessCount*2 + 1))
	require.Equal(t, ErrMaxWaitessLimitExceeded, err)

	done := make(chan struct{})

	go func(done <-chan struct{}) {
		t := uint64(1)

		for {
			select {
			case <-time.Tick(1 * time.Millisecond):
				{
					err := wCenter.DoneUpto(t)
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

	err = wCenter.WaitFor(5)
	require.NoError(t, err)

	wg.Add(1)

	go func() {
		err := wCenter.WaitFor(uint64(waitessCount) + 1)
		if err != ErrAlreadyClosed {
			errMutex.Lock()
			errHolder = err
			errMutex.Unlock()
		}
		wg.Done()
	}()

	time.Sleep(1 * time.Millisecond)

	err = wCenter.Close()
	require.NoError(t, err)

	wg.Wait()

	require.NoError(t, errHolder)

	err = wCenter.WaitFor(0)
	require.Equal(t, ErrAlreadyClosed, err)

	err = wCenter.DoneUpto(0)
	require.Equal(t, ErrAlreadyClosed, err)

	err = wCenter.Close()
	require.Equal(t, ErrAlreadyClosed, err)
}
