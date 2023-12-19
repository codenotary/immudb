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

package remoteapp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChunkedProcessRetryableStep(t *testing.T) {
	cp := chunkedProcess{
		ctx:           context.Background(),
		retryMinDelay: time.Millisecond,
		retryMaxDelay: 2 * time.Millisecond,
		retryDelayExp: 2,
		retryJitter:   0,
	}

	callCount := 0
	cp.RetryableStep(func(retries int, delay time.Duration) (bool, error) {
		require.Equal(t, callCount, retries)
		require.True(t, delay >= time.Millisecond)
		require.True(t, delay <= 2*time.Millisecond)
		if callCount < 3 {
			callCount++
			return true, nil
		}
		return false, nil
	})
	require.NoError(t, cp.Err())

	require.Equal(t, 3, callCount)
}

func TestChunkedProcessRetryableStepCancel(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	cp := chunkedProcess{
		ctx:           ctx,
		retryMinDelay: time.Second,
		retryMaxDelay: 2 * time.Second,
		retryDelayExp: 2,
		retryJitter:   0,
	}

	go func() {
		time.Sleep(time.Millisecond)
		cancelFunc()
	}()
	cp.RetryableStep(func(retries int, delay time.Duration) (bool, error) {
		return true, nil
	})
	require.Error(t, cp.ctx.Err())
	require.Equal(t, cp.ctx.Err(), cp.Err())
	require.True(t, errors.Is(cp.Err(), context.Canceled))

	cp.Step(func() error {
		require.Fail(t, "Step should not be run")
		return nil
	})
}

func TestChunkedProcessRetryableStepDeadline(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelFunc()

	cp := chunkedProcess{
		ctx:           ctx,
		retryMinDelay: time.Second,
		retryMaxDelay: 2 * time.Second,
		retryDelayExp: 2,
		retryJitter:   0,
	}

	cp.RetryableStep(func(retries int, delay time.Duration) (retryNeeded bool, err error) {
		return true, nil
	})
	require.Error(t, cp.ctx.Err())
	require.Equal(t, cp.ctx.Err(), cp.Err())
	require.True(t, errors.Is(cp.Err(), context.DeadlineExceeded))
	cp.Step(func() error {
		require.Fail(t, "Step should not be run")
		return nil
	})

}

func TestChunkedProcessRetryableStepError(t *testing.T) {
	cp := chunkedProcess{
		ctx:           context.Background(),
		retryMinDelay: time.Second,
		retryMaxDelay: 2 * time.Second,
		retryDelayExp: 2,
		retryJitter:   0,
	}

	errToReturn := errors.New("Test error")
	cp.RetryableStep(func(retries int, delay time.Duration) (bool, error) {
		return true, errToReturn
	})
	require.Equal(t, errToReturn, cp.Err())
	cp.Step(func() error {
		require.Fail(t, "Step should not be run")
		return nil
	})

}

func TestChunkedProcessNoRetryAfterError(t *testing.T) {
	cp := chunkedProcess{
		ctx: context.Background(),
	}

	firstStepCalled := 0
	secondStepCalled := 0

	cp.Step(func() error {
		firstStepCalled++
		return nil
	})
	require.NoError(t, cp.Err())

	cp.Step(func() error {
		secondStepCalled++
		return errors.New("Stopping process")
	})
	require.Error(t, cp.Err())

	cp.Step(func() error {
		require.Fail(t, "This step should not be called")
		return nil
	})

	cp.RetryableStep(func(retries int, delay time.Duration) (retryNeeded bool, err error) {
		require.Fail(t, "This step should not be called")
		return false, nil
	})

	require.Equal(t, 1, firstStepCalled)
	require.Equal(t, 1, secondStepCalled)
}
