/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package chain

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleChain(t *testing.T) {
	var (
		count = 0
		add   = func() error {
			count++
			return nil
		}
	)

	c := New()
	c.Next(add).
		Next(add).
		Next(add)

	expectedVal := 3
	assert.Equal(t, expectedVal, count)
}

func TestChainWithError(t *testing.T) {
	var (
		count = 0
		add   = func() error {
			count++
			return nil
		}
		throwError = func() error {
			return errors.New("error occured")
		}
	)

	c := New()
	c.Next(add). //+1
			Next(add).        // +1
			Next(throwError). // will interupt chain
			Next(add).        // should not execute
			Next(add).        // should not execute
			Next(add)         // should not execute

	expectedVal := 2
	assert.Equal(t, expectedVal, count)
	assert.Equal(t, c.Error(), throwError())
}

func TestChainWithString(t *testing.T) {
	var (
		word    = "FoOBar"
		toLower = func() error {
			word = strings.ToLower(word)
			return nil
		}
		toUpper = func() error {
			word = strings.ToUpper(word)
			return nil
		}
	)

	c := New()
	c.Next(toLower).Next(toUpper)

	expectedVal := "FOOBAR"
	assert.Equal(t, expectedVal, word)
	assert.Nil(t, c.Error())
}

func TestChainerWithoutError(t *testing.T) {
	var (
		count = 0
		add   = func() error {
			count++
			return nil
		}
	)

	t.Run("without error", func(t *testing.T) {
		c := Chainer(add, add, add)()
		expectedVal := 3
		assert.Equal(t, expectedVal, count)
		assert.Nil(t, c.Error())

	})
}

func TestChainerWithError(t *testing.T) {
	var (
		count = 0
		add   = func() error {
			count++
			return nil
		}
		throwError = func() error {
			return errors.New("error occured")
		}
	)

	t.Run("with error", func(t *testing.T) {
		c := Chainer(add, add, add, add, throwError, add, add)()

		expectedVal := 4
		assert.Equal(t, expectedVal, count)
		assert.NotNil(t, c.Error())
	})
}

/*
	Chain with chunked process tests. This is to verify
	that chain can replace the chunked process utility
*/
func TestChunkedProcessRetryableStep(t *testing.T) {
	cp := New()
	callCount := 0
	cp.NextWithRetry(func() error {
		if callCount < 3 {
			callCount++
			return errors.New("foobar")
		}
		return nil
	})

	require.NoError(t, cp.Error())
	require.Equal(t, 3, callCount)
}

func TestChunkedProcessRetryableStepCancel(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	cp := New().WithContext(ctx)

	go func() {
		time.Sleep(time.Millisecond)
		cancelFunc()
	}()

	callCount := 0
	cp.NextWithRetry(func() error {
		if callCount < 3 {
			callCount++
			return errors.New("foobar")
		}
		return nil
	})

	require.Error(t, cp.ctx.Err())
	require.Equal(t, cp.ctx.Err(), cp.Error())
	require.True(t, errors.Is(cp.Error(), context.Canceled))

	cp.Next(func() error {
		require.Fail(t, "Step should not be run")
		return nil
	})
}

func TestChunkedProcessRetryableStepDeadline(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelFunc()

	cp := New().WithContext(ctx)

	cp.NextWithRetry(func() error {
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	require.Error(t, cp.ctx.Err())
	require.Equal(t, cp.ctx.Err(), cp.Error())
	require.True(t, errors.Is(cp.Error(), context.DeadlineExceeded))
	cp.Next(func() error {
		require.Fail(t, "Step should not be run")
		return nil
	})

}

func TestChunkedProcessRetryableStepError(t *testing.T) {
	cp := New()

	errToReturn := errors.New("Test error")
	cp.NextWithRetry(func() error {
		return errToReturn
	})
	require.Equal(t, errToReturn, cp.Error())
	cp.Next(func() error {
		require.Fail(t, "Step should not be run")
		return nil
	})

}

func TestChunkedProcessNoRetryAfterError(t *testing.T) {
	cp := New()

	firstStepCalled := 0
	secondStepCalled := 0

	cp.Next(func() error {
		firstStepCalled++
		return nil
	})
	require.NoError(t, cp.Error())

	cp.Next(func() error {
		secondStepCalled++
		return errors.New("Stopping process")
	})
	require.Error(t, cp.Error())

	cp.Next(func() error {
		require.Fail(t, "This step should not be called")
		return nil
	})

	cp.NextWithRetry(func() error {
		require.Fail(t, "This step should not be called")
		return nil
	})

	require.Equal(t, 1, firstStepCalled)
	require.Equal(t, 1, secondStepCalled)
}
