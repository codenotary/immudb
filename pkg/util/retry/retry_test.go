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

package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetry(t *testing.T) {
	opts := Backoff{
		Steps:   5,
		retrier: NewExponentialRetrier(2*time.Millisecond, 10*time.Millisecond, 2.0, 0),
		ctx:     context.Background(),
	}

	// retries continuously on error
	i := 0
	expectedErr := errors.New("foobar")
	err := OnError(opts, func() error {
		i++
		return expectedErr
	})
	require.Equal(t, expectedErr, err)
	require.Equal(t, 5, i)

	// returns immediately
	i = 0
	err = OnError(opts, func() error {
		i++
		return nil
	})
	if err != nil || i != 1 {
		t.Errorf("unexpected error: %v", err)
	}

	// retries 2 times
	i = 0
	err = OnError(opts, func() error {
		if i < 2 {
			i++
			return errors.New("not found")
		}
		return nil
	})
	if err != nil || i != 2 {
		t.Errorf("unexpected error: %v", err)
	}

	// retries exhausted
	expectedErr = errors.New("foobaz")
	i = 0
	err = OnError(opts, func() error {
		if i < 6 {
			i++
			return expectedErr
		}
		return nil
	})
	if err == nil || i != 5 {
		t.Errorf("unexpected error: %v", err)
	}

	// max retries
	i = 0
	err = OnError(opts, func() error {
		if i < 4 {
			i++
			return expectedErr
		}
		return nil
	})
	if err != nil || i != 4 {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRetryWithContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	opts := Backoff{
		Steps:   5,
		retrier: NewExponentialRetrier(50*time.Millisecond, 1*time.Second, 2.0, 0),
		ctx:     ctx,
	}

	// retries continuously on error
	expectedErr := errors.New("foobar")
	err := OnError(opts, func() error {
		return expectedErr
	})
	require.NotNil(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
}
