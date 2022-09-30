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
	"time"
)

var (
	// DefaultRetry is backoff with jitter retrier strategy.
	DefaultRetry = Backoff{
		Steps:   5,
		retrier: NewExponentialRetrier(2*time.Millisecond, 10*time.Millisecond, 2.0, 0),
	}

	// DefaultConstant is backoff with constant retrier strategy.
	DefaultConstant = Backoff{
		Steps:   5,
		retrier: NewConstantRetrier(10 * time.Millisecond),
	}
)

var (
	// ErrRetryAgain is an error used to retry on a fail condition
	ErrRetryAgain = errors.New("retry again")
	// ErrNoRetry is an error used to not retry on error
	ErrNoRetry = errors.New("do not retry on error")
)

// Retriable returns true if the error has to be retried
type Retriable func(error) bool

// Backoff holds parameters applied to a Backoff function.
type Backoff struct {
	Steps   int
	retrier Retrier
	ctx     context.Context
}

func (b *Backoff) WithContext(ctx context.Context) *Backoff {
	b.ctx = ctx
	return b
}

// Step returns an amount of time to sleep based on the provided Backoff.
func (b *Backoff) Step() time.Duration {
	b.Steps--
	duration := b.retrier.NextInterval(b.Steps)
	return duration
}

func NewBackoff(step int, retrier Retrier) Backoff {
	return Backoff{
		Steps:   step,
		retrier: retrier,
	}
}

// retryFn retries the fn if an error is returned by the fn and if it is a retriable
// error. backoff defines the maximum retries and the wait interval between two retries.
func retryFn(backoff Backoff, retriable Retriable, fn func() (err error)) error {
	var lastErr error
	errCh := make(chan error)

LOOP:
	for backoff.Steps > 0 {
		go func(ch chan error) {
			ch <- fn()
		}(errCh)

		select {
		case err := <-errCh:
			switch {
			case err == nil: // return if the error is nil
				return nil
			case retriable(err): // if a retriable error, continue
				lastErr = err
			default: // if err is not handled by retriable, return without retrying
				return nil
			}
			if backoff.Steps == 1 {
				break LOOP
			}
			time.Sleep(backoff.Step())
		case <-backoff.ctx.Done():
			return backoff.ctx.Err()
		}
	}
	return lastErr
}

// OnError retries a function with specified backoff until the
// function executes without any error
func OnError(backoff Backoff, fn func() error) error {
	err := retryFn(backoff, IsError, fn)
	return err
}

// RetryOn retries a function when the retriable function returns true
func RetryOn(backoff Backoff, retriable Retriable, fn func() error) error {
	return retryFn(backoff, retriable, fn)
}

// IsError checks if the err is not nil.
func IsError(err error) bool {
	if err != nil {
		return true
	}
	return false
}
