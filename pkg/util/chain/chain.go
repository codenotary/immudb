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

	"github.com/codenotary/immudb/pkg/util/retry"
)

// Chain provides a way to chain function calls, with support
// for error-handling. It provides a clearer way to chain
// function calls, using the output of the previous function
// as the input of the next one. If any of the function(s) in
// the chain returns an error, the chain is broken, the next
// command in the chain is not executed and the resulting error
// is captured
type Chain struct {
	err       error
	ctx       context.Context
	backoff   retry.Backoff
	retriable retry.Retriable
}

// New returns a Chain with default context set
func New() *Chain {
	ctx := context.Background()
	b := retry.DefaultRetry.WithContext(ctx)
	return &Chain{
		ctx:       ctx,
		backoff:   *b,
		retriable: retry.IsError,
	}
}

func (c *Chain) WithContext(ctx context.Context) *Chain {
	c.ctx = ctx
	c.backoff.WithContext(ctx)
	return c
}

func (c *Chain) WithBackoff(b retry.Backoff) *Chain {
	c.backoff = *b.WithContext(c.ctx)
	return c
}

func (c *Chain) WithRetriable(r retry.Retriable) *Chain {
	c.retriable = r
	return c
}

// Next passes the result of the upstream func to the next func
//
// If any error occured at any point in the chain of events,
// the func will not be executed.
func (c *Chain) Next(fn func() error) *Chain {
	if c.err != nil {
		return c
	}

	select {
	case <-c.ctx.Done():
		c.err = c.ctx.Err()
	default:
		c.err = fn()
	}

	return c
}

// NextWithRetry executes the function with a retrier.
//
// If any error occured at any point in the chain of events,
// the function will not execute.
//
// If the context is cancelled, the operation will halt and return.
func (c *Chain) NextWithRetry(fn func() error) *Chain {
	if c.err != nil {
		return c
	}

	c.err = retry.RetryOn(c.backoff, c.retriable, func() error {
		return fn()
	})

	return c
}

// Error returns the last seen error in the chain
func (c *Chain) Error() error {
	return c.err
}

// Chainer chains multiple funcs
func Chainer(fns ...func() error) func() (c *Chain) {
	return func() (c *Chain) {
		c = New()
		for _, fn := range fns {
			c = c.NextWithRetry(fn)
		}

		return c
	}
}
