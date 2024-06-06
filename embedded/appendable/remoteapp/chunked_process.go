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
	"math"
	"math/rand"
	"time"
)

type chunkedProcess struct {
	ctx context.Context
	err error

	retryMinDelay time.Duration
	retryMaxDelay time.Duration
	retryDelayExp float64
	retryJitter   float64
}

func (c *chunkedProcess) exponentialBackoff(retries int) time.Duration {
	return time.Duration(
		math.Min(
			float64(c.retryMinDelay)*math.Pow(c.retryDelayExp, float64(retries)),
			float64(c.retryMaxDelay),
		) * (1.0 - rand.Float64()*c.retryJitter),
	)
}

func (c *chunkedProcess) Step(block func() error) {
	if c.err != nil {
		return
	}

	c.err = block()
}

// RetryableStep executes given code block retrying it with an exponential backoff delay if needed
//
// If the process is already in an erroneous state, the block won't execute.
//
// If the `block` function returns an error, the operation won't be retried and that error
// will be stored as the result of the process. Otherwise the value of `retryNeeded`
// indicates whether the opration should be retried or not.
//
// If the context is cancelled when waiting for a retry, the operation won't be retried
// and a corresponding error from the context will be stored as the result of the process
func (c *chunkedProcess) RetryableStep(
	block func(retries int, delay time.Duration) (retryNeeded bool, err error),
) {
	if c.err != nil {
		return
	}

	for retries := 0; ; retries++ {
		delay := c.exponentialBackoff(retries)

		retryNeeded, err := block(retries, delay)
		if err != nil {
			c.err = err
			return
		}

		if !retryNeeded {
			return
		}

		// add delay before the next try
		timer := time.NewTimer(delay)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			c.err = c.ctx.Err()
			return
		case <-timer.C:
		}
	}
}

func (c *chunkedProcess) Err() error {
	return c.err
}
