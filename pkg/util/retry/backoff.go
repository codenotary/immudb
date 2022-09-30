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
	"math"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Retrier interface {
	NextInterval(retry int) time.Duration
}

type exponentialRetrier struct {
	factor       float64
	jitterFactor float64
	minTimeout   time.Duration
	maxTimeout   time.Duration
}

// NewExponentialRetrier returns an instance of ExponentialRetrier
func NewExponentialRetrier(
	minTimeout, maxTimeout time.Duration,
	exponentFactor, jitterFactor float64,
) Retrier {
	return &exponentialRetrier{
		factor:       exponentFactor,
		minTimeout:   minTimeout,
		maxTimeout:   maxTimeout,
		jitterFactor: jitterFactor,
	}
}

// NextInterval returns the next time interval to retrying the operation
func (e *exponentialRetrier) NextInterval(retryCount int) time.Duration {
	if retryCount <= 0 {
		return 0 * time.Millisecond
	}

	efac := math.Pow(e.factor, float64(retryCount)) * float64(e.minTimeout)
	sleep := math.Min(efac, float64(e.maxTimeout)) * (1.0 - rand.Float64()*e.jitterFactor)

	return time.Duration(sleep)
}

type constantRetrier struct {
	timeout time.Duration
}

// NewConstanctRetrier returns an instance of ConstantRetrier
func NewConstantRetrier(timeout time.Duration) Retrier {
	return &constantRetrier{
		timeout: timeout,
	}
}

// NextInterval returns the next time interval to retrying the operation
func (cb *constantRetrier) NextInterval(retryCount int) time.Duration {
	if retryCount <= 0 {
		return 10 * time.Millisecond
	}

	return cb.timeout
}
