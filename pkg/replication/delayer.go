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

package replication

import (
	"math"
	"math/rand"
	"time"
)

type Delayer interface {
	DelayAfter(retries int) time.Duration
}

type expBackoff struct {
	retryMinDelay time.Duration
	retryMaxDelay time.Duration
	retryDelayExp float64
	retryJitter   float64
}

func (exp *expBackoff) DelayAfter(retries int) time.Duration {
	return time.Duration(
		math.Min(
			float64(exp.retryMinDelay)*math.Pow(exp.retryDelayExp, float64(retries)),
			float64(exp.retryMaxDelay),
		) * (1.0 - rand.Float64()*exp.retryJitter),
	)
}
