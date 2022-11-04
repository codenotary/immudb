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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExponentialRetrierNextInterval(t *testing.T) {
	eb := NewExponentialRetrier(2*time.Millisecond, 10*time.Millisecond, 2.0, 0)
	assert.Equal(t, 4*time.Millisecond, eb.NextInterval(1))
}

func TestExponentialRetrierMaxOverflow(t *testing.T) {
	eb := NewExponentialRetrier(2*time.Millisecond, 10*time.Millisecond, 2.0, 0)
	assert.Equal(t, 10*time.Millisecond, eb.NextInterval(40))
}

func TestExponentialRetrierWithJitter(t *testing.T) {
	var (
		dur time.Duration
		low = 10 * time.Millisecond
	)

	jb := NewExponentialRetrier(low, 3*time.Second, 2.0, 0.5)
	dur = jb.NextInterval(1)
	assert.Equal(t, between(t, dur, low, 20*time.Millisecond), true)
	dur = jb.NextInterval(2)
	assert.Equal(t, between(t, dur, low, 50*time.Millisecond), true)
	dur = jb.NextInterval(3)
	assert.Equal(t, between(t, dur, low, 100*time.Millisecond), true)
	dur = jb.NextInterval(4)
	assert.Equal(t, between(t, dur, low, 200*time.Millisecond), true)
}

func TestConstantRetrierNextInterval(t *testing.T) {
	cb := NewConstantRetrier(10 * time.Millisecond)

	assert.Equal(t, 10*time.Millisecond, cb.NextInterval(1))
	assert.Equal(t, 10*time.Millisecond, cb.NextInterval(10))
}

func between(t *testing.T, val, low, high time.Duration) bool {
	if val < low {
		return false
	}
	if val > high {
		return false
	}
	if val >= low && val <= high {
		return true
	}
	return false
}
