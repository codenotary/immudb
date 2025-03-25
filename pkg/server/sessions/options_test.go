/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package sessions

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptions(t *testing.T) {
	op := Options{}

	randSrc := bytes.NewReader([]byte{})

	op.WithMaxSessionAgeTime(time.Second).
		WithSessionGuardCheckInterval(2 * time.Second).
		WithMaxSessionInactivityTime(3 * time.Second).
		WithTimeout(4 * time.Second).
		WithMaxSessions(99).
		WithRandSource(randSrc)

	assert.Equal(t, time.Second, op.MaxSessionAgeTime)
	assert.Equal(t, 2*time.Second, op.SessionGuardCheckInterval)
	assert.Equal(t, 3*time.Second, op.MaxSessionInactivityTime)
	assert.Equal(t, 4*time.Second, op.Timeout)
	assert.Equal(t, 99, op.MaxSessions)
	assert.Equal(t, randSrc, op.RandSource)
}

func TestOptionsValidate(t *testing.T) {
	op := DefaultOptions()
	err := op.Validate()
	require.NoError(t, err)

	for _, op := range []*Options{
		DefaultOptions().WithSessionGuardCheckInterval(0),
		DefaultOptions().WithSessionGuardCheckInterval(-1 * time.Second),
		DefaultOptions().WithMaxSessionInactivityTime(-1 * time.Second),
		DefaultOptions().WithMaxSessionAgeTime(-1 * time.Second),
		DefaultOptions().WithTimeout(-1 * time.Second),
		DefaultOptions().WithMaxSessions(0),
		DefaultOptions().WithMaxSessions(-1),
		DefaultOptions().WithRandSource(nil),
	} {
		t.Run(fmt.Sprintf("%+v", op), func(t *testing.T) {
			err = op.Validate()
			require.ErrorIs(t, err, ErrInvalidOptionsProvided)
		})
	}
}

func TestOptionsNormalize(t *testing.T) {
	opts := DefaultOptions().
		WithMaxSessionAgeTime(0).
		WithMaxSessionInactivityTime(0).
		WithTimeout(0).
		Normalize()

	require.Equal(t, infinity, opts.MaxSessionInactivityTime)
	require.Equal(t, infinity, opts.MaxSessionAgeTime)
	require.Equal(t, infinity, opts.Timeout)
}
