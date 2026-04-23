/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInvalidOptions(t *testing.T) {
	require.False(t, (*Options)(nil).Valid())
	require.False(t, (&Options{}).Valid())

	require.ErrorIs(t, (*Options)(nil).Validate(), ErrInvalidOptions)
	require.ErrorIs(t, (&Options{}).Validate(), ErrInvalidOptions)
}

func TestDefaultOptions(t *testing.T) {
	require.True(t, DefaultOptions().Valid())
	require.NoError(t, DefaultOptions().Validate())
}

func TestValidateFieldConstraints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(o *Options)
	}{
		{"zero parallel uploads", func(o *Options) { o.parallelUploads = 0 }},
		{"too many parallel uploads", func(o *Options) { o.parallelUploads = 1_000_000 }},
		{"zero retry min delay", func(o *Options) { o.retryMinDelay = 0 }},
		{"zero retry max delay", func(o *Options) { o.retryMaxDelay = 0 }},
		{"max < min retry delay", func(o *Options) { o.retryMinDelay = 10 * time.Second; o.retryMaxDelay = time.Second }},
		{"retry exp <= 1", func(o *Options) { o.retryDelayExp = 1 }},
		{"negative jitter", func(o *Options) { o.retryDelayJitter = -0.1 }},
		{"jitter > 1", func(o *Options) { o.retryDelayJitter = 1.1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := DefaultOptions()
			tc.mutate(o)
			require.ErrorIs(t, o.Validate(), ErrInvalidOptions)
		})
	}
}

func TestValidOptions(t *testing.T) {
	opts := DefaultOptions()

	require.Equal(t, 11, opts.WithParallelUploads(11).parallelUploads)
	require.Equal(t, 3*time.Second, opts.WithRetryMinDelay(3*time.Second).retryMinDelay)
	require.Equal(t, 7*time.Second, opts.WithRetryMaxDelay(7*time.Second).retryMaxDelay)
	require.Equal(t, 1.3, opts.WithRetryDelayExp(1.3).retryDelayExp)
	require.Equal(t, 0.2, opts.WithRetryDelayJitter(0.2).retryDelayJitter)

	require.True(t, opts.Valid())
}
