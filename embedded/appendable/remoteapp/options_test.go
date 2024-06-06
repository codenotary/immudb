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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInvalidOptions(t *testing.T) {
	require.False(t, (*Options)(nil).Valid())
	require.False(t, (&Options{}).Valid())
}

func TestDefaultOptions(t *testing.T) {
	require.True(t, DefaultOptions().Valid())
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
