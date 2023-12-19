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

package client

import (
	"context"

	"testing"

	"github.com/codenotary/immudb/pkg/stream"
	"github.com/stretchr/testify/require"
)

func TestImmuClient_Errors(t *testing.T) {
	client := NewClient()
	ctx := context.Background()

	_, err := client.StreamVerifiedSet(ctx, nil)
	require.ErrorContains(t, err, "no key-values specified")

	// test ErrNotConnected errors
	fs := []func() (string, error){
		func() (string, error) { _, err := client.streamSet(ctx); return "streamSet", err },
		func() (string, error) { _, err := client.streamGet(ctx, nil); return "streamGet", err },
		func() (string, error) { _, err := client.streamVerifiableSet(ctx); return "streamVerifiableSet", err },
		func() (string, error) {
			_, err := client.streamVerifiableGet(ctx, nil)
			return "streamVerifiableGet", err
		},
		func() (string, error) { _, err := client.streamScan(ctx, nil); return "streamScan", err },
		func() (string, error) { _, err := client.streamZScan(ctx, nil); return "streamZScan", err },
		func() (string, error) { _, err := client.streamExecAll(ctx); return "streamExecAll", err },
		func() (string, error) { _, err := client.streamHistory(ctx, nil); return "streamHistory", err },
		func() (string, error) { _, err := client.StreamSet(ctx, nil); return "StreamSet", err },
		func() (string, error) { _, err := client.StreamGet(ctx, nil); return "StreamGet", err },
		func() (string, error) {
			_, err := client.StreamVerifiedSet(ctx, []*stream.KeyValue{{}})
			return "StreamVerifiedSet", err
		},
		func() (string, error) { _, err := client.StreamVerifiedGet(ctx, nil); return "StreamVerifiedGet", err },
		func() (string, error) { _, err := client.StreamScan(ctx, nil); return "StreamScan", err },
		func() (string, error) { _, err := client.StreamZScan(ctx, nil); return "StreamZScan", err },
		func() (string, error) { _, err := client.StreamHistory(ctx, nil); return "StreamHistory", err },
		func() (string, error) { _, err := client.StreamExecAll(ctx, nil); return "StreamExecAll", err },
	}
	for _, f := range fs {
		fn, err := f()
		require.ErrorIs(t, err, ErrNotConnected, fn)
	}
}
