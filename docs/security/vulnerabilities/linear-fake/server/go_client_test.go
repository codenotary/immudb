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
package main_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	main "linear-fake"

	immudb "github.com/codenotary/immudb/pkg/client"

	"github.com/stretchr/testify/require"
)

// Simple test routine for checking PoC vulnerability code
func TestServer(t *testing.T) {
	srv, err := main.GetFakeServer(t.TempDir(), 3322)
	require.NoError(t, err)

	go func() {
		require.NoError(t, srv.Start())
	}()
	defer srv.Stop()

	defer func() {
		list, err := filepath.Glob(".state-*")
		require.NoError(t, err)
		for _, e := range list {
			err := os.Remove(e)
			require.NoError(t, err)
		}
	}()

	opts := immudb.DefaultOptions().
		WithAddress("localhost").
		WithPort(3322)

	ctx := context.Background()

	client := immudb.NewClient().WithOptions(opts)
	err = client.OpenSession(ctx, []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	defer client.CloseSession(ctx)

	// get correct entries at TX 2
	tx2, err := client.VerifiedTxByID(ctx, 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, tx2.Header.Id)
	require.Len(t, tx2.GetEntries(), 1)
	require.Equal(t, "valid-key-0", string(tx2.GetEntries()[0].GetKey()))

	// transition to TX 3, verify consistency between TX 2 and TX 3
	tx3, err := client.VerifiedTxByID(ctx, 3)
	require.NoError(t, err)
	require.EqualValues(t, 3, tx3.Header.Id)
	require.Len(t, tx3.GetEntries(), 1)
	require.Equal(t, "valid-key-1", string(tx3.GetEntries()[0].GetKey()))

	// transition to TX 5, verify consistency between TX3 and TX 5
	tx5, err := client.VerifiedTxByID(ctx, 5)
	require.NoError(t, err)
	require.EqualValues(t, 5, tx5.Header.Id)
	require.Len(t, tx5.GetEntries(), 1)
	require.Equal(t, "key-after-1", string(tx5.GetEntries()[0].GetKey()))

	// verified get of TX2 with known state at TX 5 returning fake data
	tx2Fake, err := client.VerifiedTxByID(ctx, 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, tx2.Header.Id)
	require.Len(t, tx2Fake.GetEntries(), 1)
	require.Equal(t, "fake-key", string(tx2Fake.GetEntries()[0].GetKey()))
}
