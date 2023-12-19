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

package integration

import (
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestDatabaseLoadingUnloading(t *testing.T) {
	_, client, ctx := setupTestServerAndClient(t)

	err := client.CloseSession(ctx)
	require.NoError(t, err)

	t.Run("attempt load/unload/delete a database without an active session should fail", func(t *testing.T) {
		_, err := client.LoadDatabase(ctx, &schema.LoadDatabaseRequest{Database: "db1"})
		require.Contains(t, err.Error(), "not connected")

		_, err = client.UnloadDatabase(ctx, &schema.UnloadDatabaseRequest{Database: "db1"})
		require.Contains(t, err.Error(), "not connected")

		_, err = client.DeleteDatabase(ctx, &schema.DeleteDatabaseRequest{Database: "db1"})
		require.Contains(t, err.Error(), "not connected")
	})

	err = client.OpenSession(ctx, []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	dbSettings := &schema.DatabaseSettings{
		DatabaseName:      "db1",
		Replica:           false,
		FileSize:          1 << 20,
		MaxKeyLen:         32,
		MaxValueLen:       64,
		MaxTxEntries:      100,
		ExcludeCommitTime: false,
	}
	err = client.CreateDatabase(ctx, dbSettings)
	require.NoError(t, err)

	_, err = client.UseDatabase(ctx, &schema.Database{DatabaseName: "db1"})
	require.NoError(t, err)

	t.Run("attempt to load unexistent database should fail", func(t *testing.T) {
		_, err := client.LoadDatabase(ctx, &schema.LoadDatabaseRequest{Database: "db2"})
		require.Contains(t, err.Error(), "database does not exist")
	})

	t.Run("attempt to load an already open database should fail", func(t *testing.T) {
		_, err := client.LoadDatabase(ctx, &schema.LoadDatabaseRequest{Database: "db1"})
		require.Contains(t, err.Error(), "database already loaded")
	})

	t.Run("attempt to unload unexistent database should fail", func(t *testing.T) {
		_, err := client.UnloadDatabase(ctx, &schema.UnloadDatabaseRequest{Database: "db2"})
		require.Contains(t, err.Error(), "database does not exist")
	})

	t.Run("attempt to unload a loaded database should succeed", func(t *testing.T) {
		_, err := client.UnloadDatabase(ctx, &schema.UnloadDatabaseRequest{Database: "db1"})
		require.NoError(t, err)
	})

	t.Run("attempt to unload an already unloaded database should fail", func(t *testing.T) {
		_, err := client.UnloadDatabase(ctx, &schema.UnloadDatabaseRequest{Database: "db1"})
		require.Contains(t, err.Error(), "already closed")
	})

	t.Run("attempt to delete unexistent database should fail", func(t *testing.T) {
		_, err := client.DeleteDatabase(ctx, &schema.DeleteDatabaseRequest{Database: "db2"})
		require.Contains(t, err.Error(), "database does not exist")
	})

	t.Run("attempt to delete a closed database should succeed", func(t *testing.T) {
		_, err := client.DeleteDatabase(ctx, &schema.DeleteDatabaseRequest{Database: "db1"})
		require.NoError(t, err)
	})

	err = client.CloseSession(ctx)
	require.NoError(t, err)
}
