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
package sql

import (
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestEmptyCatalog(t *testing.T) {
	catalogStore, err := store.Open("catalog", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog")

	catalog, err := openCatalog(catalogStore, prefix)

	dbs, err := catalog.Databases()
	require.NoError(t, err)
	require.Empty(t, dbs)

	exists, err := catalog.DatabaseExist("db1")
	require.NoError(t, err)
	require.False(t, exists)

	err = catalog.UseSnapshot(nil)
	require.Equal(t, ErrIllegalArguments, err)

	err = catalog.UseSnapshot(&UseSnapshotStmt{})
	require.NoError(t, err)

	dbs, err = catalog.Databases()
	require.NoError(t, err)
	require.Empty(t, dbs)

	exists, err = catalog.DatabaseExist("db1")
	require.NoError(t, err)
	require.False(t, exists)
}
