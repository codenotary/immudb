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

package server_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGiteaCompat_RepoStatsCheckScalarSubquery reproduces the two errors
// seen in the Gitea compose env during repoStatsCheck boot. Both surface
// the same underlying pattern:
//
//	SELECT … FROM <t1> WHERE <col> != (SELECT COUNT(*) FROM <t2> WHERE … = $1 AND … = $2)
//
// Error 1 reports "unexpected IDENTIFIER, expecting LATERAL" at Parse;
// Error 2 reports "got 2 parameters but statement requires 0" at Bind.
// The engine-level ParseSQLString + InferParameters both accept the
// shape (see embedded/sql/scalar_subquery_params_test.go), so any
// failure here is attributable to the pgsql wire-layer path — either
// removePGCatalogReferences corrupting the SQL before Parse or the
// Parse handler emitting 0-param ParameterDescription despite engine
// inferParameters correctly finding two markers.
func TestGiteaCompat_RepoStatsCheckScalarSubquery(t *testing.T) {
	_, port := setupTestServer(t)

	db, err := sql.Open("postgres",
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE repository (id INTEGER NOT NULL, num_closed_issues INTEGER, PRIMARY KEY(id))`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE issue (id INTEGER NOT NULL, repo_id INTEGER, is_closed BOOLEAN, is_pull BOOLEAN, PRIMARY KEY(id))`)
	require.NoError(t, err)

	// The exact Gitea repoStatsCheck query for num_closed_issues.
	rows, err := db.Query(
		`SELECT repo.id FROM repository repo
		 WHERE repo.num_closed_issues != (
		   SELECT COUNT(*) FROM issue
		    WHERE repo_id = repo.id
		      AND is_closed = $1
		      AND is_pull = $2
		 )`,
		true, false,
	)
	require.NoErrorf(t, err,
		"Gitea repoStatsCheck-shape query must not fail at Parse or Bind; "+
			"fail here means the pgsql wire path corrupts the scalar subquery "+
			"or drops its $N bind markers from ParameterDescription")
	defer rows.Close()

	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
	}
	require.NoError(t, rows.Err())
}
