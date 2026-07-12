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
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
)

// TestPgsqlServer_PrepareEmptyOrCommentOnlyStatement is a regression test
// for the issue #2100 follow-up: an extended-protocol Parse message whose
// SQL is empty or comment-only (e.g. a driver liveness probe like
// `-- ping`) used to panic the whole server with
// "index out of range [0] with length 0" at query_machine.go, because
// since 1.11.0 ParseSQL returns zero statements without an error for such
// input and the Parse handler indexed stmts[0] unguarded.
func TestPgsqlServer_PrepareEmptyOrCommentOnlyStatement(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	require.NoError(t, err)

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)
	defer db.Close()

	for _, q := range []string{"-- ping", ""} {
		stmt, err := db.Prepare(q)
		require.NoError(t, err, "Parse of %q must not error", q)

		// Execute answers with EmptyQueryResponse. Some drivers surface
		// that as an error, which is fine — the regression being guarded
		// is a server panic that kills the session (and the process).
		stmt.Exec()
		stmt.Close()
	}

	// The connection (and the server) must still be alive and usable.
	var version string
	err = db.QueryRow("select version()").Scan(&version)
	require.NoError(t, err)
	require.NotEmpty(t, version)
}
