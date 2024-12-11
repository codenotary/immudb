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

package pgschema

import (
	"context"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func setupEngine(t *testing.T, multiDBHandler sql.MultiDBHandler) *sql.Engine {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })

	opts := sql.DefaultOptions().
		WithTableResolvers(PgCatalogResolvers()...)
	if multiDBHandler != nil {
		opts = opts.WithMultiDBHandler(multiDBHandler)
	}

	engine, err := sql.NewEngine(st, opts)
	require.NoError(t, err)
	return engine
}

func TestQueryPgCatalogTables(t *testing.T) {
	engine := setupEngine(t, &mockMultiDBHandler{
		users: []sql.User{
			&user{username: "immudb", perm: sql.PermissionSysAdmin},
		},
	})

	_, _, err := engine.Exec(context.Background(),
		nil,
		`CREATE TABLE table1 (id INTEGER, PRIMARY KEY id)`,
		nil)
	require.NoError(t, err)

	res, err := engine.Query(
		context.Background(),
		nil,
		`SELECT n.nspname as "Schema",
			c.relname as "Name",
			CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
			pg_get_userbyid(c.relowner) as "Owner"
			FROM pg_class c
				LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relkind IN ('r','p','')
				AND n.nspname <> 'pg_catalog'
				AND n.nspname !~ '^pg_toast'
				AND n.nspname <> 'information_schema'
			AND pg_table_is_visible(c.oid)
			ORDER BY 1,2;`,
		nil,
	)
	require.NoError(t, err)
	defer res.Close()

	row, err := res.Read(context.Background())
	require.NoError(t, err)

	name, _ := row.ValuesBySelector[sql.EncodeSelector("", "c", "name")].RawValue().(string)
	owner, _ := row.ValuesBySelector[sql.EncodeSelector("", "c", "owner")].RawValue().(string)
	relType, _ := row.ValuesBySelector[sql.EncodeSelector("", "c", "type")].RawValue().(string)
	schema := row.ValuesBySelector[sql.EncodeSelector("", "n", "schema")].RawValue()

	require.Equal(t, "table1", name)
	require.Equal(t, "immudb", owner)
	require.Equal(t, "table", relType)
	require.Nil(t, schema)
}

func TestQueryPgRolesTable(t *testing.T) {
	engine := setupEngine(t, &mockMultiDBHandler{
		users: []sql.User{
			&user{username: "immudb", perm: sql.PermissionSysAdmin},
			&user{username: "user1", perm: sql.PermissionReadWrite},
		},
	})

	rows, err := engine.Query(
		context.Background(),
		nil,
		`
		SELECT r.rolname, r.rolsuper, r.rolinherit,
			r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
			r.rolconnlimit, r.rolvaliduntil, r.rolreplication,
			r.rolbypassrls

		FROM pg_roles r
		WHERE r.rolname !~ '^pg_'
		ORDER BY 1;`,
		nil,
	)
	require.NoError(t, err)

	row, err := rows.Read(context.Background())
	require.NoError(t, err)

	name, _ := row.ValuesBySelector[sql.EncodeSelector("", "r", "rolname")].RawValue().(string)
	require.Equal(t, "immudb", name)

	roleSuper, _ := row.ValuesBySelector[sql.EncodeSelector("", "r", "rolsuper")].RawValue().(bool)
	require.True(t, roleSuper)

	row, err = rows.Read(context.Background())
	require.NoError(t, err)

	name, _ = row.ValuesBySelector[sql.EncodeSelector("", "r", "rolname")].RawValue().(string)
	require.Equal(t, "user1", name)

	roleSuper, _ = row.ValuesBySelector[sql.EncodeSelector("", "r", "rolsuper")].RawValue().(bool)
	require.False(t, roleSuper)
}

type mockMultiDBHandler struct {
	sql.MultiDBHandler

	users []sql.User
}

type user struct {
	username string
	perm     sql.Permission
}

func (u *user) Username() string {
	return u.username
}

func (u *user) Permission() sql.Permission {
	return u.perm
}

func (u *user) SQLPrivileges() []sql.SQLPrivilege {
	return []sql.SQLPrivilege{sql.SQLPrivilegeCreate, sql.SQLPrivilegeSelect}
}

func (h *mockMultiDBHandler) ListUsers(ctx context.Context) ([]sql.User, error) {
	return h.users, nil
}

func (h *mockMultiDBHandler) GetLoggedUser(ctx context.Context) (sql.User, error) {
	if len(h.users) == 0 {
		return nil, fmt.Errorf("no logged user")
	}
	return h.users[0], nil
}
