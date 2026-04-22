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

package sys

import (
	"context"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
)

// pg_settings: canned GUC rows. Matches what psql, pgAdmin, DBeaver,
// and the pq driver probe on connect. Values are static — immudb has
// no runtime-configurable settings exposed via SQL — but they're read
// by every client's connection bootstrap to decide protocol behaviour
// (DateStyle, client_encoding, standard_conforming_strings).
//
// The row shape matches PostgreSQL's pg_settings view (17 columns)
// except we only populate the handful of columns clients actually
// read; the rest are NULL.
func init() {
	sql.RegisterSystemTable(&sql.SystemTableDef{
		Name: "pg_settings",
		Columns: []sql.SystemTableColumn{
			{Name: "name", Type: sql.VarcharType, MaxLen: 64},
			{Name: "setting", Type: sql.VarcharType, MaxLen: 128},
			{Name: "unit", Type: sql.VarcharType, MaxLen: 1},
			{Name: "category", Type: sql.VarcharType, MaxLen: 64},
			{Name: "short_desc", Type: sql.VarcharType, MaxLen: 256},
			{Name: "extra_desc", Type: sql.VarcharType, MaxLen: 1},
			{Name: "context", Type: sql.VarcharType, MaxLen: 32},
			{Name: "vartype", Type: sql.VarcharType, MaxLen: 16},
			{Name: "source", Type: sql.VarcharType, MaxLen: 32},
			{Name: "min_val", Type: sql.VarcharType, MaxLen: 1},
			{Name: "max_val", Type: sql.VarcharType, MaxLen: 1},
			{Name: "enumvals", Type: sql.VarcharType, MaxLen: 1},
			{Name: "boot_val", Type: sql.VarcharType, MaxLen: 128},
			{Name: "reset_val", Type: sql.VarcharType, MaxLen: 128},
			{Name: "sourcefile", Type: sql.VarcharType, MaxLen: 1},
			{Name: "sourceline", Type: sql.IntegerType},
			{Name: "pending_restart", Type: sql.BooleanType},
		},
		PKColumn: "name",
		Scan: func(ctx context.Context, tx *sql.SQLTx) ([]*sql.Row, error) {
			rows := make([]*sql.Row, 0, len(pgSettingsRows))
			for _, r := range pgSettingsRows {
				rows = append(rows, rowSetting(r.name, r.value, r.category, r.desc))
			}
			return rows, nil
		},
	})
}

// pgSettingsRow is a pair used to build the static row set. Kept
// ordered so test assertions can rely on a stable iteration order.
type pgSettingsRow struct {
	name     string
	value    string
	category string
	desc     string
}

var pgSettingsRows = []pgSettingsRow{
	{"server_version", pgmeta.PgsqlServerVersion, "Preset Options", "Server version string."},
	{"server_version_num", pgmeta.PgsqlServerVersionNum, "Preset Options", "Server version as an integer."},
	{"server_encoding", "UTF8", "Preset Options", "Sets the server (database) character set encoding."},
	{"client_encoding", "UTF8", "Client Connection Defaults", "Sets the client's character set encoding."},
	{"TimeZone", "UTC", "Client Connection Defaults", "Sets the time zone for displaying and interpreting time stamps."},
	{"DateStyle", "ISO, MDY", "Client Connection Defaults", "Sets the display format for date and time values."},
	{"IntervalStyle", "postgres", "Client Connection Defaults", "Sets the display format for interval values."},
	{"lc_collate", "C", "Preset Options", "Shows the collation order locale."},
	{"lc_ctype", "C", "Preset Options", "Shows the character classification and case conversion locale."},
	{"lc_messages", "C", "Client Connection Defaults", "Sets the language in which messages are displayed."},
	{"lc_monetary", "C", "Client Connection Defaults", "Sets the locale for formatting monetary amounts."},
	{"lc_numeric", "C", "Client Connection Defaults", "Sets the locale for formatting numbers."},
	{"lc_time", "C", "Client Connection Defaults", "Sets the locale for formatting date and time values."},
	{"standard_conforming_strings", "on", "Version and Platform Compatibility", "Causes '...' strings to treat backslashes literally."},
	{"integer_datetimes", "on", "Preset Options", "Datetimes are integer based."},
	{"max_connections", "100", "Connections and Authentication", "Sets the maximum number of concurrent connections."},
	{"max_prepared_transactions", "0", "Connections and Authentication", "Sets the maximum number of simultaneously prepared transactions."},
	{"search_path", "public", "Client Connection Defaults", "Sets the schema search order for names that are not schema-qualified."},
	{"default_transaction_isolation", "read committed", "Client Connection Defaults", "Sets the transaction isolation level of each new transaction."},
	{"transaction_isolation", "read committed", "Client Connection Defaults", "Sets the current transaction's isolation level."},
}

func rowSetting(name, val, category, desc string) *sql.Row {
	return &sql.Row{ValuesByPosition: []sql.TypedValue{
		sql.NewVarchar(name),
		sql.NewVarchar(val),
		sql.NewNull(sql.VarcharType),
		sql.NewVarchar(category),
		sql.NewVarchar(desc),
		sql.NewNull(sql.VarcharType),
		sql.NewVarchar("postmaster"),
		sql.NewVarchar("string"),
		sql.NewVarchar("default"),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.VarcharType),
		sql.NewVarchar(val),
		sql.NewVarchar(val),
		sql.NewNull(sql.VarcharType),
		sql.NewNull(sql.IntegerType),
		sql.NewBool(false),
	}}
}
