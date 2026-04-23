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
	"hash/fnv"
	"math"
)

// relOID returns a stable synthetic OID for a catalog relation (table,
// view, or index) by FNV-hashing its schema-qualified name. Stability
// across restarts matters because clients (notably Rails' type map)
// cache oids within a session and re-resolve by name on reconnect —
// but a rename mid-session invalidates the cached oid, which is fine:
// most clients re-read on the next connect.
//
// The return range is [userOIDBase, math.MaxInt32], chosen so:
//   - We never collide with PostgreSQL's reserved system-OID space
//     (< 16384).
//   - Values fit in a PG `oid` (unsigned 32-bit) comfortably as a
//     signed int64 at the SQL-engine layer.
func relOID(schema, name string) int64 {
	h := fnv.New32a()
	h.Write([]byte(schema))
	h.Write([]byte{0})
	h.Write([]byte(name))
	// Modulo into the usable range. Using a power-of-two modulus keeps
	// the distribution even; 2^31 - userOIDBase would be cleaner
	// mathematically, but math.MaxInt32 with a userOIDBase offset is
	// what clients actually care about (no overflow, no collision with
	// system OIDs).
	return userOIDBase + int64(h.Sum32()%uint32(math.MaxInt32-userOIDBase))
}

// colOID returns a stable synthetic OID for a column. Columns don't
// have their own PG OID in real PostgreSQL — `pg_attribute` keys are
// (attrelid, attnum) — but several emulated catalogs and functions
// want one, and having a consistent helper keeps them aligned.
func colOID(tableSchema, tableName, colName string) int64 {
	h := fnv.New32a()
	h.Write([]byte(tableSchema))
	h.Write([]byte{0})
	h.Write([]byte(tableName))
	h.Write([]byte{0})
	h.Write([]byte(colName))
	return userOIDBase + int64(h.Sum32()%uint32(math.MaxInt32-userOIDBase))
}
