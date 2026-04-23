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

// Package sys registers pg_catalog system tables that expose immudb's
// live catalog to SQL clients using the PostgreSQL wire protocol.
//
// See docs/pg-compat-roadmap.md for the multi-phase plan. This package
// lands Phase A2: the relation catalog (pg_namespace, pg_am, pg_class,
// pg_attribute, pg_index) plus enough built-in functions for psql-style
// lookups to resolve. Wire-layer integration and retirement of the
// regex-based canned handlers in pkg/pgsql/server/ are deferred to a
// later phase.
//
// The package is meant to be blank-imported — binaries that include
// PG wire support should import _ "github.com/codenotary/immudb/pkg/pgsql/sys"
// once at startup, which triggers the init() chain that calls
// sql.RegisterSystemTable for each object.
package sys

import "github.com/codenotary/immudb/embedded/sql"

// OID of the well-known namespaces. Values match PostgreSQL's hardcoded
// system OIDs so clients that cache them (Rails type map, psql's ^B.
// fallback) keep working after connect. 11 = pg_catalog, 2200 = public,
// 13 = information_schema.
const (
	OIDNamespacePgCatalog         int64 = 11
	OIDNamespacePublic            int64 = 2200
	OIDNamespaceInformationSchema int64 = 13
)

// OID of the well-known access methods. Real PostgreSQL values.
const (
	OIDAccessMethodBtree int64 = 403
	OIDAccessMethodHash  int64 = 405
)

// Synthetic-OID range for user-catalog objects. PostgreSQL reserves
// OIDs below 16384 for system objects; user objects start there. We
// hash into [userOIDBase, math.MaxInt32] so our synthetic OIDs never
// collide with real PG system OIDs. Callers that need a stable
// relation OID get it from relOID(name); see oid.go.
const userOIDBase int64 = 16384

// Ensure embedded/sql stays imported even if future edits drop the
// direct reference; the registry lives there and we can't register
// anything if it isn't linked.
var _ = sql.RegisterSystemTable
