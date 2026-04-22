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

package sql

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// SystemTableDef describes a catalog-registered virtual table whose
// schema is visible to the SQL engine but whose rows come from Go code
// rather than persistent storage. Used to expose PG-compatibility
// catalogs (pg_type today; pg_class, pg_attribute, pg_index, etc. in
// future phases) without requiring DDL at server start.
//
// Registered definitions are installed into every Catalog on creation
// (see newCatalog) and survive Catalog.Clone(). They are effectively
// immutable: the registry is a package-level map populated via
// RegisterSystemTable at init() time.
type SystemTableDef struct {
	// Name is the SQL name used to reference the table (e.g. "pg_type").
	// Must be unique across all registrations.
	Name string

	// Columns is the table's column schema. Must be non-empty. The
	// ValuesByPosition slices of Rows returned by Scan must match this
	// order 1:1.
	Columns []SystemTableColumn

	// PKColumn is the name of the primary-key column. Must appear in
	// Columns. The PK index is created as a unique single-column index.
	// System tables that don't have a natural single-column PK should
	// synthesise one (typically a stable oid).
	PKColumn string

	// Scan returns the rows for a query against this table. When nil,
	// the table appears empty at the SQL engine layer — the historic
	// behaviour for pg_type, where rows are synthesised by the PG wire
	// layer instead.
	//
	// The returned Rows' ValuesByPosition must match Columns order;
	// ValuesBySelector is populated by newSystemTableRowReader from
	// ValuesByPosition, so Scan implementations can leave it nil.
	Scan func(ctx context.Context, tx *SQLTx) ([]*Row, error)
}

// SystemTableColumn describes a column in a SystemTableDef.
type SystemTableColumn struct {
	Name   string
	Type   SQLValueType
	MaxLen int
}

var (
	sysTablesMu sync.RWMutex
	sysTables   = map[string]*SystemTableDef{}
)

// RegisterSystemTable registers a system-table definition. Panics on
// duplicate Name or malformed definition — errors here are programmer
// bugs, not runtime conditions. Call at package init().
func RegisterSystemTable(def *SystemTableDef) {
	if def == nil {
		panic("sql: RegisterSystemTable: nil definition")
	}
	if def.Name == "" {
		panic("sql: RegisterSystemTable: empty Name")
	}
	if len(def.Columns) == 0 {
		panic(fmt.Sprintf("sql: RegisterSystemTable(%s): no columns", def.Name))
	}
	if def.PKColumn == "" {
		panic(fmt.Sprintf("sql: RegisterSystemTable(%s): empty PKColumn", def.Name))
	}
	pkFound := false
	for _, c := range def.Columns {
		if c.Name == def.PKColumn {
			pkFound = true
			break
		}
	}
	if !pkFound {
		panic(fmt.Sprintf("sql: RegisterSystemTable(%s): PKColumn %q not in Columns",
			def.Name, def.PKColumn))
	}

	sysTablesMu.Lock()
	defer sysTablesMu.Unlock()
	if _, dup := sysTables[def.Name]; dup {
		panic(fmt.Sprintf("sql: RegisterSystemTable(%s): already registered", def.Name))
	}
	sysTables[def.Name] = def
}

// registeredSystemTables returns the registry snapshot in
// name-sorted order so catalog construction is deterministic across
// restarts (affects test stability and any future oid-hash assignment
// that depends on iteration order).
func registeredSystemTables() []*SystemTableDef {
	sysTablesMu.RLock()
	defer sysTablesMu.RUnlock()

	out := make([]*SystemTableDef, 0, len(sysTables))
	for _, d := range sysTables {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// installSystemTable materialises def as a *Table on catlg. The table
// is installed only into tablesByName (not tables/tablesByID) so user
// DDL enumeration remains unaffected — matching the historic pg_type
// treatment. The PK index is single-column, unique, with id
// PKIndexID; this mirrors how user tables build their primary index
// via newTable, and keeps SELECT planning (which expects every table
// to have a primaryIndex) working uniformly.
func installSystemTable(catlg *Catalog, def *SystemTableDef) {
	// A nil Scan from the def means "zero rows at the SQL engine
	// layer" (the historic pg_type contract — rows are fabricated by
	// the PG wire layer). Install a sentinel no-op Scan so the
	// systemScan field is always non-nil on system tables; call sites
	// use that pointer as a "is this a system table?" marker.
	scan := def.Scan
	if scan == nil {
		scan = func(ctx context.Context, tx *SQLTx) ([]*Row, error) {
			return nil, nil
		}
	}

	t := &Table{
		catalog:    catlg,
		name:       def.Name,
		cols:       make([]*Column, 0, len(def.Columns)),
		colsByID:   make(map[uint32]*Column, len(def.Columns)),
		colsByName: make(map[string]*Column, len(def.Columns)),
		systemScan: scan,
	}

	for i, cSpec := range def.Columns {
		c := &Column{
			table:   t,
			id:      uint32(i + 1),
			colName: cSpec.Name,
			colType: cSpec.Type,
			maxLen:  cSpec.MaxLen,
		}
		t.cols = append(t.cols, c)
		t.colsByID[c.id] = c
		t.colsByName[c.colName] = c
	}
	t.maxColID = uint32(len(def.Columns))

	pkCol := t.colsByName[def.PKColumn]
	pkIdx := &Index{
		table:    t,
		id:       PKIndexID,
		unique:   true,
		cols:     []*Column{pkCol},
		colsByID: map[uint32]*Column{pkCol.id: pkCol},
	}
	t.indexes = []*Index{pkIdx}
	t.indexesByName = map[string]*Index{pkIdx.Name(): pkIdx}
	t.indexesByColID = map[uint32][]*Index{pkCol.id: {pkIdx}}
	t.primaryIndex = pkIdx

	catlg.tablesByName[def.Name] = t
}

// systemTableRowReader implements RowReader by iterating a
// pre-computed slice of rows. Used for SELECTs against system tables
// registered with a non-nil Scan. The rows are materialised eagerly
// in tableRef.Resolve so Scan sees a live SQLTx, then streamed one at
// a time here.
type systemTableRowReader struct {
	tx         *SQLTx
	tableAlias string
	colsByPos  []ColDescriptor
	colsBySel  map[string]ColDescriptor
	rows       []*Row
	pos        int

	onCloseCallback func()
	closed          bool
}

func newSystemTableRowReader(tx *SQLTx, table *Table, alias string, rows []*Row) (*systemTableRowReader, error) {
	if alias == "" {
		alias = table.name
	}

	colsByPos := make([]ColDescriptor, len(table.cols))
	colsBySel := make(map[string]ColDescriptor, len(table.cols))
	for i, c := range table.cols {
		d := ColDescriptor{
			Table:  alias,
			Column: c.colName,
			Type:   c.colType,
		}
		colsByPos[i] = d
		colsBySel[d.Selector()] = d
	}

	// Populate ValuesBySelector for each row from ValuesByPosition.
	// Scan implementations only need to fill ValuesByPosition, which
	// keeps their call sites short.
	for _, r := range rows {
		if len(r.ValuesByPosition) != len(colsByPos) {
			return nil, fmt.Errorf("%w: system table %q row has %d values, expected %d",
				ErrInvalidNumberOfValues, table.name, len(r.ValuesByPosition), len(colsByPos))
		}
		if r.ValuesBySelector == nil {
			r.ValuesBySelector = make(map[string]TypedValue, len(colsByPos))
		}
		for i, v := range r.ValuesByPosition {
			r.ValuesBySelector[colsByPos[i].Selector()] = v
		}
	}

	return &systemTableRowReader{
		tx:         tx,
		tableAlias: alias,
		colsByPos:  colsByPos,
		colsBySel:  colsBySel,
		rows:       rows,
	}, nil
}

func (r *systemTableRowReader) Tx() *SQLTx                         { return r.tx }
func (r *systemTableRowReader) TableAlias() string                 { return r.tableAlias }
func (r *systemTableRowReader) Parameters() map[string]interface{} { return nil }
func (r *systemTableRowReader) OrderBy() []ColDescriptor           { return nil }
func (r *systemTableRowReader) ScanSpecs() *ScanSpecs              { return nil }

func (r *systemTableRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return r.colsByPos, nil
}

func (r *systemTableRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return r.colsBySel, nil
}

func (r *systemTableRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return nil
}

func (r *systemTableRowReader) Read(ctx context.Context) (*Row, error) {
	if r.pos >= len(r.rows) {
		return nil, ErrNoMoreRows
	}
	row := r.rows[r.pos]
	r.pos++
	return row, nil
}

func (r *systemTableRowReader) Close() error {
	if r.closed {
		return ErrAlreadyClosed
	}
	r.closed = true
	if r.onCloseCallback != nil {
		r.onCloseCallback()
	}
	return nil
}

func (r *systemTableRowReader) onClose(cb func()) { r.onCloseCallback = cb }
