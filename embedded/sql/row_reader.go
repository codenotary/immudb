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
	"encoding/binary"

	"github.com/codenotary/immudb/embedded/store"
)

type RowReader interface {
	ImplicitDB() string
	ImplicitTable() string
	SetParameters(params map[string]interface{})
	Read() (*Row, error)
	Close() error
	Columns() ([]*ColDescriptor, error)
	OrderBy() []*ColDescriptor
	ScanSpecs() *ScanSpecs
	InferParameters(params map[string]SQLValueType) error
	colsBySelector() (map[string]*ColDescriptor, error)
}

type Row struct {
	Values map[string]TypedValue
}

// rows are selector-compatible if both rows have the same assigned value for all specified selectors
func (row *Row) Compatible(aRow *Row, selectors []*ColSelector, db, table string) (bool, error) {
	for _, sel := range selectors {
		c := EncodeSelector(sel.resolve(db, table))

		val1, ok := row.Values[c]
		if !ok {
			return false, ErrInvalidColumn
		}

		val2, ok := aRow.Values[c]
		if !ok {
			return false, ErrInvalidColumn
		}

		cmp, err := val1.Compare(val2)
		if err != nil {
			return false, err
		}

		if cmp != 0 {
			return false, nil
		}
	}

	return true, nil
}

type rawRowReader struct {
	e          *Engine
	snap       *store.Snapshot
	table      *Table
	asBefore   uint64
	tableAlias string
	colsByPos  []*ColDescriptor
	colsBySel  map[string]*ColDescriptor
	scanSpecs  *ScanSpecs
	reader     *store.KeyReader
}

type ColDescriptor struct {
	AggFn    string
	Database string
	Table    string
	Column   string
	Type     SQLValueType
}

func (d *ColDescriptor) Selector() string {
	return EncodeSelector(d.AggFn, d.Database, d.Table, d.Column)
}

func (e *Engine) newRawRowReader(snap *store.Snapshot, table *Table, asBefore uint64, tableAlias string, scanSpecs *ScanSpecs) (*rawRowReader, error) {
	if snap == nil || table == nil || scanSpecs == nil || scanSpecs.index == nil {
		return nil, ErrIllegalArguments
	}

	rSpec, err := keyReaderSpecFrom(e, table, scanSpecs)
	if err != nil {
		return nil, err
	}

	r, err := snap.NewKeyReader(rSpec)
	if err != nil {
		return nil, err
	}

	if tableAlias == "" {
		tableAlias = table.name
	}

	colsByPos := make([]*ColDescriptor, len(table.ColsByID()))
	colsBySel := make(map[string]*ColDescriptor, len(table.ColsByID()))

	for i, c := range table.ColsByID() {
		colDescriptor := &ColDescriptor{
			Database: table.db.name,
			Table:    tableAlias,
			Column:   c.colName,
			Type:     c.colType,
		}

		colsByPos[i-1] = colDescriptor
		colsBySel[colDescriptor.Selector()] = colDescriptor
	}

	return &rawRowReader{
		e:          e,
		snap:       snap,
		table:      table,
		asBefore:   asBefore,
		tableAlias: tableAlias,
		colsByPos:  colsByPos,
		colsBySel:  colsBySel,
		scanSpecs:  scanSpecs,
		reader:     r,
	}, nil
}

func keyReaderSpecFrom(e *Engine, table *Table, ScanSpecs *ScanSpecs) (spec *store.KeyReaderSpec, err error) {
	var indexPrefix string

	if ScanSpecs.index.isPrimary() {
		indexPrefix = PIndexPrefix
	} else {
		if ScanSpecs.index.unique {
			indexPrefix = UIndexPrefix
		} else {
			indexPrefix = SIndexPrefix
		}
	}

	prefix := e.mapKey(indexPrefix, EncodeID(table.db.id), EncodeID(table.id), EncodeID(ScanSpecs.index.id))

	var skey []byte

	for i := 0; i < ScanSpecs.fixedValuesCount; i++ {
		col, err := table.GetColumnByID(ScanSpecs.index.colIDs[i])
		if err != nil {
			return nil, err
		}

		encVal, err := EncodeValue(ScanSpecs.valuesByColID[col.id], col.colType, false)
		if err != nil {
			return nil, err
		}

		prefix = append(prefix, encVal...)
	}

	skey = make([]byte, len(prefix))
	copy(skey, prefix)

	if ScanSpecs.cmp == LowerThan || ScanSpecs.cmp == LowerOrEqualTo {
		for i := ScanSpecs.fixedValuesCount; i < len(ScanSpecs.index.colIDs); i++ {
			col, err := table.GetColumnByID(ScanSpecs.index.colIDs[i])
			if err != nil {
				return nil, err
			}

			val, specified := ScanSpecs.valuesByColID[col.id]
			var encVal []byte

			if specified {
				encVal, err = EncodeValue(val, col.colType, false)
				if err != nil {
					return nil, err
				}
			} else {
				encVal = maxKeyVal(col.colType)
			}

			skey = append(skey, encVal...)
		}

		if !ScanSpecs.index.isPrimary() && !ScanSpecs.index.unique {
			// non-unique index entries include pk value as suffix
			skey = append(skey, maxKeyVal(table.pk.colType)...)
		}
	}

	return &store.KeyReaderSpec{
		SeekKey:       skey,
		InclusiveSeek: ScanSpecs == nil || (ScanSpecs.cmp != LowerThan && ScanSpecs.cmp != GreaterThan),
		Prefix:        prefix,
		DescOrder:     ScanSpecs != nil && (ScanSpecs.cmp == LowerThan || ScanSpecs.cmp == LowerOrEqualTo),
	}, nil
}

func (r *rawRowReader) ImplicitDB() string {
	return r.table.db.name
}

func (r *rawRowReader) ImplicitTable() string {
	return r.tableAlias
}

func (r *rawRowReader) OrderBy() []*ColDescriptor {
	cols := make([]*ColDescriptor, len(r.scanSpecs.index.colIDs))

	for i, colID := range r.scanSpecs.index.colIDs {
		col := r.table.colsByID[colID]

		cols[i] = &ColDescriptor{
			Database: r.table.db.name,
			Table:    r.tableAlias,
			Column:   col.colName,
			Type:     col.colType,
		}
	}

	return cols
}

func (r *rawRowReader) ScanSpecs() *ScanSpecs {
	return r.scanSpecs
}

func (r *rawRowReader) Columns() ([]*ColDescriptor, error) {
	return r.colsByPos, nil
}

func (r *rawRowReader) colsBySelector() (map[string]*ColDescriptor, error) {
	return r.colsBySel, nil
}

func (r *rawRowReader) InferParameters(params map[string]SQLValueType) error {
	return nil
}

func (r *rawRowReader) SetParameters(params map[string]interface{}) {
}

func (r *rawRowReader) Read() (row *Row, err error) {
	var mkey []byte
	var vref *store.ValueRef

	if r.asBefore > 0 {
		mkey, vref, _, err = r.reader.ReadAsBefore(r.asBefore)
	} else {
		mkey, vref, _, _, err = r.reader.Read()
	}
	if err != nil {
		return nil, err
	}

	var v []byte

	//decompose key, determine if it's pk, when it's pk, the value holds the actual row data
	if r.scanSpecs.index.isPrimary() {
		v, err = vref.Resolve()
		if err != nil {
			return nil, err
		}
	} else {
		var encPKVal []byte

		if r.scanSpecs.index.unique {
			encPKVal, err = vref.Resolve()
			if err != nil {
				return nil, err
			}
		} else {
			_, _, _, _, encPKVal, err = r.e.unmapIndexEntry(SIndexPrefix, mkey)
			if err != nil {
				return nil, err
			}
		}

		v, _, _, err = r.snap.Get(r.e.mapKey(PIndexPrefix, EncodeID(r.table.db.id), EncodeID(r.table.id), EncodeID(pkIndexID), encPKVal))
		if err != nil {
			return nil, err
		}
	}

	values := make(map[string]TypedValue, len(r.table.ColsByID()))

	for _, col := range r.table.colsByName {
		values[EncodeSelector("", r.table.db.name, r.tableAlias, col.colName)] = &NullValue{t: col.colType}
	}

	voff := 0

	cols := int(binary.BigEndian.Uint32(v[voff:]))
	voff += EncLenLen

	for i := 0; i < cols; i++ {
		if len(v) < EncIDLen {
			return nil, ErrCorruptedData
		}

		colID := binary.BigEndian.Uint64(v[voff:])
		voff += EncIDLen

		col, err := r.table.GetColumnByID(colID)
		if err != nil {
			return nil, ErrCorruptedData
		}

		val, n, err := DecodeValue(v[voff:], col.colType)
		if err != nil {
			return nil, err
		}

		voff += n
		values[EncodeSelector("", r.table.db.name, r.tableAlias, col.colName)] = val
	}

	return &Row{Values: values}, nil
}

func (r *rawRowReader) Close() error {
	return r.reader.Close()
}
