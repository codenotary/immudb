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
	"crypto/sha256"
	"encoding/binary"

	"github.com/codenotary/immudb/embedded/store"
)

type RowReader interface {
	Tx() *SQLTx
	Database() *Database
	TableAlias() string
	SetParameters(params map[string]interface{}) error
	Read() (*Row, error)
	Close() error
	Columns() ([]ColDescriptor, error)
	OrderBy() []ColDescriptor
	ScanSpecs() *ScanSpecs
	InferParameters(params map[string]SQLValueType) error
	colsBySelector() (map[string]ColDescriptor, error)
	onClose(func())
}

type Row struct {
	Values map[string]TypedValue
}

// rows are selector-compatible if both rows have the same assigned value for all specified selectors
func (row *Row) compatible(aRow *Row, selectors []*ColSelector, db, table string) (bool, error) {
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

func (row *Row) digest(cols []ColDescriptor) (d [sha256.Size]byte, err error) {
	h := sha256.New()

	for i, col := range cols {
		v := row.Values[col.Selector()]

		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(i))
		h.Write(b[:])

		_, isNull := v.(*NullValue)
		if isNull {
			continue
		}

		encVal, err := EncodeValue(v.Value(), v.Type(), 0)
		if err != nil {
			return d, err
		}

		h.Write(encVal)
	}

	copy(d[:], h.Sum(nil))

	return
}

type rawRowReader struct {
	tx              *SQLTx
	table           *Table
	asBefore        uint64
	tableAlias      string
	colsByPos       []ColDescriptor
	colsBySel       map[string]ColDescriptor
	scanSpecs       *ScanSpecs
	reader          *store.KeyReader
	onCloseCallback func()
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

func newRawRowReader(tx *SQLTx, table *Table, asBefore uint64, tableAlias string, scanSpecs *ScanSpecs) (*rawRowReader, error) {
	if table == nil || scanSpecs == nil || scanSpecs.index == nil {
		return nil, ErrIllegalArguments
	}

	rSpec, err := keyReaderSpecFrom(tx.engine.prefix, table, scanSpecs)
	if err != nil {
		return nil, err
	}

	r, err := tx.newKeyReader(rSpec)
	if err != nil {
		return nil, err
	}

	if tableAlias == "" {
		tableAlias = table.name
	}

	colsByPos := make([]ColDescriptor, len(table.Cols()))
	colsBySel := make(map[string]ColDescriptor, len(table.Cols()))

	for i, c := range table.Cols() {
		colDescriptor := ColDescriptor{
			Database: table.db.name,
			Table:    tableAlias,
			Column:   c.colName,
			Type:     c.colType,
		}

		colsByPos[i] = colDescriptor
		colsBySel[colDescriptor.Selector()] = colDescriptor
	}

	return &rawRowReader{
		tx:         tx,
		table:      table,
		asBefore:   asBefore,
		tableAlias: tableAlias,
		colsByPos:  colsByPos,
		colsBySel:  colsBySel,
		scanSpecs:  scanSpecs,
		reader:     r,
	}, nil
}

func keyReaderSpecFrom(sqlPrefix []byte, table *Table, scanSpecs *ScanSpecs) (spec *store.KeyReaderSpec, err error) {
	prefix := mapKey(sqlPrefix, scanSpecs.index.prefix(), EncodeID(table.db.id), EncodeID(table.id), EncodeID(scanSpecs.index.id))

	var loKey []byte
	var loKeyReady bool

	var hiKey []byte
	var hiKeyReady bool

	loKey = make([]byte, len(prefix))
	copy(loKey, prefix)

	hiKey = make([]byte, len(prefix))
	copy(hiKey, prefix)

	// seekKey and endKey in the loop below are scan prefixes for beginning
	// and end of the index scanning range. On each index we try to make them more
	// concrete.
	for _, col := range scanSpecs.index.cols {
		colRange, ok := scanSpecs.rangesByColID[col.id]
		if !ok {
			break
		}

		if !hiKeyReady {
			if colRange.hRange == nil {
				hiKeyReady = true
			} else {
				encVal, err := EncodeAsKey(colRange.hRange.val.Value(), col.colType, col.MaxLen())
				if err != nil {
					return nil, err
				}
				hiKey = append(hiKey, encVal...)
			}
		}

		if !loKeyReady {
			if colRange.lRange == nil {
				loKeyReady = true
			} else {
				encVal, err := EncodeAsKey(colRange.lRange.val.Value(), col.colType, col.MaxLen())
				if err != nil {
					return nil, err
				}
				loKey = append(loKey, encVal...)
			}
		}
	}

	// Ensure the hiKey is inclusive regarding all values with that prefix
	hiKey = append(hiKey, KeyValPrefixUpperBound)

	seekKey := loKey
	endKey := hiKey

	if scanSpecs.descOrder {
		seekKey, endKey = endKey, seekKey
	}

	return &store.KeyReaderSpec{
		SeekKey:       seekKey,
		InclusiveSeek: true,
		EndKey:        endKey,
		InclusiveEnd:  true,
		Prefix:        prefix,
		DescOrder:     scanSpecs.descOrder,
		Filter:        store.IgnoreDeleted,
	}, nil
}

func (r *rawRowReader) onClose(callback func()) {
	r.onCloseCallback = callback
}

func (r *rawRowReader) Tx() *SQLTx {
	return r.tx
}

func (r *rawRowReader) Database() *Database {
	return r.table.db
}

func (r *rawRowReader) TableAlias() string {
	return r.tableAlias
}

func (r *rawRowReader) OrderBy() []ColDescriptor {
	cols := make([]ColDescriptor, len(r.scanSpecs.index.cols))

	for i, col := range r.scanSpecs.index.cols {
		cols[i] = ColDescriptor{
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

func (r *rawRowReader) Columns() ([]ColDescriptor, error) {
	ret := make([]ColDescriptor, len(r.colsByPos))
	for i := range r.colsByPos {
		ret[i] = r.colsByPos[i]
	}
	return ret, nil
}

func (r *rawRowReader) colsBySelector() (map[string]ColDescriptor, error) {
	ret := make(map[string]ColDescriptor, len(r.colsBySel))
	for sel := range r.colsBySel {
		ret[sel] = r.colsBySel[sel]
	}
	return ret, nil
}

func (r *rawRowReader) InferParameters(params map[string]SQLValueType) error {
	return nil
}

func (r *rawRowReader) SetParameters(params map[string]interface{}) error {
	return nil
}

func (r *rawRowReader) Read() (row *Row, err error) {
	var mkey []byte
	var vref store.ValueRef

	if r.asBefore > 0 {
		mkey, vref, _, err = r.reader.ReadAsBefore(r.asBefore)
	} else {
		mkey, vref, err = r.reader.Read()
	}
	if err != nil {
		return nil, err
	}

	var v []byte

	//decompose key, determine if it's pk, when it's pk, the value holds the actual row data
	if r.scanSpecs.index.IsPrimary() {
		v, err = vref.Resolve()
		if err != nil {
			return nil, err
		}
	} else {
		var encPKVals []byte

		v, err = vref.Resolve()
		if err != nil {
			return nil, err
		}

		if r.scanSpecs.index.IsUnique() {
			encPKVals = v
		} else {
			encPKVals, err = unmapIndexEntry(r.scanSpecs.index, r.tx.engine.prefix, mkey)
			if err != nil {
				return nil, err
			}
		}

		vref, err = r.tx.get(mapKey(r.tx.engine.prefix, PIndexPrefix, EncodeID(r.table.db.id), EncodeID(r.table.id), EncodeID(PKIndexID), encPKVals))
		if err != nil {
			return nil, err
		}

		v, err = vref.Resolve()
		if err != nil {
			return nil, err
		}
	}

	values := make(map[string]TypedValue, len(r.table.Cols()))

	for _, col := range r.table.Cols() {
		values[EncodeSelector("", r.table.db.name, r.tableAlias, col.colName)] = &NullValue{t: col.colType}
	}

	if len(v) < EncLenLen {
		return nil, ErrCorruptedData
	}

	voff := 0

	cols := int(binary.BigEndian.Uint32(v[voff:]))
	voff += EncLenLen

	for i := 0; i < cols; i++ {
		if len(v) < EncIDLen {
			return nil, ErrCorruptedData
		}

		colID := binary.BigEndian.Uint32(v[voff:])
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

	if len(v)-voff > 0 {
		return nil, ErrCorruptedData
	}

	return &Row{Values: values}, nil
}

func (r *rawRowReader) Close() error {
	if r.onCloseCallback != nil {
		defer r.onCloseCallback()
	}

	return r.reader.Close()
}
