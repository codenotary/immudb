/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"math"

	"github.com/codenotary/immudb/embedded/store"
)

type RowReader interface {
	Tx() *SQLTx
	Database() string
	TableAlias() string
	Parameters() map[string]interface{}
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

type ScanSpecs struct {
	Index         *Index
	rangesByColID map[uint32]*typedValueRange
	DescOrder     bool
}

type Row struct {
	ValuesByPosition []TypedValue
	ValuesBySelector map[string]TypedValue
}

// rows are selector-compatible if both rows have the same assigned value for all specified selectors
func (row *Row) compatible(aRow *Row, selectors []*ColSelector, db, table string) (bool, error) {
	for _, sel := range selectors {
		c := EncodeSelector(sel.resolve(db, table))

		val1, ok := row.ValuesBySelector[c]
		if !ok {
			return false, ErrInvalidColumn
		}

		val2, ok := aRow.ValuesBySelector[c]
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

	for i, v := range row.ValuesByPosition {
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
	tx         *SQLTx
	table      *Table
	tableAlias string
	colsByPos  []ColDescriptor
	colsBySel  map[string]ColDescriptor
	scanSpecs  *ScanSpecs

	// defines a sub-range a transactions based on a combination of tx IDs and timestamps
	// the query is resolved only taking into consideration that range of transactioins
	period period

	// underlying store supports reading entries within a range of txs
	// the range is calculated based on the period stmt, which is included here to support
	// lazy evaluation when parameters are available
	txRange *txRange

	params map[string]interface{}

	reader          *store.KeyReader
	onCloseCallback func()
}

type txRange struct {
	initialTxID uint64
	finalTxID   uint64
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

func newRawRowReader(tx *SQLTx, params map[string]interface{}, table *Table, period period, tableAlias string, scanSpecs *ScanSpecs) (*rawRowReader, error) {
	if table == nil || scanSpecs == nil || scanSpecs.Index == nil {
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
		period:     period,
		tableAlias: tableAlias,
		colsByPos:  colsByPos,
		colsBySel:  colsBySel,
		scanSpecs:  scanSpecs,
		params:     params,
		reader:     r,
	}, nil
}

func keyReaderSpecFrom(sqlPrefix []byte, table *Table, scanSpecs *ScanSpecs) (spec *store.KeyReaderSpec, err error) {
	prefix := mapKey(sqlPrefix, scanSpecs.Index.prefix(), EncodeID(table.db.id), EncodeID(table.id), EncodeID(scanSpecs.Index.id))

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
	for _, col := range scanSpecs.Index.cols {
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

	if scanSpecs.DescOrder {
		seekKey, endKey = endKey, seekKey
	}

	return &store.KeyReaderSpec{
		SeekKey:       seekKey,
		InclusiveSeek: true,
		EndKey:        endKey,
		InclusiveEnd:  true,
		Prefix:        prefix,
		DescOrder:     scanSpecs.DescOrder,
		Filters:       []store.FilterFn{store.IgnoreExpired, store.IgnoreDeleted},
	}, nil
}

func (r *rawRowReader) onClose(callback func()) {
	r.onCloseCallback = callback
}

func (r *rawRowReader) Tx() *SQLTx {
	return r.tx
}

func (r *rawRowReader) Database() string {
	return r.table.db.name
}

func (r *rawRowReader) TableAlias() string {
	return r.tableAlias
}

func (r *rawRowReader) OrderBy() []ColDescriptor {
	cols := make([]ColDescriptor, len(r.scanSpecs.Index.cols))

	for i, col := range r.scanSpecs.Index.cols {
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
	cols, err := r.colsBySelector()
	if err != nil {
		return err
	}

	if r.period.start != nil {
		_, err = r.period.start.instant.exp.inferType(cols, params, r.Database(), r.TableAlias())
		if err != nil {
			return err
		}
	}

	if r.period.end != nil {
		_, err = r.period.end.instant.exp.inferType(cols, params, r.Database(), r.TableAlias())
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *rawRowReader) SetParameters(params map[string]interface{}) (err error) {
	r.params, err = normalizeParams(params)
	return err
}

func (r *rawRowReader) Parameters() map[string]interface{} {
	return r.params
}

func (r *rawRowReader) reduceTxRange() (err error) {
	if r.txRange != nil || (r.period.start == nil && r.period.end == nil) {
		return nil
	}

	txRange := &txRange{
		initialTxID: uint64(0),
		finalTxID:   uint64(math.MaxUint64),
	}

	if r.period.start != nil {
		txRange.initialTxID, err = r.period.start.instant.resolve(r.tx, r.params, true, r.period.start.inclusive)
		if err == store.ErrTxNotFound {
			txRange.initialTxID = uint64(math.MaxUint64)
		}
		if err != nil && err != store.ErrTxNotFound {
			return err
		}
	}

	if r.period.end != nil {
		txRange.finalTxID, err = r.period.end.instant.resolve(r.tx, r.params, false, r.period.end.inclusive)
		if err == store.ErrTxNotFound {
			txRange.finalTxID = uint64(0)
		}
		if err != nil && err != store.ErrTxNotFound {
			return err
		}
	}

	r.txRange = txRange

	return nil
}

func (r *rawRowReader) Read() (row *Row, err error) {
	var mkey []byte
	var vref store.ValueRef

	// evaluation of txRange is postponed to allow parameters to be provided after rowReader initialization
	err = r.reduceTxRange()
	if err != nil {
		return nil, err
	}

	if r.txRange == nil {
		mkey, vref, err = r.reader.Read()
	} else {
		mkey, vref, _, err = r.reader.ReadBetween(r.txRange.initialTxID, r.txRange.finalTxID)
	}
	if err != nil {
		return nil, err
	}

	var v []byte

	//decompose key, determine if it's pk, when it's pk, the value holds the actual row data
	if r.scanSpecs.Index.IsPrimary() {
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

		if r.scanSpecs.Index.IsUnique() {
			encPKVals = v
		} else {
			encPKVals, err = unmapIndexEntry(r.scanSpecs.Index, r.tx.engine.prefix, mkey)
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

	valuesByPosition := make([]TypedValue, len(r.table.Cols()))
	valuesBySelector := make(map[string]TypedValue, len(r.table.Cols()))

	for i, col := range r.table.Cols() {
		v := &NullValue{t: col.colType}

		valuesByPosition[i] = v
		valuesBySelector[EncodeSelector("", r.table.db.name, r.tableAlias, col.colName)] = v
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

		valuesByPosition[i] = val
		valuesBySelector[EncodeSelector("", r.table.db.name, r.tableAlias, col.colName)] = val
	}

	if len(v)-voff > 0 {
		return nil, ErrCorruptedData
	}

	return &Row{ValuesByPosition: valuesByPosition, ValuesBySelector: valuesBySelector}, nil
}

func (r *rawRowReader) Close() error {
	if r.onCloseCallback != nil {
		defer r.onCloseCallback()
	}

	return r.reader.Close()
}
