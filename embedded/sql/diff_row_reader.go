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
)

const (
	diffInsert = "INSERT"
	diffUpdate = "UPDATE"
	diffDelete = "DELETE"
)

type diffRowReader struct {
	tx         *SQLTx
	table      *Table
	tableAlias string
	colsByPos  []ColDescriptor
	colsBySel  map[string]ColDescriptor
	scanSpecs  *ScanSpecs
	params     map[string]interface{}

	beforeReader *rawRowReader
	afterReader  *rawRowReader

	beforeRow *Row
	afterRow  *Row
	beforeEOF bool
	afterEOF  bool

	pkCols []ColDescriptor

	onCloseCallback func()
}

func newDiffRowReader(tx *SQLTx, params map[string]interface{}, table *Table, p period, tableAlias string, scanSpecs *ScanSpecs) (*diffRowReader, error) {
	if table == nil || scanSpecs == nil || scanSpecs.Index == nil {
		return nil, ErrIllegalArguments
	}

	if p.start == nil || p.end == nil {
		return nil, ErrDiffRequiresPeriod
	}

	if tableAlias == "" {
		tableAlias = table.name
	}

	// Build column descriptors: _diff_action + table columns
	nCols := 1 + len(table.cols)
	colsByPos := make([]ColDescriptor, nCols)
	colsBySel := make(map[string]ColDescriptor, nCols)

	diffCol := ColDescriptor{
		Table:  tableAlias,
		Column: diffActionCol,
		Type:   VarcharType,
	}
	colsByPos[0] = diffCol
	colsBySel[diffCol.Selector()] = diffCol

	for i, c := range table.cols {
		colDescriptor := ColDescriptor{
			Table:  tableAlias,
			Column: c.colName,
			Type:   c.colType,
		}
		colsByPos[1+i] = colDescriptor
		colsBySel[colDescriptor.Selector()] = colDescriptor
	}

	// PK column descriptors for merge-join comparison
	pkCols := make([]ColDescriptor, len(scanSpecs.Index.cols))
	for i, col := range scanSpecs.Index.cols {
		pkCols[i] = ColDescriptor{
			Table:  tableAlias,
			Column: col.colName,
			Type:   col.colType,
		}
	}

	// Inner readers get clean ScanSpecs (no diff/history flags)
	innerScanSpecs := &ScanSpecs{
		Index:         scanSpecs.Index,
		rangesByColID: scanSpecs.rangesByColID,
		DescOrder:     scanSpecs.DescOrder,
	}

	// "before" reader: snapshot up to just before the diff range starts.
	// SINCE TX 50 (inclusive) → before sees up to TX 49 → end={inclusive:false, TX 50}
	// AFTER TX 50 (exclusive) → before sees up to TX 50 → end={inclusive:true, TX 50}
	beforePeriod := period{
		end: &openPeriod{
			inclusive: !p.start.inclusive,
			instant:   p.start.instant,
		},
	}

	beforeReader, err := newRawRowReader(tx, params, table, beforePeriod, tableAlias, innerScanSpecs)
	if err != nil {
		return nil, err
	}

	// "after" reader: snapshot at end of diff range
	afterPeriod := period{
		end: p.end,
	}

	afterReader, err := newRawRowReader(tx, params, table, afterPeriod, tableAlias, innerScanSpecs)
	if err != nil {
		beforeReader.Close()
		return nil, err
	}

	return &diffRowReader{
		tx:           tx,
		table:        table,
		tableAlias:   tableAlias,
		colsByPos:    colsByPos,
		colsBySel:    colsBySel,
		scanSpecs:    scanSpecs,
		params:       params,
		beforeReader: beforeReader,
		afterReader:  afterReader,
		pkCols:       pkCols,
	}, nil
}

func (r *diffRowReader) onClose(callback func()) {
	r.onCloseCallback = callback
}

func (r *diffRowReader) Tx() *SQLTx {
	return r.tx
}

func (r *diffRowReader) TableAlias() string {
	return r.tableAlias
}

func (r *diffRowReader) OrderBy() []ColDescriptor {
	cols := make([]ColDescriptor, len(r.scanSpecs.Index.cols))
	for i, col := range r.scanSpecs.Index.cols {
		cols[i] = ColDescriptor{
			Table:  r.tableAlias,
			Column: col.colName,
			Type:   col.colType,
		}
	}
	return cols
}

func (r *diffRowReader) ScanSpecs() *ScanSpecs {
	return r.scanSpecs
}

func (r *diffRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	ret := make([]ColDescriptor, len(r.colsByPos))
	copy(ret, r.colsByPos)
	return ret, nil
}

func (r *diffRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	ret := make(map[string]ColDescriptor, len(r.colsBySel))
	for sel := range r.colsBySel {
		ret[sel] = r.colsBySel[sel]
	}
	return ret, nil
}

func (r *diffRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	if err := r.beforeReader.InferParameters(ctx, params); err != nil {
		return err
	}
	return r.afterReader.InferParameters(ctx, params)
}

func (r *diffRowReader) Parameters() map[string]interface{} {
	return r.params
}

func (r *diffRowReader) Read(ctx context.Context) (*Row, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Advance "before" cursor if needed
		if r.beforeRow == nil && !r.beforeEOF {
			row, err := r.beforeReader.Read(ctx)
			if err == ErrNoMoreRows {
				r.beforeEOF = true
			} else if err != nil {
				return nil, err
			} else {
				r.beforeRow = row
			}
		}

		// Advance "after" cursor if needed
		if r.afterRow == nil && !r.afterEOF {
			row, err := r.afterReader.Read(ctx)
			if err == ErrNoMoreRows {
				r.afterEOF = true
			} else if err != nil {
				return nil, err
			} else {
				r.afterRow = row
			}
		}

		// Both exhausted
		if r.beforeEOF && r.afterEOF {
			return nil, ErrNoMoreRows
		}

		// Only "before" has rows → DELETE
		if r.afterEOF {
			row := r.buildDiffRow(diffDelete, r.beforeRow)
			r.beforeRow = nil
			return row, nil
		}

		// Only "after" has rows → INSERT
		if r.beforeEOF {
			row := r.buildDiffRow(diffInsert, r.afterRow)
			r.afterRow = nil
			return row, nil
		}

		// Both have rows — compare PKs
		cmp, err := r.comparePK(r.beforeRow, r.afterRow)
		if err != nil {
			return nil, err
		}

		// Adjust comparison for descending sort order.
		// In ascending order, cmp < 0 means before's PK comes first in scan → DELETE.
		// In descending order, the meaning is reversed.
		if r.scanSpecs.DescOrder {
			cmp = -cmp
		}

		switch {
		case cmp < 0:
			// before's row comes first in scan order → row was deleted
			row := r.buildDiffRow(diffDelete, r.beforeRow)
			r.beforeRow = nil
			return row, nil

		case cmp > 0:
			// after's row comes first in scan order → row was inserted
			row := r.buildDiffRow(diffInsert, r.afterRow)
			r.afterRow = nil
			return row, nil

		default:
			// Same PK — check if values changed
			equal, err := r.rowsEqual(r.beforeRow, r.afterRow)
			if err != nil {
				return nil, err
			}

			if equal {
				// No change, skip
				r.beforeRow = nil
				r.afterRow = nil
				continue
			}

			// UPDATE — emit "after" values
			row := r.buildDiffRow(diffUpdate, r.afterRow)
			r.beforeRow = nil
			r.afterRow = nil
			return row, nil
		}
	}
}

func (r *diffRowReader) comparePK(beforeRow, afterRow *Row) (int, error) {
	for _, pkCol := range r.pkCols {
		sel := pkCol.Selector()

		bVal, ok := beforeRow.ValuesBySelector[sel]
		if !ok {
			return 0, fmt.Errorf("%w: %s", ErrInvalidColumn, pkCol.Column)
		}

		aVal, ok := afterRow.ValuesBySelector[sel]
		if !ok {
			return 0, fmt.Errorf("%w: %s", ErrInvalidColumn, pkCol.Column)
		}

		cmp, err := bVal.Compare(aVal)
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

func (r *diffRowReader) rowsEqual(beforeRow, afterRow *Row) (bool, error) {
	bd, err := beforeRow.digest(r.beforeReader.colsByPos)
	if err != nil {
		return false, err
	}

	ad, err := afterRow.digest(r.afterReader.colsByPos)
	if err != nil {
		return false, err
	}

	return bd == ad, nil
}

func (r *diffRowReader) buildDiffRow(action string, sourceRow *Row) *Row {
	valuesByPosition := make([]TypedValue, len(r.colsByPos))
	valuesBySelector := make(map[string]TypedValue, len(r.colsBySel))

	actionVal := &Varchar{val: action}
	valuesByPosition[0] = actionVal
	valuesBySelector[r.colsByPos[0].Selector()] = actionVal

	for i := 1; i < len(r.colsByPos); i++ {
		col := r.colsByPos[i]
		sel := col.Selector()

		val, ok := sourceRow.ValuesBySelector[sel]
		if ok {
			valuesByPosition[i] = val
			valuesBySelector[sel] = val
		} else {
			nullVal := &NullValue{t: col.Type}
			valuesByPosition[i] = nullVal
			valuesBySelector[sel] = nullVal
		}
	}

	return &Row{
		ValuesByPosition: valuesByPosition,
		ValuesBySelector: valuesBySelector,
	}
}

func (r *diffRowReader) Close() error {
	if r.onCloseCallback != nil {
		defer r.onCloseCallback()
	}

	err1 := r.beforeReader.Close()
	err2 := r.afterReader.Close()

	if err1 != nil {
		return err1
	}
	return err2
}
