/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

	"github.com/codenotary/immudb/embedded/multierr"
)

// fullOuterJoinRowReader materializes both sides and produces:
// 1. All left rows with matching right rows (or NULLs if no match)
// 2. Unmatched right rows with NULLs for left columns
type fullOuterJoinRowReader struct {
	leftReader  RowReader
	rightReader RowReader

	leftCols  []ColDescriptor
	rightCols []ColDescriptor

	cond     ValueExp
	tx       *SQLTx

	// materialized state
	loaded       bool
	resultRows   []*Row
	resultIdx    int

	onCloseCallback func()
}

func newFullOuterJoinRowReader(ctx context.Context, leftReader, rightReader RowReader, cond ValueExp) (*fullOuterJoinRowReader, error) {
	leftCols, err := leftReader.Columns(ctx)
	if err != nil {
		return nil, err
	}

	rightCols, err := rightReader.Columns(ctx)
	if err != nil {
		return nil, err
	}

	return &fullOuterJoinRowReader{
		leftReader:  leftReader,
		rightReader: rightReader,
		leftCols:    leftCols,
		rightCols:   rightCols,
		cond:        cond,
		tx:          leftReader.Tx(),
	}, nil
}

func (r *fullOuterJoinRowReader) materialize(ctx context.Context) error {
	if r.loaded {
		return nil
	}
	r.loaded = true

	// Read all left rows
	var leftRows []*Row
	for {
		row, err := r.leftReader.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		if err != nil {
			return err
		}
		leftRows = append(leftRows, row)
	}

	// Read all right rows
	var rightRows []*Row
	for {
		row, err := r.rightReader.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		if err != nil {
			return err
		}
		rightRows = append(rightRows, row)
	}

	rightMatched := make([]bool, len(rightRows))
	tableAlias := r.leftReader.TableAlias()

	// Phase 1: For each left row, find matching right rows
	for _, lRow := range leftRows {
		matched := false

		for ri, rRow := range rightRows {
			combined := combineRows(lRow, rRow)

			condVal, err := r.cond.reduce(r.tx, combined, tableAlias)
			if err != nil {
				continue
			}

			boolVal, ok := condVal.RawValue().(bool)
			if !ok || !boolVal {
				continue
			}

			// Match found
			r.resultRows = append(r.resultRows, combined)
			rightMatched[ri] = true
			matched = true
		}

		if !matched {
			// Left row with NULLs for right columns
			r.resultRows = append(r.resultRows, combineRowWithNulls(lRow, r.rightCols))
		}
	}

	// Phase 2: Unmatched right rows with NULLs for left columns
	for ri, rRow := range rightRows {
		if !rightMatched[ri] {
			r.resultRows = append(r.resultRows, combineNullsWithRow(r.leftCols, rRow))
		}
	}

	return nil
}

func combineRows(left, right *Row) *Row {
	row := &Row{
		ValuesByPosition: make([]TypedValue, 0, len(left.ValuesByPosition)+len(right.ValuesByPosition)),
		ValuesBySelector: make(map[string]TypedValue, len(left.ValuesBySelector)+len(right.ValuesBySelector)),
	}

	row.ValuesByPosition = append(row.ValuesByPosition, left.ValuesByPosition...)
	row.ValuesByPosition = append(row.ValuesByPosition, right.ValuesByPosition...)

	for k, v := range left.ValuesBySelector {
		row.ValuesBySelector[k] = v
	}
	for k, v := range right.ValuesBySelector {
		row.ValuesBySelector[k] = v
	}

	return row
}

func combineRowWithNulls(left *Row, rightCols []ColDescriptor) *Row {
	row := &Row{
		ValuesByPosition: make([]TypedValue, 0, len(left.ValuesByPosition)+len(rightCols)),
		ValuesBySelector: make(map[string]TypedValue, len(left.ValuesBySelector)+len(rightCols)),
	}

	row.ValuesByPosition = append(row.ValuesByPosition, left.ValuesByPosition...)
	for k, v := range left.ValuesBySelector {
		row.ValuesBySelector[k] = v
	}

	for _, col := range rightCols {
		nv := NewNull(col.Type)
		row.ValuesByPosition = append(row.ValuesByPosition, nv)
		row.ValuesBySelector[col.Selector()] = nv
	}

	return row
}

func combineNullsWithRow(leftCols []ColDescriptor, right *Row) *Row {
	row := &Row{
		ValuesByPosition: make([]TypedValue, 0, len(leftCols)+len(right.ValuesByPosition)),
		ValuesBySelector: make(map[string]TypedValue, len(leftCols)+len(right.ValuesBySelector)),
	}

	for _, col := range leftCols {
		nv := NewNull(col.Type)
		row.ValuesByPosition = append(row.ValuesByPosition, nv)
		row.ValuesBySelector[col.Selector()] = nv
	}

	row.ValuesByPosition = append(row.ValuesByPosition, right.ValuesByPosition...)
	for k, v := range right.ValuesBySelector {
		row.ValuesBySelector[k] = v
	}

	return row
}

func (r *fullOuterJoinRowReader) onClose(callback func()) {
	r.onCloseCallback = callback
}

func (r *fullOuterJoinRowReader) Tx() *SQLTx {
	return r.tx
}

func (r *fullOuterJoinRowReader) TableAlias() string {
	return r.leftReader.TableAlias()
}

func (r *fullOuterJoinRowReader) OrderBy() []ColDescriptor {
	return nil
}

func (r *fullOuterJoinRowReader) ScanSpecs() *ScanSpecs {
	return nil
}

func (r *fullOuterJoinRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	cols := make([]ColDescriptor, 0, len(r.leftCols)+len(r.rightCols))
	cols = append(cols, r.leftCols...)
	cols = append(cols, r.rightCols...)
	return cols, nil
}

func (r *fullOuterJoinRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	result := make(map[string]ColDescriptor, len(r.leftCols)+len(r.rightCols))
	for _, c := range r.leftCols {
		result[c.Selector()] = c
	}
	for _, c := range r.rightCols {
		result[c.Selector()] = c
	}
	return result, nil
}

func (r *fullOuterJoinRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	if err := r.leftReader.InferParameters(ctx, params); err != nil {
		return err
	}
	return r.rightReader.InferParameters(ctx, params)
}

func (r *fullOuterJoinRowReader) Parameters() map[string]interface{} {
	return r.leftReader.Parameters()
}

func (r *fullOuterJoinRowReader) Read(ctx context.Context) (*Row, error) {
	if err := r.materialize(ctx); err != nil {
		return nil, err
	}

	if r.resultIdx >= len(r.resultRows) {
		return nil, ErrNoMoreRows
	}

	row := r.resultRows[r.resultIdx]
	r.resultIdx++
	return row, nil
}

func (r *fullOuterJoinRowReader) Close() error {
	merr := multierr.NewMultiErr()
	merr.Append(r.leftReader.Close())
	merr.Append(r.rightReader.Close())

	if r.onCloseCallback != nil {
		r.onCloseCallback()
	}

	return merr.Reduce()
}
