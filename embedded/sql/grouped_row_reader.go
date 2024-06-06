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

package sql

import (
	"context"
	"errors"
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
)

type groupedRowReader struct {
	rowReader RowReader

	selectors   []Selector
	groupByCols []*ColSelector
	cols        []ColDescriptor

	currRow *Row
	empty   bool
}

func newGroupedRowReader(rowReader RowReader, selectors []Selector, groupBy []*ColSelector) (*groupedRowReader, error) {
	if rowReader == nil || len(selectors) == 0 {
		return nil, ErrIllegalArguments
	}

	gr := &groupedRowReader{
		rowReader:   rowReader,
		selectors:   selectors,
		groupByCols: groupBy,
		empty:       true,
	}

	cols, err := gr.columns()
	if err == nil {
		gr.cols = cols
	}
	return gr, err
}

func (gr *groupedRowReader) onClose(callback func()) {
	gr.rowReader.onClose(callback)
}

func (gr *groupedRowReader) Tx() *SQLTx {
	return gr.rowReader.Tx()
}

func (gr *groupedRowReader) TableAlias() string {
	return gr.rowReader.TableAlias()
}

func (gr *groupedRowReader) OrderBy() []ColDescriptor {
	return gr.rowReader.OrderBy()
}

func (gr *groupedRowReader) ScanSpecs() *ScanSpecs {
	return gr.rowReader.ScanSpecs()
}

func (gr *groupedRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return gr.cols, nil
}

func (gr *groupedRowReader) columns() ([]ColDescriptor, error) {
	colsBySel, err := gr.colsBySelector(context.Background())
	if err != nil {
		return nil, err
	}

	selectorMap := make(map[string]bool)
	colsByPos := make([]ColDescriptor, 0, len(gr.selectors))
	for _, sel := range gr.selectors {
		encSel := EncodeSelector(sel.resolve(gr.rowReader.TableAlias()))
		colsByPos = append(colsByPos, colsBySel[encSel])
		selectorMap[encSel] = true
	}

	for _, col := range gr.groupByCols {
		sel := EncodeSelector(col.resolve(gr.rowReader.TableAlias()))
		if !selectorMap[sel] {
			colsByPos = append(colsByPos, colsBySel[sel])
		}
	}
	return colsByPos, nil
}

func (gr *groupedRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	colDescriptors, err := gr.rowReader.colsBySelector(ctx)
	if err != nil {
		return nil, err
	}

	for _, sel := range gr.selectors {
		aggFn, table, col := sel.resolve(gr.rowReader.TableAlias())

		if aggFn == "" {
			continue
		}

		des := ColDescriptor{
			AggFn:  aggFn,
			Table:  table,
			Column: col,
			Type:   IntegerType,
		}

		encSel := des.Selector()

		if aggFn == COUNT {
			colDescriptors[encSel] = des
			continue
		}

		colDesc, ok := colDescriptors[EncodeSelector("", table, col)]
		if !ok {
			return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
		}

		des.Type = colDesc.Type
		colDescriptors[encSel] = des
	}
	return colDescriptors, nil
}

func allAggregations(selectors []Selector) bool {
	for _, sel := range selectors {
		_, isAggregation := sel.(*AggColSelector)
		if !isAggregation {
			return false
		}
	}
	return true
}

func zeroForType(t SQLValueType) TypedValue {
	switch t {
	case IntegerType:
		{
			return &Integer{}
		}
	case Float64Type:
		{
			return &Float64{}
		}
	case BooleanType:
		{
			return &Bool{}
		}
	case VarcharType:
		{
			return &Varchar{}
		}
	case UUIDType:
		{
			return &UUID{}
		}
	case BLOBType:
		{
			return &Blob{}
		}
	case TimestampType:
		{
			return &Timestamp{}
		}
	}
	return nil
}

func (gr *groupedRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return gr.rowReader.InferParameters(ctx, params)
}

func (gr *groupedRowReader) Parameters() map[string]interface{} {
	return gr.rowReader.Parameters()
}

func (gr *groupedRowReader) Read(ctx context.Context) (*Row, error) {
	for {
		row, err := gr.rowReader.Read(ctx)
		if errors.Is(err, store.ErrNoMoreEntries) {
			return gr.emitCurrentRow(ctx)
		}

		if err != nil {
			return nil, err
		}

		gr.empty = false

		if gr.currRow == nil {
			gr.currRow = row
			err = gr.initAggregations(gr.currRow)
			if err != nil {
				return nil, err
			}
			continue
		}

		compatible, err := gr.currRow.compatible(row, gr.groupByCols, gr.rowReader.TableAlias())
		if err != nil {
			return nil, err
		}

		if !compatible {
			r := gr.currRow
			gr.currRow = row

			err = gr.initAggregations(gr.currRow)
			if err != nil {
				return nil, err
			}
			return r, nil
		}

		// Compatible rows get merged
		err = updateRow(gr.currRow, row)
		if err != nil {
			return nil, err
		}
	}
}

func updateRow(currRow, newRow *Row) error {
	for _, v := range currRow.ValuesBySelector {
		aggV, isAggregatedValue := v.(AggregatedValue)

		if isAggregatedValue {
			if aggV.ColBounded() {
				val, exists := newRow.ValuesBySelector[aggV.Selector()]
				if !exists {
					return ErrColumnDoesNotExist
				}

				err := aggV.updateWith(val)
				if err != nil {
					return err
				}
			}

			if !aggV.ColBounded() {
				err := aggV.updateWith(nil)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (gr *groupedRowReader) emitCurrentRow(ctx context.Context) (*Row, error) {
	if gr.empty && allAggregations(gr.selectors) && len(gr.groupByCols) == 0 {
		zr, err := gr.zeroRow(ctx)
		if err != nil {
			return nil, err
		}

		gr.empty = false
		return zr, nil
	}

	if gr.currRow == nil {
		return nil, ErrNoMoreRows
	}

	r := gr.currRow
	gr.currRow = nil

	return r, nil
}

func (gr *groupedRowReader) zeroRow(ctx context.Context) (*Row, error) {
	// special case when all selectors are aggregations
	zeroRow := &Row{
		ValuesByPosition: make([]TypedValue, len(gr.selectors)),
		ValuesBySelector: make(map[string]TypedValue, len(gr.selectors)),
	}

	colsBySelector, err := gr.colsBySelector(ctx)
	if err != nil {
		return nil, err
	}

	for i, sel := range gr.selectors {
		aggFn, table, col := sel.resolve(gr.rowReader.TableAlias())
		encSel := EncodeSelector(aggFn, table, col)

		var zero TypedValue
		if aggFn == COUNT {
			zero = zeroForType(IntegerType)
		} else {
			zero = zeroForType(colsBySelector[encSel].Type)
		}

		zeroRow.ValuesByPosition[i] = zero
		zeroRow.ValuesBySelector[encSel] = zero
	}
	return zeroRow, nil
}

func (gr *groupedRowReader) initAggregations(row *Row) error {
	// augment row with aggregated values
	for _, sel := range gr.selectors {
		aggFn, table, col := sel.resolve(gr.rowReader.TableAlias())
		v, err := initAggValue(aggFn, table, col)
		if err != nil {
			return err
		}

		if v == nil {
			continue
		}

		encSel := EncodeSelector(aggFn, table, col)
		row.ValuesBySelector[encSel] = v
	}

	for i, col := range gr.cols {
		v := row.ValuesBySelector[col.Selector()]

		if i < len(row.ValuesByPosition) {
			row.ValuesByPosition[i] = v
		} else {
			row.ValuesByPosition = append(row.ValuesByPosition, v)
		}
	}
	row.ValuesByPosition = row.ValuesByPosition[:len(gr.cols)]
	return updateRow(row, row)
}

func initAggValue(aggFn, table, col string) (TypedValue, error) {
	var v TypedValue
	switch aggFn {
	case COUNT:
		{
			if col != "*" {
				return nil, ErrLimitedCount
			}

			v = &CountValue{sel: EncodeSelector("", table, col)}
		}
	case SUM:
		{
			v = &SumValue{
				val: &NullValue{t: AnyType},
				sel: EncodeSelector("", table, col),
			}
		}
	case MIN:
		{
			v = &MinValue{
				val: &NullValue{t: AnyType},
				sel: EncodeSelector("", table, col),
			}
		}
	case MAX:
		{
			v = &MaxValue{
				val: &NullValue{t: AnyType},
				sel: EncodeSelector("", table, col),
			}
		}
	case AVG:
		{
			v = &AVGValue{
				s:   &NullValue{t: AnyType},
				sel: EncodeSelector("", table, col),
			}
		}
	}
	return v, nil
}

func (gr *groupedRowReader) Close() error {
	return gr.rowReader.Close()
}
