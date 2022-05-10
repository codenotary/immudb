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
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
)

type groupedRowReader struct {
	rowReader RowReader

	selectors []Selector

	groupBy []*ColSelector

	currRow  *Row
	nonEmpty bool
}

func newGroupedRowReader(rowReader RowReader, selectors []Selector, groupBy []*ColSelector) (*groupedRowReader, error) {
	if rowReader == nil || len(selectors) == 0 || len(groupBy) > 1 {
		return nil, ErrIllegalArguments
	}

	// TODO: leverage multi-column indexing
	if len(groupBy) == 1 &&
		rowReader.OrderBy()[0].Selector() != EncodeSelector(groupBy[0].resolve(rowReader.Database(), rowReader.TableAlias())) {
		return nil, ErrLimitedGroupBy
	}

	return &groupedRowReader{
		rowReader: rowReader,
		selectors: selectors,
		groupBy:   groupBy,
	}, nil
}

func (gr *groupedRowReader) onClose(callback func()) {
	gr.rowReader.onClose(callback)
}

func (gr *groupedRowReader) Tx() *SQLTx {
	return gr.rowReader.Tx()
}

func (gr *groupedRowReader) Database() string {
	return gr.rowReader.Database()
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

func (gr *groupedRowReader) Columns() ([]ColDescriptor, error) {
	colsBySel, err := gr.colsBySelector()
	if err != nil {
		return nil, err
	}

	colsByPos := make([]ColDescriptor, len(gr.selectors))

	for i, sel := range gr.selectors {
		encSel := EncodeSelector(sel.resolve(gr.rowReader.Database(), gr.rowReader.TableAlias()))
		colsByPos[i] = colsBySel[encSel]
	}

	return colsByPos, nil
}

func (gr *groupedRowReader) colsBySelector() (map[string]ColDescriptor, error) {
	colDescriptors, err := gr.rowReader.colsBySelector()
	if err != nil {
		return nil, err
	}

	for _, sel := range gr.selectors {
		aggFn, db, table, col := sel.resolve(gr.rowReader.Database(), gr.rowReader.TableAlias())

		if aggFn == "" {
			continue
		}

		des := ColDescriptor{
			AggFn:    aggFn,
			Database: db,
			Table:    table,
			Column:   col,
			Type:     IntegerType,
		}

		encSel := des.Selector()

		if aggFn == COUNT {
			colDescriptors[encSel] = des
			continue
		}

		colDesc, ok := colDescriptors[EncodeSelector("", db, table, col)]
		if !ok {
			return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
		}

		if aggFn == MAX || aggFn == MIN {
			colDescriptors[encSel] = colDesc
		} else {
			// SUM, AVG
			colDescriptors[encSel] = des
		}
	}

	return colDescriptors, nil
}

func allAgregations(selectors []Selector) bool {
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
			return &Number{}
		}
	case BooleanType:
		{
			return &Bool{}
		}
	case VarcharType:
		{
			return &Varchar{}
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

func (gr *groupedRowReader) InferParameters(params map[string]SQLValueType) error {
	return gr.rowReader.InferParameters(params)
}

func (gr *groupedRowReader) Parameters() map[string]interface{} {
	return gr.rowReader.Parameters()
}

func (gr *groupedRowReader) SetParameters(params map[string]interface{}) error {
	return gr.rowReader.SetParameters(params)
}

func (gr *groupedRowReader) Read() (*Row, error) {
	for {
		row, err := gr.rowReader.Read()
		if err == store.ErrNoMoreEntries {
			if !gr.nonEmpty && allAgregations(gr.selectors) {
				// special case when all selectors are aggregations
				zeroRow := &Row{
					ValuesByPosition: make([]TypedValue, len(gr.selectors)),
					ValuesBySelector: make(map[string]TypedValue, len(gr.selectors)),
				}

				colsBySelector, err := gr.colsBySelector()
				if err != nil {
					return nil, err
				}

				for i, sel := range gr.selectors {
					aggFn, db, table, col := sel.resolve(gr.rowReader.Database(), gr.rowReader.TableAlias())
					encSel := EncodeSelector(aggFn, db, table, col)

					var zero TypedValue
					if aggFn == COUNT || aggFn == SUM || aggFn == AVG {
						zero = zeroForType(IntegerType)
					} else {
						zero = zeroForType(colsBySelector[encSel].Type)
					}

					zeroRow.ValuesByPosition[i] = zero
					zeroRow.ValuesBySelector[encSel] = zero
				}

				gr.nonEmpty = true

				return zeroRow, nil
			}

			if gr.currRow == nil {
				return nil, err
			}

			r := gr.currRow
			gr.currRow = nil

			return r, nil
		}
		if err != nil {
			return nil, err
		}

		gr.nonEmpty = true

		if gr.currRow == nil {
			gr.currRow = row
			err = gr.initAggregations()
			if err != nil {
				return nil, err
			}
			continue
		}

		compatible, err := gr.currRow.compatible(row, gr.groupBy, gr.rowReader.Database(), gr.rowReader.TableAlias())
		if err != nil {
			return nil, err
		}

		if !compatible {
			r := gr.currRow
			gr.currRow = row

			err = gr.initAggregations()
			if err != nil {
				return nil, err
			}

			return r, nil
		}

		// Compatible rows get merged
		for _, v := range gr.currRow.ValuesBySelector {
			aggV, isAggregatedValue := v.(AggregatedValue)

			if isAggregatedValue {
				if aggV.ColBounded() {
					val, exists := row.ValuesBySelector[aggV.Selector()]
					if !exists {
						return nil, ErrColumnDoesNotExist
					}

					err = aggV.updateWith(val)
					if err != nil {
						return nil, err
					}
				}

				if !aggV.ColBounded() {
					err = aggV.updateWith(nil)
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}
}

func (gr *groupedRowReader) initAggregations() error {
	// augment row with aggregated values
	for _, sel := range gr.selectors {
		aggFn, db, table, col := sel.resolve(gr.rowReader.Database(), gr.rowReader.TableAlias())

		encSel := EncodeSelector(aggFn, db, table, col)

		var v TypedValue

		switch aggFn {
		case COUNT:
			{
				if col != "*" {
					return ErrLimitedCount
				}

				v = &CountValue{sel: EncodeSelector("", db, table, col)}
			}
		case SUM:
			{
				v = &SumValue{sel: EncodeSelector("", db, table, col)}
			}
		case MIN:
			{
				v = &MinValue{sel: EncodeSelector("", db, table, col)}
			}
		case MAX:
			{
				v = &MaxValue{sel: EncodeSelector("", db, table, col)}
			}
		case AVG:
			{
				v = &AVGValue{sel: EncodeSelector("", db, table, col)}
			}
		default:
			{
				continue
			}
		}

		gr.currRow.ValuesByPosition = append(gr.currRow.ValuesByPosition, v)
		gr.currRow.ValuesBySelector[encSel] = v
	}

	for _, v := range gr.currRow.ValuesBySelector {
		aggV, isAggregatedValue := v.(AggregatedValue)

		if isAggregatedValue {
			if aggV.ColBounded() {
				val, exists := gr.currRow.ValuesBySelector[aggV.Selector()]
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

func (gr *groupedRowReader) Close() error {
	return gr.rowReader.Close()
}
