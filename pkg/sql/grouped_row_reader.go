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

import "github.com/codenotary/immudb/embedded/store"

type groupedRowReader struct {
	e    *Engine
	snap *store.Snapshot

	rowReader RowReader

	selectors []Selector

	groupBy []*ColSelector

	currRow *Row
}

func (e *Engine) newGroupedRowReader(snap *store.Snapshot, rowReader RowReader, selectors []Selector, groupBy []*ColSelector) (*groupedRowReader, error) {
	if snap == nil {
		return nil, ErrIllegalArguments
	}

	return &groupedRowReader{
		e:         e,
		snap:      snap,
		rowReader: rowReader,
		selectors: selectors,
		groupBy:   groupBy,
	}, nil
}

func (gr *groupedRowReader) ImplicitDB() string {
	return gr.rowReader.ImplicitDB()
}

func (gr *groupedRowReader) Columns() []*ColDescriptor {
	return gr.rowReader.Columns()
}

func (gr *groupedRowReader) Read() (*Row, error) {
	for {
		row, err := gr.rowReader.Read()
		if err == store.ErrNoMoreEntries {
			if gr.currRow == nil {
				return nil, err
			}

			r := gr.currRow
			gr.currRow = nil
			return r, nil
		}

		if gr.currRow == nil {
			gr.currRow = row
			err = gr.initAggregations()
			if err != nil {
				return nil, err
			}
			continue
		}

		compatible, err := gr.currRow.Compatible(row, gr.groupBy, gr.ImplicitDB(), gr.Alias())
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
		for _, v := range gr.currRow.Values {
			aggV, isAggregatedValue := v.(AggregatedValue)

			if isAggregatedValue {
				val, exists := row.Values[aggV.Selector()]
				if !exists {
					return nil, ErrColumnDoesNotExist
				}

				err = aggV.updateWith(val)
				if err != nil {
					return nil, err
				}
			}
		}
	}
}

func (gr *groupedRowReader) initAggregations() error {
	// augment row with aggregated values
	for _, sel := range gr.selectors {
		aggFn, db, table, col := sel.resolve(gr.ImplicitDB(), gr.Alias())

		encSel := EncodeSelector(aggFn, db, table, col)

		switch aggFn {
		case COUNT:
			{
				if col == "*" {
					database, ok := gr.e.catalog.dbsByName[db]
					if !ok {
						return ErrDatabaseDoesNotExist
					}
					table, ok := database.tablesByName[table]
					if !ok {
						return ErrTableDoesNotExist
					}
					col = table.pk.colName
				}

				gr.currRow.Values[encSel] = &CountValue{sel: EncodeSelector("", db, table, col)}
			}
		case SUM:
			{
				gr.currRow.Values[encSel] = &SumValue{sel: EncodeSelector("", db, table, col)}
			}
		case MIN:
			{
				gr.currRow.Values[encSel] = &MinValue{sel: EncodeSelector("", db, table, col)}
			}
		case MAX:
			{
				gr.currRow.Values[encSel] = &MaxValue{sel: EncodeSelector("", db, table, col)}
			}
		case AVG:
			{
				gr.currRow.Values[encSel] = &AVGValue{sel: EncodeSelector("", db, table, col)}
			}
		}
	}

	for _, v := range gr.currRow.Values {
		aggV, isAggregatedValue := v.(AggregatedValue)

		if isAggregatedValue {
			val, exists := gr.currRow.Values[v.(AggregatedValue).Selector()]
			if !exists {
				return ErrColumnDoesNotExist
			}

			err := aggV.updateWith(val)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (gr *groupedRowReader) Alias() string {
	return gr.rowReader.Alias()
}

func (gr *groupedRowReader) Close() error {
	return gr.rowReader.Close()
}
