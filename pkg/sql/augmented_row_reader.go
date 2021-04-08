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

type augmentedRowReader struct {
	e    *Engine
	snap *store.Snapshot

	rowReader RowReader

	selectors []Selector
}

func (e *Engine) newAugmentedRowReader(snap *store.Snapshot, rowReader RowReader, selectors []Selector) (*augmentedRowReader, error) {
	if snap == nil || len(selectors) == 0 {
		return nil, ErrIllegalArguments
	}

	return &augmentedRowReader{
		e:         e,
		snap:      snap,
		rowReader: rowReader,
		selectors: selectors,
	}, nil
}

func (ar *augmentedRowReader) ImplicitDB() string {
	return ar.rowReader.ImplicitDB()
}

func (ar *augmentedRowReader) Columns() []*ColDescriptor {
	return ar.rowReader.Columns()
}

func (ar *augmentedRowReader) Read() (*Row, error) {
	for {
		row, err := ar.rowReader.Read()
		if err != nil {
			return nil, err
		}

		// augment row with aggregated values
		for _, sel := range ar.selectors {
			aggFn, db, table, col := sel.resolve(ar.ImplicitDB(), ar.Alias())
			if err != nil {
				return nil, err
			}

			encSel := EncodeSelector(aggFn, db, table, col)

			switch aggFn {
			case COUNT:
				{
					row.Values[encSel] = &CountValue{sel: EncodeSelector("", db, table, col)}
				}
			case SUM:
				{
					row.Values[encSel] = &SumValue{sel: EncodeSelector("", db, table, col)}
				}
			case MAX:
				{
					row.Values[encSel] = &MaxValue{sel: EncodeSelector("", db, table, col)}
				}
			case MIN:
				{
					row.Values[encSel] = &MinValue{sel: EncodeSelector("", db, table, col)}
				}
			case AVG:
				{
					row.Values[encSel] = &AVGValue{sel: EncodeSelector("", db, table, col)}
				}
			}
		}

		return row, nil
	}
}

func (ar *augmentedRowReader) Alias() string {
	return ar.rowReader.Alias()
}

func (ar *augmentedRowReader) Close() error {
	return ar.rowReader.Close()
}
