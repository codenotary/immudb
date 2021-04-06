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

	params map[string]interface{}

	selectors []Selector

	currRow *Row
}

func (e *Engine) newGroupedRowReader(snap *store.Snapshot, rowReader RowReader, params map[string]interface{}, selectors []Selector) (*groupedRowReader, error) {
	if snap == nil {
		return nil, ErrIllegalArguments
	}

	return &groupedRowReader{
		e:         e,
		snap:      snap,
		rowReader: rowReader,
		params:    params,
		selectors: selectors,
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

		// TODO: group by always over NON-NULLABLE columns

		if gr.currRow == nil {
			gr.currRow = row
			continue
		}

		if !gr.compatibleRow(row) {
			r := gr.currRow
			gr.currRow = row
			return r, nil
		}

		// if row is compatible with currRow, then merge it

		// same agregations are ok with latest state vs new, such as MIN, COUNT
		// others need complete sequence of values, such as SUM, AVG

		// selectors which are not part of an agregation should fail to receive more than 1 value
	}
}

func (gr *groupedRowReader) compatibleRow(row *Row) bool {
	return false
}

func (gr *groupedRowReader) Alias() string {
	return gr.rowReader.Alias()
}

func (gr *groupedRowReader) Close() error {
	return gr.rowReader.Close()
}
