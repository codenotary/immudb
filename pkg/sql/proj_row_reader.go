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

type projectedRowReader struct {
	e    *Engine
	snap *store.Snapshot

	rowReader RowReader

	selectors []Selector
}

func (e *Engine) newProjectedRowReader(snap *store.Snapshot, rowReader RowReader, selectors []Selector) (*projectedRowReader, error) {
	if snap == nil {
		return nil, ErrIllegalArguments
	}

	return &projectedRowReader{
		e:         e,
		snap:      snap,
		rowReader: rowReader,
		selectors: selectors,
	}, nil
}

func (pr *projectedRowReader) Read() (*Row, error) {
	row, err := pr.rowReader.Read()
	if err != nil {
		return nil, err
	}

	prow := &Row{
		ImplicitDB:   row.ImplicitDB,
		ImplictTable: row.ImplictTable,
		Values:       make(map[string]Value, len(pr.selectors)),
	}

	for _, sel := range pr.selectors {
		c := sel.resolve(prow.ImplicitDB, prow.ImplictTable)

		val, ok := row.Values[c]
		if !ok {
			return nil, ErrInvalidColumn
		}

		selAlias := sel.alias()

		if selAlias != "" {
			asel := &ColSelector{col: selAlias}
			c = asel.resolve(prow.ImplicitDB, prow.ImplictTable)
		}

		prow.Values[c] = val
	}

	return prow, nil
}

func (pr *projectedRowReader) Alias() string {
	return pr.rowReader.Alias()
}

func (pr *projectedRowReader) Close() error {
	return pr.rowReader.Close()
}
