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

type conditionalRowReader struct {
	e    *Engine
	snap *store.Snapshot

	rowReader RowReader

	condition BoolExp

	params map[string]interface{}
}

func (e *Engine) newConditionalRowReader(snap *store.Snapshot, rowReader RowReader, condition BoolExp, params map[string]interface{}) (*conditionalRowReader, error) {
	if snap == nil {
		return nil, ErrIllegalArguments
	}

	return &conditionalRowReader{
		e:         e,
		snap:      snap,
		rowReader: rowReader,
		condition: condition,
		params:    params,
	}, nil
}

func (cr *conditionalRowReader) Read() (*Row, error) {
	for {
		row, err := cr.rowReader.Read()
		if err != nil {
			return nil, err
		}

		r, err := cr.condition.eval(row, row.ImplicitDB, row.ImplictTable, cr.params)
		if err != nil {
			return nil, err
		}

		satisfies, boolExp := r.(*Bool)
		if !boolExp {
			return nil, ErrInvalidCondition
		}

		if satisfies.val {
			return row, err
		}
	}
}

func (cr *conditionalRowReader) Alias() string {
	return cr.rowReader.Alias()
}

func (cr *conditionalRowReader) Close() error {
	return cr.rowReader.Close()
}
