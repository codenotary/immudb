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

import (
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
)

type jointRowReader struct {
	e    *Engine
	snap *tbtree.Snapshot

	rowReader RowReader

	joins []*JoinSpec
}

func (e *Engine) newJointRowReader(snap *tbtree.Snapshot, rowReader RowReader, joins []*JoinSpec) (*jointRowReader, error) {
	if snap == nil || len(joins) == 0 {
		return nil, ErrIllegalArguments
	}

	for _, jspec := range joins {
		if jspec.joinType != InnerJoin {
			return nil, ErrUnsupportedJoinType
		}
	}

	return &jointRowReader{
		e:         e,
		snap:      snap,
		rowReader: rowReader,
		joins:     joins,
	}, nil
}

func (jointr *jointRowReader) Read() (*Row, error) {
	for {
		row, err := jointr.rowReader.Read()
		if err != nil {
			return nil, err
		}

		unsolvedFK := false

		for _, jspec := range jointr.joins {
			tableRef, ok := jspec.ds.(*TableRef)
			if !ok {
				return nil, ErrLimitedJoins
			}

			table, err := tableRef.referencedTable(jointr.e)

			fkSel, err := jspec.cond.jointColumnTo(table.pk)
			if err != nil {
				return nil, err
			}

			fkVal, ok := row.Values[fkSel.resolve(jointr.e.implicitDatabase)]
			if !ok {
				return nil, ErrInvalidJointColumn
			}

			fkEncVal, err := encodeValue(fkVal, table.pk.colType, asPK)
			if err != nil {
				return nil, err
			}

			pkOrd := &OrdCol{
				sel: &ColSelector{
					db:    table.db.name,
					table: table.name,
					col:   table.pk.colName,
				},
				initKeyVal:    fkEncVal,
				useInitKeyVal: true,
			}

			jr, err := jspec.ds.Resolve(jointr.e, jointr.snap, pkOrd)
			if err != nil {
				return nil, err
			}

			jrow, err := jr.Read()
			if err == store.ErrNoMoreEntries {
				unsolvedFK = true
				break
			}
			if err != nil {
				return nil, err
			}

			// Note: by adding values this way joins behave as nested i.e. following joins will be able to seek values
			// from previously resolved ones.
			for c, v := range jrow.Values {
				row.Values[c] = v
			}
		}

		if !unsolvedFK {
			return row, nil
		}
	}
}

func (jointr *jointRowReader) Close() error {
	return jointr.rowReader.Close()
}
