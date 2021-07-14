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
)

type jointRowReader struct {
	e          *Engine
	implicitDB *Database

	snap *store.Snapshot

	rowReader RowReader

	joins []*JoinSpec

	params map[string]interface{}
}

func (e *Engine) newJointRowReader(db *Database, snap *store.Snapshot, params map[string]interface{}, rowReader RowReader, joins []*JoinSpec) (*jointRowReader, error) {
	if db == nil || snap == nil || rowReader == nil || len(joins) == 0 {
		return nil, ErrIllegalArguments
	}

	for _, jspec := range joins {
		if jspec.joinType != InnerJoin {
			return nil, ErrUnsupportedJoinType
		}

		tableRef, ok := jspec.ds.(*TableRef)
		if !ok {
			return nil, ErrLimitedJoins
		}

		_, err := tableRef.referencedTable(e, db)
		if err != nil {
			return nil, err
		}
	}

	return &jointRowReader{
		e:          e,
		implicitDB: db,
		snap:       snap,
		params:     params,
		rowReader:  rowReader,
		joins:      joins,
	}, nil
}

func (jointr *jointRowReader) ImplicitDB() string {
	return jointr.rowReader.ImplicitDB()
}

func (jointr *jointRowReader) ImplicitTable() string {
	return jointr.rowReader.ImplicitTable()
}

func (jointr *jointRowReader) Columns() ([]*ColDescriptor, error) {
	colsBySel, err := jointr.colsBySelector()
	if err != nil {
		return nil, err
	}

	colsByPos := make([]*ColDescriptor, len(colsBySel))

	i := 0
	for _, c := range colsBySel {
		colsByPos[i] = c
		i++
	}

	return colsByPos, nil
}

func (jointr *jointRowReader) colsBySelector() (map[string]*ColDescriptor, error) {
	colDescriptors, err := jointr.rowReader.colsBySelector()
	if err != nil {
		return nil, err
	}

	for _, jspec := range jointr.joins {
		tableRef := jspec.ds.(*TableRef)
		table, _ := tableRef.referencedTable(jointr.e, jointr.implicitDB)

		for _, c := range table.ColsByID() {
			des := &ColDescriptor{
				Database: table.db.name,
				Table:    tableRef.Alias(),
				Column:   c.colName,
				Type:     c.colType,
			}
			colDescriptors[des.Selector()] = des
		}
	}

	return colDescriptors, nil
}

func (jointr *jointRowReader) InferParameters(params map[string]SQLValueType) error {
	err := jointr.rowReader.InferParameters(params)
	if err != nil {
		return err
	}

	for _, join := range jointr.joins {
		err = join.ds.inferParameters(jointr.e, jointr.implicitDB, params)
		if err != nil {
			return err
		}

		cols, err := jointr.colsBySelector()
		if err != nil {
			return err
		}

		_, err = join.cond.inferType(cols, params, jointr.ImplicitDB(), jointr.ImplicitTable())
		if err != nil {
			return err
		}
	}

	return err
}

func (jointr *jointRowReader) SetParameters(params map[string]interface{}) {
	jointr.rowReader.SetParameters(params)
	jointr.params = params
}

func (jointr *jointRowReader) Read() (*Row, error) {
	for {
		row, err := jointr.rowReader.Read()
		if err != nil {
			return nil, err
		}

		unsolvedFK := false

		for _, jspec := range jointr.joins {
			tableRef := jspec.ds.(*TableRef)
			table, err := tableRef.referencedTable(jointr.e, jointr.implicitDB)
			if err != nil {
				return nil, err
			}

			fkSel, err := jspec.cond.jointColumnTo(table.pk, tableRef.Alias())
			if err != nil {
				return nil, err
			}

			fkVal, ok := row.Values[EncodeSelector(fkSel.resolve(jointr.rowReader.ImplicitDB(), jointr.rowReader.ImplicitTable()))]
			if !ok {
				return nil, ErrInvalidJointColumn
			}

			fkEncVal, err := EncodeValue(fkVal, table.pk.colType, asKey)
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

			jr, err := jspec.ds.Resolve(jointr.e, jointr.implicitDB, jointr.snap, jointr.params, pkOrd)
			if err != nil {
				return nil, err
			}

			jrow, err := jr.Read()
			if err == store.ErrNoMoreEntries {
				unsolvedFK = true
				jr.Close()
				break
			}
			if err != nil {
				jr.Close()
				return nil, err
			}

			// Note: by adding values this way joins behave as nested i.e. following joins will be able to seek values
			// from previously resolved ones.
			for c, v := range jrow.Values {
				row.Values[c] = v
			}

			err = jr.Close()
			if err != nil {
				return nil, err
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
