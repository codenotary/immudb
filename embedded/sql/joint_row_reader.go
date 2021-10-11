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
	"fmt"

	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/store"
)

type jointRowReader struct {
	e          *Engine
	implicitDB *Database

	snap *store.Snapshot

	rowReader RowReader

	joins []*JoinSpec

	rowReaders       []RowReader
	rowReadersValues []map[string]TypedValue

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
	}

	return &jointRowReader{
		e:                e,
		implicitDB:       db,
		snap:             snap,
		params:           params,
		rowReader:        rowReader,
		joins:            joins,
		rowReaders:       []RowReader{rowReader},
		rowReadersValues: make([]map[string]TypedValue, 1+len(joins)),
	}, nil
}

func (jointr *jointRowReader) ImplicitDB() string {
	return jointr.rowReader.ImplicitDB()
}

func (jointr *jointRowReader) ImplicitTable() string {
	return jointr.rowReader.ImplicitTable()
}

func (jointr *jointRowReader) OrderBy() []*ColDescriptor {
	return jointr.rowReader.OrderBy()
}

func (jointr *jointRowReader) ScanSpecs() *ScanSpecs {
	return jointr.rowReader.ScanSpecs()
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
	initColDescriptors, err := jointr.rowReader.colsBySelector()
	if err != nil {
		return nil, err
	}

	// Copy to avoid modifying the original initColDdescriptors
	colDescriptors := make(map[string]*ColDescriptor, len(initColDescriptors))
	for sel, des := range initColDescriptors {
		colDescriptors[sel] = des
	}

	for _, jspec := range jointr.joins {

		// TODO (byo) optimize this by getting selector list only or opening all joint readers
		//            on jointRowReader creation,
		// Note: We're using a dummy ScanSpec object that is only used during read, we're only interested
		//       in column list though
		rr, err := jspec.ds.Resolve(jointr.e, jointr.snap, jointr.implicitDB, nil, &ScanSpecs{index: &Index{}})
		if err != nil {
			return nil, err
		}
		defer rr.Close()

		cd, err := rr.colsBySelector()
		if err != nil {
			return nil, err
		}

		for sel, des := range cd {
			if _, exists := colDescriptors[sel]; exists {
				return nil, fmt.Errorf(
					"error resolving '%s' in a join: %w",
					sel,
					ErrAmbiguousSelector,
				)
			}
			colDescriptors[sel] = des
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

func (jointr *jointRowReader) SetParameters(params map[string]interface{}) error {
	err := jointr.rowReader.SetParameters(params)
	if err != nil {
		return err
	}

	jointr.params, err = normalizeParams(params)

	return err
}

func (jointr *jointRowReader) Read() (row *Row, err error) {
	for {
		row := &Row{Values: make(map[string]TypedValue)}

		for len(jointr.rowReaders) > 0 {
			lastReader := jointr.rowReaders[len(jointr.rowReaders)-1]

			r, err := lastReader.Read()
			if err == ErrNoMoreRows {
				// previous reader will need to read next row
				jointr.rowReaders = jointr.rowReaders[:len(jointr.rowReaders)-1]

				err = lastReader.Close()
				if err != nil {
					return nil, err
				}

				continue
			}
			if err != nil {
				return nil, err
			}

			// override row data
			jointr.rowReadersValues[len(jointr.rowReaders)-1] = r.Values

			break
		}

		if len(jointr.rowReaders) == 0 {
			return nil, ErrNoMoreRows
		}

		// append values from readers
		for i := 0; i < len(jointr.rowReaders); i++ {
			for c, v := range jointr.rowReadersValues[i] {
				row.Values[c] = v
			}
		}

		unsolvedFK := false

		for i := len(jointr.rowReaders) - 1; i < len(jointr.joins); i++ {
			jspec := jointr.joins[i]

			jointq := &SelectStmt{
				ds:      jspec.ds,
				where:   jspec.cond.reduceSelectors(row, jointr.ImplicitDB(), jointr.ImplicitTable()),
				indexOn: jspec.indexOn,
			}

			reader, err := jointq.Resolve(jointr.e, jointr.snap, jointr.implicitDB, jointr.params, nil)
			if err != nil {
				return nil, err
			}

			r, err := reader.Read()
			if err == ErrNoMoreRows {
				// previous reader will need to read next row
				unsolvedFK = true

				err = reader.Close()
				if err != nil {
					return nil, err
				}

				break
			}
			if err != nil {
				return nil, err
			}

			// progress with the joint readers
			// append the reader and kept the values for following rows
			jointr.rowReaders = append(jointr.rowReaders, reader)
			jointr.rowReadersValues[i+1] = r.Values

			for c, v := range r.Values {
				row.Values[c] = v
			}
		}

		// all readers have a valid read
		if !unsolvedFK {
			return row, nil
		}
	}
}

func (jointr *jointRowReader) Close() error {
	merr := multierr.NewMultiErr()

	for _, rowReader := range jointr.rowReaders {
		err := rowReader.Close()
		merr.Append(err)
	}

	if merr.HasErrors() {
		return merr
	}

	return nil
}
