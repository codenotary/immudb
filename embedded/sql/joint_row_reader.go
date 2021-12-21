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
)

type jointRowReader struct {
	rowReader RowReader

	joins []*JoinSpec

	rowReaders       []RowReader
	rowReadersValues []map[string]TypedValue

	params map[string]interface{}
}

func newJointRowReader(rowReader RowReader, joins []*JoinSpec, params map[string]interface{}) (*jointRowReader, error) {
	if rowReader == nil || len(joins) == 0 {
		return nil, ErrIllegalArguments
	}

	for _, jspec := range joins {
		if jspec.joinType != InnerJoin {
			return nil, ErrUnsupportedJoinType
		}
	}

	return &jointRowReader{
		params:           params,
		rowReader:        rowReader,
		joins:            joins,
		rowReaders:       []RowReader{rowReader},
		rowReadersValues: make([]map[string]TypedValue, 1+len(joins)),
	}, nil
}

func (jointr *jointRowReader) onClose(callback func()) {
	jointr.rowReader.onClose(callback)
}

func (jointr *jointRowReader) Tx() *SQLTx {
	return jointr.rowReader.Tx()
}

func (jointr *jointRowReader) Database() *Database {
	return jointr.rowReader.Database()
}

func (jointr *jointRowReader) TableAlias() string {
	return jointr.rowReader.TableAlias()
}

func (jointr *jointRowReader) OrderBy() []ColDescriptor {
	return jointr.rowReader.OrderBy()
}

func (jointr *jointRowReader) ScanSpecs() *ScanSpecs {
	return jointr.rowReader.ScanSpecs()
}

func (jointr *jointRowReader) Columns() ([]ColDescriptor, error) {
	return jointr.colsByPos()
}

func (jointr *jointRowReader) colsBySelector() (map[string]ColDescriptor, error) {
	colDescriptors, err := jointr.rowReader.colsBySelector()
	if err != nil {
		return nil, err
	}

	for _, jspec := range jointr.joins {

		// TODO (byo) optimize this by getting selector list only or opening all joint readers
		//            on jointRowReader creation,
		// Note: We're using a dummy ScanSpec object that is only used during read, we're only interested
		//       in column list though
		rr, err := jspec.ds.Resolve(jointr.Tx(), nil, &ScanSpecs{index: &Index{}})
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
					"error resolving '%s' in a join: %w, "+
						"use aliasing to assign unique names "+
						"for all tables, sub-queries and columns",
					sel,
					ErrAmbiguousSelector,
				)
			}
			colDescriptors[sel] = des
		}
	}

	return colDescriptors, nil
}

func (jointr *jointRowReader) colsByPos() ([]ColDescriptor, error) {
	colDescriptors, err := jointr.rowReader.Columns()
	if err != nil {
		return nil, err
	}

	for _, jspec := range jointr.joins {

		// TODO (byo) optimize this by getting selector list only or opening all joint readers
		//            on jointRowReader creation,
		// Note: We're using a dummy ScanSpec object that is only used during read, we're only interested
		//       in column list though
		rr, err := jspec.ds.Resolve(jointr.Tx(), nil, &ScanSpecs{index: &Index{}})
		if err != nil {
			return nil, err
		}
		defer rr.Close()

		cd, err := rr.Columns()
		if err != nil {
			return nil, err
		}

		colDescriptors = append(colDescriptors, cd...)
	}

	return colDescriptors, nil
}

func (jointr *jointRowReader) InferParameters(params map[string]SQLValueType) error {
	err := jointr.rowReader.InferParameters(params)
	if err != nil {
		return err
	}

	cols, err := jointr.colsBySelector()
	if err != nil {
		return err
	}

	for _, join := range jointr.joins {
		err = join.ds.inferParameters(jointr.Tx(), params)
		if err != nil {
			return err
		}

		_, err = join.cond.inferType(cols, params, jointr.Database().Name(), jointr.TableAlias())
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
				where:   jspec.cond.reduceSelectors(row, jointr.Database().Name(), jointr.TableAlias()),
				indexOn: jspec.indexOn,
			}

			reader, err := jointq.Resolve(jointr.Tx(), jointr.params, nil)
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

	return merr.Reduce()
}
