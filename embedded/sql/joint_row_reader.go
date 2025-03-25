/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sql

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/embedded/multierr"
)

type jointRowReader struct {
	rowReader RowReader

	joins []*JoinSpec

	rowReaders                 []RowReader
	rowReadersValuesByPosition [][]TypedValue
	rowReadersValuesBySelector []map[string]TypedValue
}

func newJointRowReader(rowReader RowReader, joins []*JoinSpec) (*jointRowReader, error) {
	if rowReader == nil || len(joins) == 0 {
		return nil, ErrIllegalArguments
	}

	for _, jspec := range joins {
		switch jspec.joinType {
		case InnerJoin, LeftJoin:
		default:
			return nil, ErrUnsupportedJoinType
		}
	}

	return &jointRowReader{
		rowReader:                  rowReader,
		joins:                      joins,
		rowReaders:                 []RowReader{rowReader},
		rowReadersValuesByPosition: make([][]TypedValue, 1+len(joins)),
		rowReadersValuesBySelector: make([]map[string]TypedValue, 1+len(joins)),
	}, nil
}

func (jointr *jointRowReader) onClose(callback func()) {
	jointr.rowReader.onClose(callback)
}

func (jointr *jointRowReader) Tx() *SQLTx {
	return jointr.rowReader.Tx()
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

func (jointr *jointRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return jointr.colsByPos(ctx)
}

func (jointr *jointRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	colDescriptors, err := jointr.rowReader.colsBySelector(ctx)
	if err != nil {
		return nil, err
	}

	jointDescriptors := make(map[string]ColDescriptor, len(colDescriptors))
	for sel, desc := range colDescriptors {
		jointDescriptors[sel] = desc
	}

	for _, jspec := range jointr.joins {
		// TODO (byo) optimize this by getting selector list only or opening all joint readers
		//            on jointRowReader creation,
		// Note: We're using a dummy ScanSpec object that is only used during read, we're only interested
		//       in column list though
		rr, err := jspec.ds.Resolve(ctx, jointr.Tx(), nil, &ScanSpecs{Index: &Index{}})
		if err != nil {
			return nil, err
		}
		defer rr.Close()

		cd, err := rr.colsBySelector(ctx)
		if err != nil {
			return nil, err
		}

		for sel, des := range cd {
			if _, exists := jointDescriptors[sel]; exists {
				return nil, fmt.Errorf(
					"error resolving '%s' in a join: %w, "+
						"use aliasing to assign unique names "+
						"for all tables, sub-queries and columns",
					sel,
					ErrAmbiguousSelector,
				)
			}
			jointDescriptors[sel] = des
		}
	}
	return jointDescriptors, nil
}

func (jointr *jointRowReader) colsByPos(ctx context.Context) ([]ColDescriptor, error) {
	colDescriptors, err := jointr.rowReader.Columns(ctx)
	if err != nil {
		return nil, err
	}

	for _, jspec := range jointr.joins {

		// TODO (byo) optimize this by getting selector list only or opening all joint readers
		//            on jointRowReader creation,
		// Note: We're using a dummy ScanSpec object that is only used during read, we're only interested
		//       in column list though
		rr, err := jspec.ds.Resolve(ctx, jointr.Tx(), nil, &ScanSpecs{Index: &Index{}})
		if err != nil {
			return nil, err
		}
		defer rr.Close()

		cd, err := rr.Columns(ctx)
		if err != nil {
			return nil, err
		}

		colDescriptors = append(colDescriptors, cd...)
	}

	return colDescriptors, nil
}

func (jointr *jointRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	err := jointr.rowReader.InferParameters(ctx, params)
	if err != nil {
		return err
	}

	cols, err := jointr.colsBySelector(ctx)
	if err != nil {
		return err
	}

	for _, join := range jointr.joins {
		err = join.ds.inferParameters(ctx, jointr.Tx(), params)
		if err != nil {
			return err
		}

		_, err = join.cond.inferType(cols, params, jointr.TableAlias())
		if err != nil {
			return err
		}
	}
	return err
}

func (jointr *jointRowReader) Parameters() map[string]interface{} {
	return jointr.rowReader.Parameters()
}

func (jointr *jointRowReader) Read(ctx context.Context) (row *Row, err error) {
	for {
		row := &Row{
			ValuesByPosition: make([]TypedValue, 0),
			ValuesBySelector: make(map[string]TypedValue),
		}

		for len(jointr.rowReaders) > 0 {
			lastReader := jointr.rowReaders[len(jointr.rowReaders)-1]

			r, err := lastReader.Read(ctx)
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
			jointr.rowReadersValuesByPosition[len(jointr.rowReaders)-1] = r.ValuesByPosition
			jointr.rowReadersValuesBySelector[len(jointr.rowReaders)-1] = r.ValuesBySelector

			break
		}

		if len(jointr.rowReaders) == 0 {
			return nil, ErrNoMoreRows
		}

		// append values from readers
		for i := 0; i < len(jointr.rowReaders); i++ {
			row.ValuesByPosition = append(row.ValuesByPosition, jointr.rowReadersValuesByPosition[i]...)

			for c, v := range jointr.rowReadersValuesBySelector[i] {
				row.ValuesBySelector[c] = v
			}
		}

		unsolvedFK := false

		for i := len(jointr.rowReaders) - 1; i < len(jointr.joins); i++ {
			jspec := jointr.joins[i]

			jointq := &SelectStmt{
				ds:      jspec.ds,
				where:   jspec.cond.reduceSelectors(row, jointr.TableAlias()),
				indexOn: jspec.indexOn,
			}

			reader, err := jointq.Resolve(ctx, jointr.Tx(), jointr.Parameters(), nil)
			if err != nil {
				return nil, err
			}

			r, err := reader.Read(ctx)
			if err == ErrNoMoreRows {
				if jspec.joinType == InnerJoin {
					// previous reader will need to read next row
					unsolvedFK = true

					err = reader.Close()
					if err != nil {
						return nil, err
					}

					break
				} else { // LEFT JOIN: fill column values with NULLs
					cols, err := reader.Columns(ctx)
					if err != nil {
						return nil, err
					}

					r = &Row{
						ValuesByPosition: make([]TypedValue, len(cols)),
						ValuesBySelector: make(map[string]TypedValue, len(cols)),
					}

					for i, col := range cols {
						nullValue := NewNull(col.Type)

						r.ValuesByPosition[i] = nullValue
						r.ValuesBySelector[col.Selector()] = nullValue
					}
				}
			} else if err != nil {
				reader.Close()
				return nil, err
			}

			// progress with the joint readers
			// append the reader and kept the values for following rows
			jointr.rowReaders = append(jointr.rowReaders, reader)
			jointr.rowReadersValuesByPosition[i+1] = r.ValuesByPosition
			jointr.rowReadersValuesBySelector[i+1] = r.ValuesBySelector

			row.ValuesByPosition = append(row.ValuesByPosition, r.ValuesByPosition...)

			for c, v := range r.ValuesBySelector {
				row.ValuesBySelector[c] = v
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

	// Closing joint readers backwards - the first reader executes the onClose callback
	// thus it must be closed at the end
	for i := len(jointr.rowReaders) - 1; i >= 0; i-- {
		err := jointr.rowReaders[i].Close()
		merr.Append(err)
	}

	return merr.Reduce()
}
