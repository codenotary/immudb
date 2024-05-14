/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
)

type sortDirection int8

const (
	sortDirectionDesc sortDirection = -1
	sortDirectionAsc  sortDirection = 1
)

type sortRowReader struct {
	rowReader          RowReader
	ordCols            []*OrdCol
	orderByDescriptors []ColDescriptor
	sortKeysPositions  []int
	sorter             fileSorter

	resultReader resultReader
}

func newSortRowReader(rowReader RowReader, ordCols []*OrdCol) (*sortRowReader, error) {
	if rowReader == nil || len(ordCols) == 0 {
		return nil, ErrIllegalArguments
	}

	descriptors, err := rowReader.Columns(context.Background())
	if err != nil {
		return nil, err
	}

	colPosBySelector, err := getColPositionsBySelector(descriptors)
	if err != nil {
		return nil, err
	}

	colTypes, err := getColTypes(rowReader)
	if err != nil {
		return nil, err
	}

	sortKeysPositions, err := getSortKeysPositions(colPosBySelector, ordCols, rowReader.TableAlias())
	if err != nil {
		return nil, err
	}

	tx := rowReader.Tx()
	sr := &sortRowReader{
		rowReader:          rowReader,
		orderByDescriptors: getOrderByDescriptors(descriptors, sortKeysPositions),
		ordCols:            ordCols,
		sortKeysPositions:  sortKeysPositions,
		sorter: fileSorter{
			colPosBySelector: colPosBySelector,
			colTypes:         colTypes,
			tx:               tx,
			sortBufSize:      tx.engine.sortBufferSize,
			sortBuf:          make([]*Row, tx.engine.sortBufferSize),
		},
	}

	directions := make([]sortDirection, len(ordCols))
	for i, col := range ordCols {
		directions[i] = sortDirectionAsc
		if col.descOrder {
			directions[i] = sortDirectionDesc
		}
	}

	k1 := make(Tuple, len(sortKeysPositions))
	k2 := make(Tuple, len(sortKeysPositions))

	sr.sorter.cmp = func(t1, t2 Tuple) int {
		sr.extractSortKey(t1, k1)
		sr.extractSortKey(t2, k2)

		res, idx, _ := k1.Compare(k2)
		if idx >= 0 {
			return res * int(directions[idx])
		}
		return res
	}
	return sr, nil
}

func getColTypes(r RowReader) ([]string, error) {
	descriptors, err := r.Columns(context.Background())
	if err != nil {
		return nil, err
	}

	cols := make([]string, len(descriptors))
	for i, desc := range descriptors {
		cols[i] = desc.Type
	}
	return cols, err
}

func getSortKeysPositions(colPosBySelector map[string]int, cols []*OrdCol, tableAlias string) ([]int, error) {
	sortKeysPositions := make([]int, len(cols))
	for i, col := range cols {
		aggFn, table, col := col.sel.resolve(tableAlias)
		encSel := EncodeSelector(aggFn, table, col)

		pos, exists := colPosBySelector[encSel]
		if !exists {
			return nil, ErrColumnDoesNotExist
		}
		sortKeysPositions[i] = pos
	}
	return sortKeysPositions, nil
}

func getColPositionsBySelector(desc []ColDescriptor) (map[string]int, error) {
	colPositionsBySelector := make(map[string]int)
	for i, desc := range desc {
		colPositionsBySelector[desc.Selector()] = i
	}
	return colPositionsBySelector, nil
}

func (sr *sortRowReader) extractSortKey(t Tuple, out Tuple) {
	for i, pos := range sr.sortKeysPositions {
		out[i] = t[pos]
	}
}

func getOrderByDescriptors(descriptors []ColDescriptor, sortKeysPositions []int) []ColDescriptor {
	orderByDescriptors := make([]ColDescriptor, len(sortKeysPositions))
	for i, pos := range sortKeysPositions {
		orderByDescriptors[i] = descriptors[pos]
	}
	return orderByDescriptors
}

func (sr *sortRowReader) onClose(callback func()) {
	sr.rowReader.onClose(callback)
}

func (sr *sortRowReader) Tx() *SQLTx {
	return sr.rowReader.Tx()
}

func (sr *sortRowReader) TableAlias() string {
	return sr.rowReader.TableAlias()
}

func (sr *sortRowReader) Parameters() map[string]interface{} {
	return sr.rowReader.Parameters()
}

func (sr *sortRowReader) OrderBy() []ColDescriptor {
	return sr.orderByDescriptors
}

func (sr *sortRowReader) ScanSpecs() *ScanSpecs {
	return sr.rowReader.ScanSpecs()
}

func (sr *sortRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return sr.rowReader.Columns(ctx)
}

func (sr *sortRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return sr.rowReader.colsBySelector(ctx)
}

func (sr *sortRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return sr.rowReader.InferParameters(ctx, params)
}

func (sr *sortRowReader) Read(ctx context.Context) (*Row, error) {
	if sr.resultReader == nil {
		reader, err := sr.readAndSort(ctx)
		if err != nil {
			return nil, err
		}
		sr.resultReader = reader
	}
	return sr.resultReader.Read()
}

func (sr *sortRowReader) readAndSort(ctx context.Context) (resultReader, error) {
	err := sr.readAll(ctx)
	if err != nil {
		return nil, err
	}
	return sr.sorter.finalize()
}

func (sr *sortRowReader) readAll(ctx context.Context) error {
	for {
		row, err := sr.rowReader.Read(ctx)
		if err == ErrNoMoreRows {
			return nil
		}

		if err != nil {
			return err
		}

		err = sr.sorter.update(row)
		if err != nil {
			return err
		}
	}
}

func (sr *sortRowReader) Close() error {
	return sr.rowReader.Close()
}
