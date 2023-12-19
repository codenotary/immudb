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
	"fmt"
)

type projectedRowReader struct {
	rowReader RowReader

	tableAlias string

	selectors []Selector
}

func newProjectedRowReader(ctx context.Context, rowReader RowReader, tableAlias string, selectors []Selector) (*projectedRowReader, error) {
	// case: SELECT *
	if len(selectors) == 0 {
		cols, err := rowReader.Columns(ctx)
		if err != nil {
			return nil, err
		}

		for _, col := range cols {
			sel := &ColSelector{
				table: col.Table,
				col:   col.Column,
			}
			selectors = append(selectors, sel)
		}
	}

	return &projectedRowReader{
		rowReader:  rowReader,
		tableAlias: tableAlias,
		selectors:  selectors,
	}, nil
}

func (pr *projectedRowReader) onClose(callback func()) {
	pr.rowReader.onClose(callback)
}

func (pr *projectedRowReader) Tx() *SQLTx {
	return pr.rowReader.Tx()
}

func (pr *projectedRowReader) TableAlias() string {
	if pr.tableAlias == "" {
		return pr.rowReader.TableAlias()
	}

	return pr.tableAlias
}

func (pr *projectedRowReader) OrderBy() []ColDescriptor {
	return pr.rowReader.OrderBy()
}

func (pr *projectedRowReader) ScanSpecs() *ScanSpecs {
	return pr.rowReader.ScanSpecs()
}

func (pr *projectedRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	colsBySel, err := pr.colsBySelector(ctx)
	if err != nil {
		return nil, err
	}

	colsByPos := make([]ColDescriptor, len(pr.selectors))

	for i, sel := range pr.selectors {
		aggFn, table, col := sel.resolve(pr.rowReader.TableAlias())

		if pr.tableAlias != "" {
			table = pr.tableAlias
		}

		if aggFn == "" && sel.alias() != "" {
			col = sel.alias()
		}

		if aggFn != "" {
			aggFn = ""
			col = sel.alias()
			if col == "" {
				col = fmt.Sprintf("col%d", i)
			}
		}

		colsByPos[i] = ColDescriptor{
			AggFn:  aggFn,
			Table:  table,
			Column: col,
		}

		encSel := colsByPos[i].Selector()

		colsByPos[i].Type = colsBySel[encSel].Type
	}

	return colsByPos, nil
}

func (pr *projectedRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	dsColDescriptors, err := pr.rowReader.colsBySelector(ctx)
	if err != nil {
		return nil, err
	}

	colDescriptors := make(map[string]ColDescriptor, len(pr.selectors))

	for i, sel := range pr.selectors {
		aggFn, table, col := sel.resolve(pr.rowReader.TableAlias())

		encSel := EncodeSelector(aggFn, table, col)

		colDesc, ok := dsColDescriptors[encSel]
		if !ok {
			return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
		}

		if pr.tableAlias != "" {
			table = pr.tableAlias
		}

		if aggFn == "" && sel.alias() != "" {
			col = sel.alias()
		}

		if aggFn != "" {
			aggFn = ""
			col = sel.alias()
			if col == "" {
				col = fmt.Sprintf("col%d", i)
			}
		}

		des := ColDescriptor{
			AggFn:  aggFn,
			Table:  table,
			Column: col,
			Type:   colDesc.Type,
		}

		colDescriptors[des.Selector()] = des
	}

	return colDescriptors, nil
}

func (pr *projectedRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return pr.rowReader.InferParameters(ctx, params)
}

func (pr *projectedRowReader) Parameters() map[string]interface{} {
	return pr.rowReader.Parameters()
}

func (pr *projectedRowReader) Read(ctx context.Context) (*Row, error) {
	row, err := pr.rowReader.Read(ctx)
	if err != nil {
		return nil, err
	}

	prow := &Row{
		ValuesByPosition: make([]TypedValue, len(pr.selectors)),
		ValuesBySelector: make(map[string]TypedValue, len(pr.selectors)),
	}

	for i, sel := range pr.selectors {
		aggFn, table, col := sel.resolve(pr.rowReader.TableAlias())

		encSel := EncodeSelector(aggFn, table, col)

		val, ok := row.ValuesBySelector[encSel]
		if !ok {
			return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
		}

		if pr.tableAlias != "" {
			table = pr.tableAlias
		}

		if aggFn == "" && sel.alias() != "" {
			col = sel.alias()
		}

		if aggFn != "" {
			aggFn = ""
			col = sel.alias()
			if col == "" {
				col = fmt.Sprintf("col%d", i)
			}
		}

		prow.ValuesByPosition[i] = val
		prow.ValuesBySelector[EncodeSelector(aggFn, table, col)] = val
	}

	return prow, nil
}

func (pr *projectedRowReader) Close() error {
	return pr.rowReader.Close()
}
