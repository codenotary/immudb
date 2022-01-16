/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

import "fmt"

type projectedRowReader struct {
	rowReader RowReader

	tableAlias string

	selectors []Selector
}

func newProjectedRowReader(rowReader RowReader, tableAlias string, selectors []Selector) (*projectedRowReader, error) {
	// case: SELECT *
	if len(selectors) == 0 {
		cols, err := rowReader.Columns()
		if err != nil {
			return nil, err
		}

		for _, col := range cols {
			sel := &ColSelector{
				db:    col.Database,
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

func (pr *projectedRowReader) Database() string {
	return pr.rowReader.Database()
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

func (pr *projectedRowReader) Columns() ([]ColDescriptor, error) {
	colsBySel, err := pr.colsBySelector()
	if err != nil {
		return nil, err
	}

	colsByPos := make([]ColDescriptor, len(pr.selectors))

	for i, sel := range pr.selectors {
		aggFn, db, table, col := sel.resolve(pr.rowReader.Database(), pr.rowReader.TableAlias())

		if pr.tableAlias != "" {
			db = pr.Database()
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
			AggFn:    aggFn,
			Database: db,
			Table:    table,
			Column:   col,
		}

		encSel := colsByPos[i].Selector()

		colsByPos[i].Type = colsBySel[encSel].Type
	}

	return colsByPos, nil
}

func (pr *projectedRowReader) colsBySelector() (map[string]ColDescriptor, error) {
	dsColDescriptors, err := pr.rowReader.colsBySelector()
	if err != nil {
		return nil, err
	}

	colDescriptors := make(map[string]ColDescriptor, len(pr.selectors))

	for i, sel := range pr.selectors {
		aggFn, db, table, col := sel.resolve(pr.rowReader.Database(), pr.rowReader.TableAlias())

		encSel := EncodeSelector(aggFn, db, table, col)

		colDesc, ok := dsColDescriptors[encSel]
		if !ok {
			return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
		}

		if pr.tableAlias != "" {
			db = pr.Database()
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
			AggFn:    aggFn,
			Database: db,
			Table:    table,
			Column:   col,
			Type:     colDesc.Type,
		}

		colDescriptors[des.Selector()] = des
	}

	return colDescriptors, nil
}

func (pr *projectedRowReader) InferParameters(params map[string]SQLValueType) error {
	return pr.rowReader.InferParameters(params)
}

func (pr *projectedRowReader) Parameters() map[string]interface{} {
	return pr.rowReader.Parameters()
}

func (pr *projectedRowReader) SetParameters(params map[string]interface{}) error {
	return pr.rowReader.SetParameters(params)
}

func (pr *projectedRowReader) Read() (*Row, error) {
	row, err := pr.rowReader.Read()
	if err != nil {
		return nil, err
	}

	prow := &Row{
		ValuesByPosition: make([]TypedValue, len(pr.selectors)),
		ValuesBySelector: make(map[string]TypedValue, len(pr.selectors)),
	}

	for i, sel := range pr.selectors {
		aggFn, db, table, col := sel.resolve(pr.rowReader.Database(), pr.rowReader.TableAlias())

		encSel := EncodeSelector(aggFn, db, table, col)

		val, ok := row.ValuesBySelector[encSel]
		if !ok {
			return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
		}

		if pr.tableAlias != "" {
			db = pr.Database()
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
		prow.ValuesBySelector[EncodeSelector(aggFn, db, table, col)] = val
	}

	return prow, nil
}

func (pr *projectedRowReader) Close() error {
	return pr.rowReader.Close()
}
