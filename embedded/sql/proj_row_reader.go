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
	rowReader  RowReader
	tableAlias string

	targets []TargetEntry
}

func newProjectedRowReader(ctx context.Context, rowReader RowReader, tableAlias string, targets []TargetEntry) (*projectedRowReader, error) {
	// case: SELECT *
	if len(targets) == 0 {
		cols, err := rowReader.Columns(ctx)
		if err != nil {
			return nil, err
		}

		if len(cols) == 0 {
			return nil, fmt.Errorf("SELECT * with no tables specified is not valid")
		}

		for _, col := range cols {
			targets = append(targets, TargetEntry{
				Exp: &ColSelector{
					table: col.Table,
					col:   col.Column,
				},
			})
		}
	}

	return &projectedRowReader{
		rowReader:  rowReader,
		tableAlias: tableAlias,
		targets:    targets,
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

	colsByPos := make([]ColDescriptor, len(pr.targets))

	for i, t := range pr.targets {
		var aggFn, table, col string = "", pr.rowReader.TableAlias(), ""
		if s, ok := t.Exp.(Selector); ok {
			aggFn, table, col = s.resolve(pr.rowReader.TableAlias())
		}

		if pr.tableAlias != "" {
			table = pr.tableAlias
		}

		if t.As != "" {
			col = t.As
		} else if aggFn != "" || col == "" {
			col = fmt.Sprintf("col%d", i)
		}
		aggFn = ""

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

	colDescriptors := make(map[string]ColDescriptor, len(pr.targets))
	emptyParams := make(map[string]string)

	for i, t := range pr.targets {
		var aggFn, table, col string = "", pr.rowReader.TableAlias(), ""
		if s, ok := t.Exp.(Selector); ok {
			aggFn, table, col = s.resolve(pr.rowReader.TableAlias())
		}

		sqlType, err := t.Exp.inferType(dsColDescriptors, emptyParams, pr.rowReader.TableAlias())
		if err != nil {
			return nil, err
		}

		if pr.tableAlias != "" {
			table = pr.tableAlias
		}

		if t.As != "" {
			col = t.As
		} else if aggFn != "" || col == "" {
			col = fmt.Sprintf("col%d", i)
		}
		aggFn = ""

		des := ColDescriptor{
			AggFn:  aggFn,
			Table:  table,
			Column: col,
			Type:   sqlType,
		}
		colDescriptors[des.Selector()] = des
	}
	return colDescriptors, nil
}

func (pr *projectedRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	if err := pr.rowReader.InferParameters(ctx, params); err != nil {
		return err
	}

	cols, err := pr.rowReader.colsBySelector(ctx)
	if err != nil {
		return err
	}

	for _, ex := range pr.targets {
		_, err = ex.Exp.inferType(cols, params, pr.TableAlias())
		if err != nil {
			return err
		}
	}
	return nil
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
		ValuesByPosition: make([]TypedValue, len(pr.targets)),
		ValuesBySelector: make(map[string]TypedValue, len(pr.targets)),
	}

	for i, t := range pr.targets {
		e, err := t.Exp.substitute(pr.Parameters())
		if err != nil {
			return nil, fmt.Errorf("%w: when evaluating WHERE clause", err)
		}

		v, err := e.reduce(pr.Tx(), row, pr.rowReader.TableAlias())
		if err != nil {
			return nil, err
		}

		var aggFn, table, col string = "", pr.rowReader.TableAlias(), ""
		if s, ok := t.Exp.(Selector); ok {
			aggFn, table, col = s.resolve(pr.rowReader.TableAlias())
		}

		if pr.tableAlias != "" {
			table = pr.tableAlias
		}

		if t.As != "" {
			col = t.As
		} else if aggFn != "" || col == "" {
			col = fmt.Sprintf("col%d", i)
		}

		prow.ValuesByPosition[i] = v
		prow.ValuesBySelector[EncodeSelector("", table, col)] = v
	}
	return prow, nil
}

func (pr *projectedRowReader) Close() error {
	return pr.rowReader.Close()
}
