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
	"errors"
	"fmt"

	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/store"
)

type unionRowReader struct {
	rowReaders []RowReader
	currReader int

	cols  []ColDescriptor
	alias string
}

func newUnionRowReader(ctx context.Context, rowReaders []RowReader) (*unionRowReader, error) {
	if len(rowReaders) == 0 {
		return nil, ErrIllegalArguments
	}

	cols, err := rowReaders[0].Columns(ctx)
	if err != nil {
		return nil, err
	}

	for i := 1; i < len(rowReaders); i++ {
		cs, err := rowReaders[i].Columns(ctx)
		if err != nil {
			return nil, err
		}

		if len(cols) != len(cs) {
			return nil, fmt.Errorf("%w: each subquery must have same number of columns", ErrColumnMismatchInUnionStmt)
		}

		for c := 0; c < len(cols); c++ {
			if cols[c].Type != cs[c].Type {
				return nil, fmt.Errorf("%w: expecting type '%v' for column '%s'", ErrColumnMismatchInUnionStmt, cols[c].Type, cs[c].Column)
			}
		}
	}

	return &unionRowReader{
		rowReaders: rowReaders,
		cols:       cols,
	}, nil
}

func (ur *unionRowReader) onClose(callback func()) {
	ur.rowReaders[0].onClose(callback)
}

func (ur *unionRowReader) Tx() *SQLTx {
	return ur.rowReaders[0].Tx()
}

func (ur *unionRowReader) TableAlias() string {
	return ur.alias
}

func (ur *unionRowReader) Parameters() map[string]interface{} {
	return ur.rowReaders[0].Parameters()
}

func (ur *unionRowReader) OrderBy() []ColDescriptor {
	return nil
}

func (ur *unionRowReader) ScanSpecs() *ScanSpecs {
	return nil
}

func (ur *unionRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	cols, err := ur.rowReaders[0].Columns(ctx)
	if err != nil {
		return nil, err
	}
	if ur.alias != "" {
		aliasedCols := make([]ColDescriptor, len(cols))
		for i, c := range cols {
			aliasedCols[i] = ColDescriptor{Table: ur.alias, Column: c.Column, Type: c.Type}
		}
		return aliasedCols, nil
	}
	return cols, nil
}

func (ur *unionRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	cols, err := ur.Columns(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[string]ColDescriptor, len(cols))
	for _, c := range cols {
		result[c.Selector()] = c
	}
	return result, nil
}

func (ur *unionRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	for _, r := range ur.rowReaders {
		err := r.InferParameters(ctx, params)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ur *unionRowReader) Read(ctx context.Context) (*Row, error) {
	for {
		row, err := ur.rowReaders[ur.currReader].Read(ctx)
		if errors.Is(err, store.ErrNoMoreEntries) && ur.currReader+1 < len(ur.rowReaders) {
			ur.currReader++
			continue
		}
		if err != nil {
			return nil, err
		}

		if ur.currReader > 0 || ur.alias != "" {
			// remap selectors using the alias or first subquery's column names
			valuesBySelector := make(map[string]TypedValue, len(ur.cols))

			table := ur.alias
			for i, c := range ur.cols {
				col := c
				if table != "" {
					col = ColDescriptor{Table: table, Column: c.Column, Type: c.Type}
				}
				valuesBySelector[col.Selector()] = row.ValuesByPosition[i]
			}

			row.ValuesBySelector = valuesBySelector
		}

		return row, nil
	}
}

func (ur *unionRowReader) Close() error {
	merr := multierr.NewMultiErr()

	// Closing in reverse order to ensure the onClose callback
	// is called after the last reader is closed
	for i := len(ur.rowReaders) - 1; i >= 0; i-- {
		err := ur.rowReaders[i].Close()
		merr.Append(err)
	}

	return merr.Reduce()
}
