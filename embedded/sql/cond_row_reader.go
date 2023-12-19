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

type conditionalRowReader struct {
	rowReader RowReader

	condition ValueExp
}

func newConditionalRowReader(rowReader RowReader, condition ValueExp) *conditionalRowReader {
	return &conditionalRowReader{
		rowReader: rowReader,
		condition: condition,
	}
}

func (cr *conditionalRowReader) onClose(callback func()) {
	cr.rowReader.onClose(callback)
}

func (cr *conditionalRowReader) Tx() *SQLTx {
	return cr.rowReader.Tx()
}

func (cr *conditionalRowReader) TableAlias() string {
	return cr.rowReader.TableAlias()
}

func (cr *conditionalRowReader) Parameters() map[string]interface{} {
	return cr.rowReader.Parameters()
}

func (cr *conditionalRowReader) OrderBy() []ColDescriptor {
	return cr.rowReader.OrderBy()
}

func (cr *conditionalRowReader) ScanSpecs() *ScanSpecs {
	return cr.rowReader.ScanSpecs()
}

func (cr *conditionalRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return cr.rowReader.Columns(ctx)
}

func (cr *conditionalRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return cr.rowReader.colsBySelector(ctx)
}

func (cr *conditionalRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	err := cr.rowReader.InferParameters(ctx, params)
	if err != nil {
		return err
	}

	cols, err := cr.colsBySelector(ctx)
	if err != nil {
		return err
	}

	_, err = cr.condition.inferType(cols, params, cr.TableAlias())

	return err
}

func (cr *conditionalRowReader) Read(ctx context.Context) (*Row, error) {
	for {
		row, err := cr.rowReader.Read(ctx)
		if err != nil {
			return nil, err
		}

		cond, err := cr.condition.substitute(cr.Parameters())
		if err != nil {
			return nil, fmt.Errorf("%w: when evaluating WHERE clause", err)
		}

		r, err := cond.reduce(cr.Tx(), row, cr.rowReader.TableAlias())
		if err != nil {
			return nil, fmt.Errorf("%w: when evaluating WHERE clause", err)
		}

		nval, isNull := r.(*NullValue)
		if isNull && nval.Type() == BooleanType {
			continue
		}

		satisfies, boolExp := r.(*Bool)
		if !boolExp {
			return nil, fmt.Errorf("%w: expected '%s' in WHERE clause, but '%s' was provided", ErrInvalidCondition, BooleanType, r.Type())
		}

		if satisfies.val {
			return row, nil
		}
	}
}

func (cr *conditionalRowReader) Close() error {
	return cr.rowReader.Close()
}
