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

type setOpType int

const (
	setOpExcept    setOpType = iota
	setOpIntersect
)

// setOpRowReader implements EXCEPT and INTERSECT set operations.
// It materializes the right side into memory and then filters left side rows.
type setOpRowReader struct {
	left  RowReader
	right RowReader
	cols  []ColDescriptor

	opType      setOpType
	rightLoaded bool
	rightRows   map[string]bool
}

func newSetOpRowReader(ctx context.Context, left, right RowReader, opType setOpType) (*setOpRowReader, error) {
	leftCols, err := left.Columns(ctx)
	if err != nil {
		return nil, err
	}

	rightCols, err := right.Columns(ctx)
	if err != nil {
		return nil, err
	}

	if len(leftCols) != len(rightCols) {
		return nil, fmt.Errorf("%w: each subquery must have same number of columns", ErrColumnMismatchInUnionStmt)
	}

	for c := 0; c < len(leftCols); c++ {
		if leftCols[c].Type != rightCols[c].Type {
			return nil, fmt.Errorf("%w: expecting type '%v' for column '%s'", ErrColumnMismatchInUnionStmt, leftCols[c].Type, rightCols[c].Column)
		}
	}

	return &setOpRowReader{
		left:      left,
		right:     right,
		cols:      leftCols,
		opType:    opType,
		rightRows: make(map[string]bool),
	}, nil
}

func (sr *setOpRowReader) loadRight(ctx context.Context) error {
	if sr.rightLoaded {
		return nil
	}
	sr.rightLoaded = true

	for {
		row, err := sr.right.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		if err != nil {
			return err
		}
		key := rowDigest(row)
		sr.rightRows[key] = true
	}

	// Close the right reader after materializing — it's no longer needed
	return sr.right.Close()
}

func rowDigest(row *Row) string {
	var b []byte
	for _, v := range row.ValuesByPosition {
		if v.IsNull() {
			b = append(b, 0)
		} else {
			b = append(b, 1)
			b = append(b, []byte(fmt.Sprintf("%v", v.RawValue()))...)
		}
		b = append(b, '|')
	}
	return string(b)
}

func (sr *setOpRowReader) onClose(callback func()) {
	sr.left.onClose(callback)
}

func (sr *setOpRowReader) Tx() *SQLTx {
	return sr.left.Tx()
}

func (sr *setOpRowReader) TableAlias() string {
	return ""
}

func (sr *setOpRowReader) Parameters() map[string]interface{} {
	return sr.left.Parameters()
}

func (sr *setOpRowReader) OrderBy() []ColDescriptor {
	return nil
}

func (sr *setOpRowReader) ScanSpecs() *ScanSpecs {
	return nil
}

func (sr *setOpRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return sr.cols, nil
}

func (sr *setOpRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return sr.left.colsBySelector(ctx)
}

func (sr *setOpRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	if err := sr.left.InferParameters(ctx, params); err != nil {
		return err
	}
	return sr.right.InferParameters(ctx, params)
}

func (sr *setOpRowReader) Read(ctx context.Context) (*Row, error) {
	if err := sr.loadRight(ctx); err != nil {
		return nil, err
	}

	for {
		row, err := sr.left.Read(ctx)
		if err != nil {
			return nil, err
		}

		key := rowDigest(row)
		inRight := sr.rightRows[key]

		switch sr.opType {
		case setOpExcept:
			if !inRight {
				return row, nil
			}
		case setOpIntersect:
			if inRight {
				return row, nil
			}
		}
	}
}

func (sr *setOpRowReader) Close() error {
	merr := multierr.NewMultiErr()
	merr.Append(sr.left.Close())
	if !sr.rightLoaded {
		merr.Append(sr.right.Close())
	}
	return merr.Reduce()
}
