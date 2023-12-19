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
	"crypto/sha256"
)

type distinctRowReader struct {
	rowReader RowReader
	cols      []ColDescriptor

	readRows map[[sha256.Size]byte]struct{}
}

func newDistinctRowReader(ctx context.Context, rowReader RowReader) (*distinctRowReader, error) {
	cols, err := rowReader.Columns(ctx)
	if err != nil {
		return nil, err
	}

	return &distinctRowReader{
		rowReader: rowReader,
		cols:      cols,
		readRows:  make(map[[sha256.Size]byte]struct{}),
	}, nil
}

func (dr *distinctRowReader) onClose(callback func()) {
	dr.rowReader.onClose(callback)
}

func (dr *distinctRowReader) Tx() *SQLTx {
	return dr.rowReader.Tx()
}

func (dr *distinctRowReader) TableAlias() string {
	return dr.rowReader.TableAlias()
}

func (dr *distinctRowReader) Parameters() map[string]interface{} {
	return dr.rowReader.Parameters()
}

func (dr *distinctRowReader) OrderBy() []ColDescriptor {
	return dr.rowReader.OrderBy()
}

func (dr *distinctRowReader) ScanSpecs() *ScanSpecs {
	return dr.rowReader.ScanSpecs()
}

func (dr *distinctRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return dr.rowReader.Columns(ctx)
}

func (dr *distinctRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return dr.rowReader.colsBySelector(ctx)
}

func (dr *distinctRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return dr.rowReader.InferParameters(ctx, params)
}

func (dr *distinctRowReader) Read(ctx context.Context) (*Row, error) {
	for {
		if len(dr.readRows) == dr.rowReader.Tx().distinctLimit() {
			return nil, ErrTooManyRows
		}

		row, err := dr.rowReader.Read(ctx)
		if err != nil {
			return nil, err
		}

		digest, err := row.digest(dr.cols)
		if err != nil {
			return nil, err
		}

		_, ok := dr.readRows[digest]
		if ok {
			continue
		}

		dr.readRows[digest] = struct{}{}

		return row, nil
	}
}

func (dr *distinctRowReader) Close() error {
	return dr.rowReader.Close()
}
