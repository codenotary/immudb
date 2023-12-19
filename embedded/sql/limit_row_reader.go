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

import "context"

type limitRowReader struct {
	rowReader RowReader

	limit int
	read  int
}

func newLimitRowReader(rowReader RowReader, limit int) *limitRowReader {
	return &limitRowReader{
		rowReader: rowReader,
		limit:     limit,
	}
}

func (lr *limitRowReader) onClose(callback func()) {
	lr.rowReader.onClose(callback)
}

func (lr *limitRowReader) Tx() *SQLTx {
	return lr.rowReader.Tx()
}

func (lr *limitRowReader) TableAlias() string {
	return lr.rowReader.TableAlias()
}

func (lr *limitRowReader) Parameters() map[string]interface{} {
	return lr.rowReader.Parameters()
}

func (lr *limitRowReader) OrderBy() []ColDescriptor {
	return lr.rowReader.OrderBy()
}

func (lr *limitRowReader) ScanSpecs() *ScanSpecs {
	return lr.rowReader.ScanSpecs()
}

func (lr *limitRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return lr.rowReader.Columns(ctx)
}

func (lr *limitRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return lr.rowReader.colsBySelector(ctx)
}

func (lr *limitRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return lr.rowReader.InferParameters(ctx, params)
}

func (lr *limitRowReader) Read(ctx context.Context) (*Row, error) {
	if lr.read >= lr.limit {
		return nil, ErrNoMoreRows
	}

	row, err := lr.rowReader.Read(ctx)
	if err != nil {
		return nil, err
	}

	lr.read++

	return row, nil
}

func (lr *limitRowReader) Close() error {
	return lr.rowReader.Close()
}
