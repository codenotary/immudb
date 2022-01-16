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

type limitRowReader struct {
	rowReader RowReader

	limit int
	read  int
}

func newLimitRowReader(rowReader RowReader, limit int) (*limitRowReader, error) {
	return &limitRowReader{
		rowReader: rowReader,
		limit:     limit,
	}, nil
}

func (lr *limitRowReader) onClose(callback func()) {
	lr.rowReader.onClose(callback)
}

func (lr *limitRowReader) Tx() *SQLTx {
	return lr.rowReader.Tx()
}

func (lr *limitRowReader) Database() string {
	return lr.rowReader.Database()
}

func (lr *limitRowReader) TableAlias() string {
	return lr.rowReader.TableAlias()
}

func (lr *limitRowReader) Parameters() map[string]interface{} {
	return lr.rowReader.Parameters()
}

func (lr *limitRowReader) SetParameters(params map[string]interface{}) error {
	return lr.rowReader.SetParameters(params)
}

func (lr *limitRowReader) OrderBy() []ColDescriptor {
	return lr.rowReader.OrderBy()
}

func (lr *limitRowReader) ScanSpecs() *ScanSpecs {
	return lr.rowReader.ScanSpecs()
}

func (lr *limitRowReader) Columns() ([]ColDescriptor, error) {
	return lr.rowReader.Columns()
}

func (lr *limitRowReader) colsBySelector() (map[string]ColDescriptor, error) {
	return lr.rowReader.colsBySelector()
}

func (lr *limitRowReader) InferParameters(params map[string]SQLValueType) error {
	return lr.rowReader.InferParameters(params)
}

func (lr *limitRowReader) Read() (*Row, error) {
	if lr.read >= lr.limit {
		return nil, ErrNoMoreRows
	}

	row, err := lr.rowReader.Read()
	if err != nil {
		return nil, err
	}

	lr.read++

	return row, nil
}

func (lr *limitRowReader) Close() error {
	return lr.rowReader.Close()
}
