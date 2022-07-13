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

type offsetRowReader struct {
	rowReader RowReader

	offset  int
	skipped int
}

func newOffsetRowReader(rowReader RowReader, offset int) *offsetRowReader {
	return &offsetRowReader{
		rowReader: rowReader,
		offset:    offset,
	}
}

func (r *offsetRowReader) onClose(callback func()) {
	r.rowReader.onClose(callback)
}

func (r *offsetRowReader) Tx() *SQLTx {
	return r.rowReader.Tx()
}

func (r *offsetRowReader) Database() string {
	return r.rowReader.Database()
}

func (r *offsetRowReader) TableAlias() string {
	return r.rowReader.TableAlias()
}

func (r *offsetRowReader) Parameters() map[string]interface{} {
	return r.rowReader.Parameters()
}

func (r *offsetRowReader) SetParameters(params map[string]interface{}) error {
	return r.rowReader.SetParameters(params)
}

func (r *offsetRowReader) OrderBy() []ColDescriptor {
	return r.rowReader.OrderBy()
}

func (r *offsetRowReader) ScanSpecs() *ScanSpecs {
	return r.rowReader.ScanSpecs()
}

func (r *offsetRowReader) Columns() ([]ColDescriptor, error) {
	return r.rowReader.Columns()
}

func (r *offsetRowReader) colsBySelector() (map[string]ColDescriptor, error) {
	return r.rowReader.colsBySelector()
}

func (r *offsetRowReader) InferParameters(params map[string]SQLValueType) error {
	return r.rowReader.InferParameters(params)
}

func (r *offsetRowReader) Read() (*Row, error) {
	for {
		row, err := r.rowReader.Read()
		if err != nil {
			return nil, err
		}

		if r.skipped < r.offset {
			r.skipped++
			continue
		}

		return row, nil
	}
}

func (r *offsetRowReader) Close() error {
	return r.rowReader.Close()
}
