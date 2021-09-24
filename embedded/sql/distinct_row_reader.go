/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

import "crypto/sha256"

type distinctRowReader struct {
	e *Engine

	rowReader RowReader

	limit uint64

	readRows map[[sha256.Size]byte]struct{}
}

func (e *Engine) newDistinctRowReader(rowReader RowReader, limit uint64) (*distinctRowReader, error) {
	return &distinctRowReader{
		e:         e,
		rowReader: rowReader,
		readRows:  make(map[[sha256.Size]byte]struct{}),
		limit:     limit,
	}, nil
}

func (dr *distinctRowReader) ImplicitDB() string {
	return dr.rowReader.ImplicitDB()
}

func (dr *distinctRowReader) ImplicitTable() string {
	return dr.rowReader.ImplicitTable()
}

func (dr *distinctRowReader) SetParameters(params map[string]interface{}) {
	dr.rowReader.SetParameters(params)
}

func (dr *distinctRowReader) OrderBy() []*ColDescriptor {
	return dr.rowReader.OrderBy()
}

func (dr *distinctRowReader) ScanSpecs() *ScanSpecs {
	return dr.rowReader.ScanSpecs()
}

func (dr *distinctRowReader) Columns() ([]*ColDescriptor, error) {
	return dr.rowReader.Columns()
}

func (dr *distinctRowReader) colsBySelector() (map[string]*ColDescriptor, error) {
	return dr.rowReader.colsBySelector()
}

func (dr *distinctRowReader) InferParameters(params map[string]SQLValueType) error {
	return dr.rowReader.InferParameters(params)
}

func (dr *distinctRowReader) Read() (*Row, error) {
	for {
		if dr.limit > 0 && uint64(len(dr.readRows)) == dr.limit {
			return nil, ErrNoMoreRows
		}

		if len(dr.readRows) == dr.e.distinctLimit {
			return nil, ErrTooManyRows
		}

		row, err := dr.rowReader.Read()
		if err != nil {
			return nil, err
		}

		digest, err := row.digest()
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
