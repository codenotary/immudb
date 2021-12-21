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

type conditionalRowReader struct {
	rowReader RowReader

	condition ValueExp

	params map[string]interface{}
}

func newConditionalRowReader(rowReader RowReader, condition ValueExp, params map[string]interface{}) (*conditionalRowReader, error) {
	return &conditionalRowReader{
		rowReader: rowReader,
		condition: condition,
		params:    params,
	}, nil
}

func (cr *conditionalRowReader) onClose(callback func()) {
	cr.rowReader.onClose(callback)
}

func (cr *conditionalRowReader) Tx() *SQLTx {
	return cr.rowReader.Tx()
}

func (cr *conditionalRowReader) Database() *Database {
	return cr.rowReader.Database()
}

func (cr *conditionalRowReader) TableAlias() string {
	return cr.rowReader.TableAlias()
}

func (cr *conditionalRowReader) SetParameters(params map[string]interface{}) error {
	err := cr.rowReader.SetParameters(params)
	if err != nil {
		return err
	}

	cr.params, err = normalizeParams(params)

	return err
}

func (cr *conditionalRowReader) OrderBy() []ColDescriptor {
	return cr.rowReader.OrderBy()
}

func (cr *conditionalRowReader) ScanSpecs() *ScanSpecs {
	return cr.rowReader.ScanSpecs()
}

func (cr *conditionalRowReader) Columns() ([]ColDescriptor, error) {
	return cr.rowReader.Columns()
}

func (cr *conditionalRowReader) colsBySelector() (map[string]ColDescriptor, error) {
	return cr.rowReader.colsBySelector()
}

func (cr *conditionalRowReader) InferParameters(params map[string]SQLValueType) error {
	err := cr.rowReader.InferParameters(params)
	if err != nil {
		return err
	}

	cols, err := cr.colsBySelector()
	if err != nil {
		return err
	}

	_, err = cr.condition.inferType(cols, params, cr.Database().Name(), cr.TableAlias())

	return err
}

func (cr *conditionalRowReader) Read() (*Row, error) {
	for {
		row, err := cr.rowReader.Read()
		if err != nil {
			return nil, err
		}

		cond, err := cr.condition.substitute(cr.params)
		if err != nil {
			return nil, err
		}

		r, err := cond.reduce(cr.Tx().catalog, row, cr.rowReader.Database().Name(), cr.rowReader.TableAlias())
		if err != nil {
			return nil, err
		}

		nval, isNull := r.(*NullValue)
		if isNull && nval.Type() == BooleanType {
			continue
		}

		satisfies, boolExp := r.(*Bool)
		if !boolExp {
			return nil, ErrInvalidCondition
		}

		if satisfies.val {
			return row, err
		}
	}
}

func (cr *conditionalRowReader) Close() error {
	return cr.rowReader.Close()
}
