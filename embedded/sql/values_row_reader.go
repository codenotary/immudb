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

import "fmt"

type valuesRowReader struct {
	tx        *SQLTx
	colsByPos []ColDescriptor
	colsBySel map[string]ColDescriptor

	dbAlias    string
	tableAlias string

	values [][]ValueExp
	read   int

	params map[string]interface{}

	onCloseCallback func()
	closed          bool
}

func newValuesRowReader(tx *SQLTx, cols []ColDescriptor, dbAlias, tableAlias string, values [][]ValueExp) (*valuesRowReader, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("%w: empty column list", ErrIllegalArguments)
	}

	if dbAlias == "" {
		return nil, fmt.Errorf("%w: db alias is mandatory", ErrIllegalArguments)
	}

	if tableAlias == "" {
		return nil, fmt.Errorf("%w: table alias is mandatory", ErrIllegalArguments)
	}

	colsByPos := make([]ColDescriptor, len(cols))
	colsBySel := make(map[string]ColDescriptor, len(cols))

	for i, c := range cols {
		if c.AggFn != "" || c.Database != "" || c.Table != "" {
			return nil, fmt.Errorf("%w: only column name may be specified", ErrIllegalArguments)
		}

		col := ColDescriptor{
			Database: dbAlias,
			Table:    tableAlias,
			Column:   c.Column,
			Type:     c.Type,
		}

		colsByPos[i] = col
		colsBySel[col.Selector()] = col
	}

	for _, vs := range values {
		if len(cols) != len(vs) {
			return nil, ErrInvalidNumberOfValues
		}
	}

	return &valuesRowReader{
		tx:         tx,
		colsByPos:  colsByPos,
		colsBySel:  colsBySel,
		dbAlias:    dbAlias,
		tableAlias: tableAlias,
		values:     values,
	}, nil
}

func (vr *valuesRowReader) onClose(callback func()) {
	vr.onCloseCallback = callback
}

func (vr *valuesRowReader) Tx() *SQLTx {
	return vr.tx
}

func (vr *valuesRowReader) Database() string {
	if vr.dbAlias == "" {
		return vr.tx.currentDB.name
	}

	return vr.dbAlias
}

func (vr *valuesRowReader) TableAlias() string {
	return vr.tableAlias
}

func (vr *valuesRowReader) SetParameters(params map[string]interface{}) error {
	params, err := normalizeParams(params)
	if err != nil {
		return err
	}

	vr.params = params

	return nil
}

func (vr *valuesRowReader) Parameters() map[string]interface{} {
	return vr.params
}

func (vr *valuesRowReader) OrderBy() []ColDescriptor {
	return nil
}

func (vr *valuesRowReader) ScanSpecs() *ScanSpecs {
	return nil
}

func (vr *valuesRowReader) Columns() ([]ColDescriptor, error) {
	return vr.colsByPos, nil
}

func (vr *valuesRowReader) colsBySelector() (map[string]ColDescriptor, error) {
	return vr.colsBySel, nil
}

func (vr *valuesRowReader) InferParameters(params map[string]SQLValueType) error {
	for _, vs := range vr.values {
		for _, v := range vs {
			v.inferType(vr.colsBySel, params, vr.dbAlias, vr.tableAlias)
		}
	}
	return nil
}

func (vr *valuesRowReader) Read() (*Row, error) {
	if vr.read == len(vr.values) {
		return nil, ErrNoMoreRows
	}

	vs := vr.values[vr.read]

	valuesByPosition := make([]TypedValue, len(vs))
	valuesBySelector := make(map[string]TypedValue, len(vs))

	for i, v := range vs {
		sv, err := v.substitute(vr.params)
		if err != nil {
			return nil, err
		}

		rv, err := sv.reduce(vr.tx.catalog, nil, vr.dbAlias, vr.tableAlias)
		if err != nil {
			return nil, err
		}

		err = rv.requiresType(vr.colsByPos[i].Type, vr.colsBySel, nil, vr.dbAlias, vr.tableAlias)
		if err != nil {
			return nil, err
		}

		valuesByPosition[i] = rv
		valuesBySelector[vr.colsByPos[i].Selector()] = rv
	}

	row := &Row{
		ValuesByPosition: valuesByPosition,
		ValuesBySelector: valuesBySelector,
	}

	vr.read++

	return row, nil
}

func (vr *valuesRowReader) Close() error {
	if vr.closed {
		return ErrAlreadyClosed
	}

	vr.closed = true

	if vr.onCloseCallback != nil {
		vr.onCloseCallback()
	}

	return nil
}
