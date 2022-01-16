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

import (
	"errors"
)

var errDummy = errors.New("dummy error")

type dummyRowReader struct {
	failReturningColumns bool
	failInferringParams  bool
	database             string
	params               map[string]interface{}
}

func (r *dummyRowReader) onClose(callback func()) {
}

func (r *dummyRowReader) Tx() *SQLTx {
	return nil
}

func (r *dummyRowReader) Database() string {
	return r.database
}

func (r *dummyRowReader) TableAlias() string {
	return "table1"
}

func (r *dummyRowReader) Read() (*Row, error) {
	return nil, errDummy
}

func (r *dummyRowReader) Close() error {
	return errDummy
}

func (r *dummyRowReader) OrderBy() []ColDescriptor {
	return nil
}

func (r *dummyRowReader) ScanSpecs() *ScanSpecs {
	return nil
}

func (r *dummyRowReader) Columns() ([]ColDescriptor, error) {
	if r.failReturningColumns {
		return nil, errDummy
	}

	return nil, nil
}

func (r *dummyRowReader) Parameters() map[string]interface{} {
	return r.params
}

func (r *dummyRowReader) SetParameters(params map[string]interface{}) error {
	r.params = params
	return nil
}

func (r *dummyRowReader) InferParameters(params map[string]SQLValueType) error {
	if r.failInferringParams {
		return errDummy
	}

	return nil
}

func (r *dummyRowReader) colsBySelector() (map[string]ColDescriptor, error) {
	return nil, errDummy
}
