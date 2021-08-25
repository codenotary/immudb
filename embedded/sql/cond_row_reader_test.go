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

import (
	"errors"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestConditionalRowReader(t *testing.T) {
	catalogStore, err := store.Open("catalog_cond_row_reader", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_cond_row_reader")

	dataStore, err := store.Open("catalog_cond_row_reader", store.DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("catalog_cond_row_reader")

	engine, err := NewEngine(catalogStore, dataStore, prefix)
	require.NoError(t, err)

	dummyr := &dummyRowReader{}

	rowReader, err := engine.newConditionalRowReader(dummyr, &Bool{val: true}, nil)
	require.NoError(t, err)

	_, err = rowReader.Columns()
	require.Equal(t, errDummy, err)

	err = rowReader.InferParameters(nil)
	require.Equal(t, errDummy, err)

	dummyr.failInferringParams = true

	err = rowReader.InferParameters(nil)
	require.Equal(t, errDummy, err)
}

var errDummy = errors.New("dummy error")

type dummyRowReader struct {
	failInferringParams bool
}

func (r *dummyRowReader) ImplicitDB() string {
	return "db1"
}

func (r *dummyRowReader) ImplicitTable() string {
	return "table1"
}

func (r *dummyRowReader) Read() (*Row, error) {
	return nil, errDummy
}

func (r *dummyRowReader) Close() error {
	return errDummy
}

func (r *dummyRowReader) OrderBy() []*ColDescriptor {
	return nil
}

func (r *dummyRowReader) ScanSpecs() *ScanSpecs {
	return nil
}

func (r *dummyRowReader) Columns() ([]*ColDescriptor, error) {
	return nil, errDummy
}

func (r *dummyRowReader) SetParameters(params map[string]interface{}) {
}

func (r *dummyRowReader) InferParameters(params map[string]SQLValueType) error {
	if r.failInferringParams {
		return errDummy
	}

	return nil
}

func (r *dummyRowReader) colsBySelector() (map[string]*ColDescriptor, error) {
	return nil, errDummy
}
