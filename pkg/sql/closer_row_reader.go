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

import "github.com/codenotary/immudb/embedded/store"

type closerRowReader struct {
	e *Engine

	snap      *store.Snapshot
	rowReader RowReader
}

func (e *Engine) newCloserRowReader(snap *store.Snapshot, rowReader RowReader) (*closerRowReader, error) {
	return &closerRowReader{
		e:         e,
		snap:      snap,
		rowReader: rowReader,
	}, nil
}

func (cr *closerRowReader) ImplicitDB() string {
	return cr.rowReader.ImplicitDB()
}

func (cr *closerRowReader) ImplicitTable() string {
	return cr.rowReader.ImplicitTable()
}

func (cr *closerRowReader) Columns() (map[string]SQLValueType, error) {
	return cr.rowReader.Columns()
}

func (cr *closerRowReader) Read() (*Row, error) {
	return cr.rowReader.Read()
}

func (cr *closerRowReader) Close() error {
	defer cr.snap.Close()

	return cr.rowReader.Close()
}
