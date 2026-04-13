/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

// countingRowReader handles bare COUNT(*) queries (SELECT COUNT(*) FROM tbl
// with no WHERE, JOINs, GROUP BY, or HAVING) without decoding column values.
// It wraps a rawRowReader and returns exactly one Row containing the count.
type countingRowReader struct {
	rawReader *rawRowReader
	// encSel is the encoded selector key expected by projectedRowReader, e.g.
	// "COUNT()(t.*)" for a table aliased as "t".
	encSel  string
	colDesc ColDescriptor
	done    bool
}

func newCountingRowReader(rawReader *rawRowReader, agg *AggColSelector) *countingRowReader {
	aggFn, table, col := agg.resolve(rawReader.tableAlias)
	encSel := EncodeSelector(aggFn, table, col)
	return &countingRowReader{
		rawReader: rawReader,
		encSel:    encSel,
		colDesc: ColDescriptor{
			AggFn:  aggFn,
			Table:  table,
			Column: col,
			Type:   IntegerType,
		},
	}
}

func (cr *countingRowReader) onClose(callback func()) {
	cr.rawReader.onClose(callback)
}

func (cr *countingRowReader) Tx() *SQLTx {
	return cr.rawReader.Tx()
}

func (cr *countingRowReader) TableAlias() string {
	return cr.rawReader.TableAlias()
}

func (cr *countingRowReader) Parameters() map[string]interface{} {
	return cr.rawReader.Parameters()
}

func (cr *countingRowReader) OrderBy() []ColDescriptor {
	return nil
}

func (cr *countingRowReader) ScanSpecs() *ScanSpecs {
	return cr.rawReader.ScanSpecs()
}

func (cr *countingRowReader) Columns(_ context.Context) ([]ColDescriptor, error) {
	return []ColDescriptor{cr.colDesc}, nil
}

// colsBySelector includes both the COUNT(*) aggregation descriptor and all
// raw table column descriptors so that projectedRowReader validation succeeds.
func (cr *countingRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	colsBySel, err := cr.rawReader.colsBySelector(ctx)
	if err != nil {
		return nil, err
	}
	colsBySel[cr.encSel] = cr.colDesc
	return colsBySel, nil
}

func (cr *countingRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return cr.rawReader.InferParameters(ctx, params)
}

// Read counts all matching index entries without decoding column values and
// returns a single Row. Subsequent calls return ErrNoMoreRows.
func (cr *countingRowReader) Read(ctx context.Context) (*Row, error) {
	if cr.done {
		return nil, ErrNoMoreRows
	}
	cr.done = true

	n, err := cr.rawReader.CountAll(ctx)
	if err != nil {
		return nil, err
	}

	val := &Integer{val: n}
	return &Row{
		ValuesByPosition: []TypedValue{val},
		ValuesBySelector: map[string]TypedValue{cr.encSel: val},
	}, nil
}

func (cr *countingRowReader) Close() error {
	return cr.rawReader.Close()
}
