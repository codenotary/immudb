/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

import (
	"context"
	"fmt"
	"sort"
)

// windowRowReader materializes all input rows, groups them by partition key,
// sorts within each partition, computes window function values, and emits
// rows with the window columns appended.
type windowRowReader struct {
	inner RowReader

	windowFns   []*WindowFnExp
	innerCols   []ColDescriptor
	maxRows     int // 0 = unlimited

	loaded    bool
	rows      []*Row
	rowIdx    int
}

func newWindowRowReader(ctx context.Context, inner RowReader, windowFns []*WindowFnExp, maxRows int) (*windowRowReader, error) {
	innerCols, err := inner.Columns(ctx)
	if err != nil {
		return nil, err
	}

	return &windowRowReader{
		inner:     inner,
		windowFns: windowFns,
		innerCols: innerCols,
		maxRows:   maxRows,
	}, nil
}

func (wr *windowRowReader) materialize(ctx context.Context) error {
	if wr.loaded {
		return nil
	}
	wr.loaded = true

	// Read all rows (with optional limit to prevent OOM)
	var allRows []*Row
	for {
		row, err := wr.inner.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		if err != nil {
			return err
		}
		allRows = append(allRows, row)

		if wr.maxRows > 0 && len(allRows) > wr.maxRows {
			return fmt.Errorf("%w: %d rows exceed limit of %d",
				ErrWindowRowsLimitExceeded, len(allRows), wr.maxRows)
		}
	}

	if len(allRows) == 0 {
		return nil
	}

	tx := wr.inner.Tx()
	tableAlias := wr.inner.TableAlias()

	// Process each window function
	for _, wfn := range wr.windowFns {
		// Partition rows
		partitions := wr.partitionRows(allRows, wfn, tx, tableAlias)

		// Sort within each partition
		for _, partition := range partitions {
			if len(wfn.orderBy) > 0 {
				wr.sortPartition(partition, wfn, tx, tableAlias)
			}
		}

		// Compute window values
		for _, partition := range partitions {
			wr.computeWindowValues(partition, wfn, tx, tableAlias)
		}
	}

	wr.rows = allRows
	return nil
}

func (wr *windowRowReader) partitionRows(rows []*Row, wfn *WindowFnExp, tx *SQLTx, tableAlias string) [][]*Row {
	if len(wfn.partitionBy) == 0 {
		return [][]*Row{rows}
	}

	partitionMap := make(map[string][]*Row)
	var keys []string

	for _, row := range rows {
		key := wr.partitionKey(row, wfn, tx, tableAlias)
		if _, exists := partitionMap[key]; !exists {
			keys = append(keys, key)
		}
		partitionMap[key] = append(partitionMap[key], row)
	}

	partitions := make([][]*Row, 0, len(keys))
	for _, k := range keys {
		partitions = append(partitions, partitionMap[k])
	}
	return partitions
}

func (wr *windowRowReader) partitionKey(row *Row, wfn *WindowFnExp, tx *SQLTx, tableAlias string) string {
	var key string
	for _, pexp := range wfn.partitionBy {
		val, err := pexp.reduce(tx, row, tableAlias)
		if err != nil || val == nil {
			key += "NULL|"
			continue
		}
		key += fmt.Sprintf("%v|", val.RawValue())
	}
	return key
}

func (wr *windowRowReader) sortPartition(partition []*Row, wfn *WindowFnExp, tx *SQLTx, tableAlias string) {
	sort.SliceStable(partition, func(i, j int) bool {
		for _, ord := range wfn.orderBy {
			vi, erri := ord.exp.reduce(tx, partition[i], tableAlias)
			vj, errj := ord.exp.reduce(tx, partition[j], tableAlias)
			if erri != nil || errj != nil {
				continue
			}
			cmp, err := vi.Compare(vj)
			if err != nil || cmp == 0 {
				continue
			}
			if ord.descOrder {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

func (wr *windowRowReader) computeWindowValues(partition []*Row, wfn *WindowFnExp, tx *SQLTx, tableAlias string) {
	fnName := wfn.fnName
	colSel := wfn.selectorName()

	switch fnName {
	case "ROW_NUMBER":
		for i, row := range partition {
			appendWindowValue(row, colSel, NewInteger(int64(i+1)))
		}

	case "RANK":
		rank := 1
		for i, row := range partition {
			if i > 0 && !wr.orderByEqual(partition[i-1], row, wfn, tx, tableAlias) {
				rank = i + 1
			}
			appendWindowValue(row, colSel, NewInteger(int64(rank)))
		}

	case "DENSE_RANK":
		rank := 1
		for i, row := range partition {
			if i > 0 && !wr.orderByEqual(partition[i-1], row, wfn, tx, tableAlias) {
				rank++
			}
			appendWindowValue(row, colSel, NewInteger(int64(rank)))
		}

	case "COUNT":
		count := int64(len(partition))
		for _, row := range partition {
			appendWindowValue(row, colSel, NewInteger(count))
		}

	case "SUM":
		var sum float64
		for _, row := range partition {
			if len(wfn.params) > 0 {
				val, err := wfn.params[0].reduce(tx, row, tableAlias)
				if err == nil && !val.IsNull() {
					switch v := val.RawValue().(type) {
					case int64:
						sum += float64(v)
					case float64:
						sum += v
					}
				}
			}
		}
		for _, row := range partition {
			appendWindowValue(row, colSel, NewFloat64(sum))
		}

	case "MIN", "MAX":
		var result TypedValue
		for _, row := range partition {
			if len(wfn.params) > 0 {
				val, err := wfn.params[0].reduce(tx, row, tableAlias)
				if err == nil && !val.IsNull() {
					if result == nil {
						result = val
					} else {
						cmp, err := val.Compare(result)
						if err == nil {
							if (fnName == "MIN" && cmp < 0) || (fnName == "MAX" && cmp > 0) {
								result = val
							}
						}
					}
				}
			}
		}
		for _, row := range partition {
			if result != nil {
				appendWindowValue(row, colSel, result)
			} else {
				appendWindowValue(row, colSel, NewNull(IntegerType))
			}
		}

	case "AVG":
		var sum float64
		var count int64
		for _, row := range partition {
			if len(wfn.params) > 0 {
				val, err := wfn.params[0].reduce(tx, row, tableAlias)
				if err == nil && !val.IsNull() {
					switch v := val.RawValue().(type) {
					case int64:
						sum += float64(v)
						count++
					case float64:
						sum += v
						count++
					}
				}
			}
		}
		if count > 0 {
			avg := sum / float64(count)
			for _, row := range partition {
				appendWindowValue(row, colSel, NewFloat64(avg))
			}
		} else {
			for _, row := range partition {
				appendWindowValue(row, colSel, NewNull(Float64Type))
			}
		}

	case "LAG":
		for i, row := range partition {
			offset := 1
			if len(wfn.params) > 1 {
				if ov, ok := wfn.params[1].(*Integer); ok {
					offset = int(ov.val)
				}
			}
			prev := i - offset
			if prev >= 0 && prev < len(partition) && len(wfn.params) > 0 {
				val, err := wfn.params[0].reduce(tx, partition[prev], tableAlias)
				if err == nil {
					appendWindowValue(row, colSel, val)
					continue
				}
			}
			appendWindowValue(row, colSel, NewNull(AnyType))
		}

	case "LEAD":
		for i, row := range partition {
			offset := 1
			if len(wfn.params) > 1 {
				if ov, ok := wfn.params[1].(*Integer); ok {
					offset = int(ov.val)
				}
			}
			next := i + offset
			if next >= 0 && next < len(partition) && len(wfn.params) > 0 {
				val, err := wfn.params[0].reduce(tx, partition[next], tableAlias)
				if err == nil {
					appendWindowValue(row, colSel, val)
					continue
				}
			}
			appendWindowValue(row, colSel, NewNull(AnyType))
		}

	case "FIRST_VALUE":
		if len(partition) > 0 && len(wfn.params) > 0 {
			firstVal, err := wfn.params[0].reduce(tx, partition[0], tableAlias)
			if err != nil {
				firstVal = NewNull(AnyType)
			}
			for _, row := range partition {
				appendWindowValue(row, colSel, firstVal)
			}
		}

	case "LAST_VALUE":
		if len(partition) > 0 && len(wfn.params) > 0 {
			lastVal, err := wfn.params[0].reduce(tx, partition[len(partition)-1], tableAlias)
			if err != nil {
				lastVal = NewNull(AnyType)
			}
			for _, row := range partition {
				appendWindowValue(row, colSel, lastVal)
			}
		}

	case "NTILE":
		buckets := int64(1)
		if len(wfn.params) > 0 {
			if bv, ok := wfn.params[0].(*Integer); ok && bv.val > 0 {
				buckets = bv.val
			}
		}
		n := int64(len(partition))
		for i, row := range partition {
			bucket := (int64(i) * buckets / n) + 1
			appendWindowValue(row, colSel, NewInteger(bucket))
		}

	default:
		// Unknown window function — append NULL
		for _, row := range partition {
			appendWindowValue(row, colSel, NewNull(IntegerType))
		}
	}
}

func appendWindowValue(row *Row, selector string, val TypedValue) {
	row.ValuesByPosition = append(row.ValuesByPosition, val)
	row.ValuesBySelector[selector] = val
}

func (wr *windowRowReader) orderByEqual(a, b *Row, wfn *WindowFnExp, tx *SQLTx, tableAlias string) bool {
	for _, ord := range wfn.orderBy {
		va, erra := ord.exp.reduce(tx, a, tableAlias)
		vb, errb := ord.exp.reduce(tx, b, tableAlias)
		if erra != nil || errb != nil {
			return false
		}
		cmp, err := va.Compare(vb)
		if err != nil || cmp != 0 {
			return false
		}
	}
	return true
}

func (wr *windowRowReader) onClose(callback func()) {
	wr.inner.onClose(callback)
}

func (wr *windowRowReader) Tx() *SQLTx {
	return wr.inner.Tx()
}

func (wr *windowRowReader) TableAlias() string {
	return wr.inner.TableAlias()
}

func (wr *windowRowReader) OrderBy() []ColDescriptor {
	return nil
}

func (wr *windowRowReader) ScanSpecs() *ScanSpecs {
	return nil
}

func (wr *windowRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	cols := make([]ColDescriptor, len(wr.innerCols))
	copy(cols, wr.innerCols)

	for _, wfn := range wr.windowFns {
		cols = append(cols, ColDescriptor{
			Table:  wr.inner.TableAlias(),
			Column: wfn.alias,
			Type:   wfn.resultType(),
		})
	}
	return cols, nil
}

func (wr *windowRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	cols, err := wr.Columns(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[string]ColDescriptor, len(cols))
	for _, c := range cols {
		result[c.Selector()] = c
	}
	return result, nil
}

func (wr *windowRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return wr.inner.InferParameters(ctx, params)
}

func (wr *windowRowReader) Parameters() map[string]interface{} {
	return wr.inner.Parameters()
}

func (wr *windowRowReader) Read(ctx context.Context) (*Row, error) {
	if err := wr.materialize(ctx); err != nil {
		return nil, err
	}

	if wr.rowIdx >= len(wr.rows) {
		return nil, ErrNoMoreRows
	}

	row := wr.rows[wr.rowIdx]
	wr.rowIdx++
	return row, nil
}

func (wr *windowRowReader) Close() error {
	return wr.inner.Close()
}
