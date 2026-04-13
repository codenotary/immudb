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

import (
	"container/heap"
	"context"
	"fmt"
)

// topNSortThreshold is the maximum LIMIT value for which the top-N heap
// optimisation is applied. For larger limits the full file-sort path is used.
const topNSortThreshold = 1000

type sortDirection int8

const (
	sortDirectionDesc sortDirection = -1
	sortDirectionAsc  sortDirection = 1
)

type sortRowReader struct {
	rowReader          RowReader
	ordExps            []*OrdExp
	orderByDescriptors []ColDescriptor
	sorter             fileSorter

	resultReader resultReader

	// topNLimit, when > 0, activates the bounded heap optimisation: only the
	// top-N rows (in sort order) are retained in memory instead of sorting
	// the full result set. Set from the query LIMIT when applicable.
	topNLimit int

	// onCloseCallback fires from this reader's own Close (not from inner
	// readers'). Stored locally rather than propagated so that the join
	// machinery's mid-iteration Close on inner readers does not trigger
	// qtx.Cancel before sortRowReader.finalize finishes merging temp files.
	onCloseCallback func()
}

func newSortRowReader(rowReader RowReader, ordExps []*OrdExp) (*sortRowReader, error) {
	if rowReader == nil || len(ordExps) == 0 {
		return nil, ErrIllegalArguments
	}

	descriptors, err := rowReader.Columns(context.Background())
	if err != nil {
		return nil, err
	}

	for _, col := range ordExps {
		colPos, isColRef := col.exp.(*Integer)
		if isColRef && (colPos.val <= 0 || colPos.val > int64(len(descriptors))) {
			return nil, fmt.Errorf("position %d is not in select list", colPos.val)
		}
	}

	colPosBySelector, err := getColPositionsBySelector(descriptors)
	if err != nil {
		return nil, err
	}

	colTypes, err := getColTypes(rowReader)
	if err != nil {
		return nil, err
	}

	orderByDescriptors, err := getOrderByDescriptors(ordExps, rowReader)
	if err != nil {
		return nil, err
	}

	tx := rowReader.Tx()
	sr := &sortRowReader{
		rowReader:          rowReader,
		ordExps:            ordExps,
		orderByDescriptors: orderByDescriptors,
		sorter: fileSorter{
			colPosBySelector: colPosBySelector,
			colTypes:         colTypes,
			tx:               tx,
			sortBufSize:      tx.engine.sortBufferSize,
			sortBuf:          make([]*Row, tx.engine.sortBufferSize),
		},
	}

	directions := make([]sortDirection, len(ordExps))
	for i, col := range ordExps {
		directions[i] = sortDirectionAsc
		if col.descOrder {
			directions[i] = sortDirectionDesc
		}
	}

	t1 := make(Tuple, len(ordExps))
	t2 := make(Tuple, len(ordExps))

	nullsOrders := make([]NullsOrder, len(ordExps))
	for i, col := range ordExps {
		nullsOrders[i] = col.nullsOrder
	}

	sr.sorter.cmp = func(r1, r2 *Row) (int, error) {
		if err := sr.evalSortExps(r1, t1); err != nil {
			return 0, err
		}

		if err := sr.evalSortExps(r2, t2); err != nil {
			return 0, err
		}

		for i := range t1 {
			v1Null := t1[i] == nil || t1[i].IsNull()
			v2Null := t2[i] == nil || t2[i].IsNull()

			if v1Null && v2Null {
				continue
			}
			if v1Null || v2Null {
				nullOrder := nullsOrders[i]
				if nullOrder == NullsDefault {
					// immudb default: NULLS FIRST for ASC, NULLS LAST for DESC
					if directions[i] == sortDirectionAsc {
						nullOrder = NullsFirst
					} else {
						nullOrder = NullsLast
					}
				}
				if v1Null {
					if nullOrder == NullsFirst {
						return -1, nil
					}
					return 1, nil
				}
				if nullOrder == NullsFirst {
					return 1, nil
				}
				return -1, nil
			}

			res, err := t1[i].Compare(t2[i])
			if err != nil {
				return 0, err
			}
			if res != 0 {
				return res * int(directions[i]), nil
			}
		}
		return 0, nil
	}
	return sr, nil
}

func (s *sortRowReader) evalSortExps(inRow *Row, out Tuple) error {
	for i, col := range s.ordExps {
		colPos, isColRef := col.exp.(*Integer)
		if isColRef {
			if colPos.val < 1 || colPos.val > int64(len(inRow.ValuesByPosition)) {
				return fmt.Errorf("position %d is not in select list", colPos.val)
			}
			out[i] = inRow.ValuesByPosition[colPos.val-1]
		} else {
			val, err := col.exp.reduce(s.Tx(), inRow, s.TableAlias())
			if err != nil {
				return err
			}
			out[i] = val
		}
	}
	return nil
}

func getOrderByDescriptors(ordExps []*OrdExp, rowReader RowReader) ([]ColDescriptor, error) {
	colsBySel, err := rowReader.colsBySelector(context.Background())
	if err != nil {
		return nil, err
	}

	params := make(map[string]string)
	orderByDescriptors := make([]ColDescriptor, len(ordExps))
	for i, col := range ordExps {
		sqlType, err := col.exp.inferType(colsBySel, params, rowReader.TableAlias())
		if err != nil {
			return nil, err
		}

		if sel := col.AsSelector(); sel != nil {
			aggFn, table, col := sel.resolve(rowReader.TableAlias())
			orderByDescriptors[i] = ColDescriptor{
				AggFn:  aggFn,
				Table:  table,
				Column: col,
				Type:   sqlType,
			}
		} else {
			orderByDescriptors[i] = ColDescriptor{
				Column: col.exp.String(),
				Type:   sqlType,
			}
		}
	}
	return orderByDescriptors, nil
}

func getColTypes(r RowReader) ([]string, error) {
	descriptors, err := r.Columns(context.Background())
	if err != nil {
		return nil, err
	}

	cols := make([]string, len(descriptors))
	for i, desc := range descriptors {
		cols[i] = desc.Type
	}
	return cols, err
}

func getColPositionsBySelector(desc []ColDescriptor) (map[string]int, error) {
	colPositionsBySelector := make(map[string]int)
	for i, desc := range desc {
		colPositionsBySelector[desc.Selector()] = i
	}
	return colPositionsBySelector, nil
}

// onClose registers a callback that fires exactly once when this reader's
// own Close runs. We deliberately do NOT propagate the callback down to
// sr.rowReader because inner readers (notably jointRowReader) Close
// nested sub-readers mid-iteration; propagating would let qtx.Cancel
// fire while sortRowReader.finalize is still merging temp files. See
// joint_row_reader.go:198 and the JOIN+GROUP+ORDER regression test.
func (sr *sortRowReader) onClose(callback func()) {
	sr.onCloseCallback = callback
}

func (sr *sortRowReader) Tx() *SQLTx {
	return sr.rowReader.Tx()
}

func (sr *sortRowReader) TableAlias() string {
	return sr.rowReader.TableAlias()
}

func (sr *sortRowReader) Parameters() map[string]interface{} {
	return sr.rowReader.Parameters()
}

func (sr *sortRowReader) OrderBy() []ColDescriptor {
	return sr.orderByDescriptors
}

func (sr *sortRowReader) ScanSpecs() *ScanSpecs {
	return sr.rowReader.ScanSpecs()
}

func (sr *sortRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return sr.rowReader.Columns(ctx)
}

func (sr *sortRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return sr.rowReader.colsBySelector(ctx)
}

func (sr *sortRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return sr.rowReader.InferParameters(ctx, params)
}

func (sr *sortRowReader) Read(ctx context.Context) (*Row, error) {
	if sr.resultReader == nil {
		reader, err := sr.readAndSort(ctx)
		if err != nil {
			return nil, err
		}
		sr.resultReader = reader
	}
	return sr.resultReader.Read()
}

func (sr *sortRowReader) readAndSort(ctx context.Context) (resultReader, error) {
	if sr.topNLimit > 0 {
		return sr.readAndSortTopN(ctx)
	}
	err := sr.readAll(ctx)
	if err != nil {
		return nil, err
	}
	return sr.sorter.finalize()
}

// topNHeap is a max-heap of *Row values used by the top-N sort optimisation.
// It retains only the N rows with the smallest sort key (the rows that appear
// first in the final ORDER BY output). A max-heap lets us cheaply compare and
// evict the current worst candidate without touching the other rows.
type topNHeap struct {
	rows []*Row
	cmp  func(r1, r2 *Row) (int, error)
	err  error // first comparison error, if any
}

func (h *topNHeap) Len() int { return len(h.rows) }

// Less makes this a max-heap: the root holds the row that sorts LAST (worst).
func (h *topNHeap) Less(i, j int) bool {
	res, err := h.cmp(h.rows[i], h.rows[j])
	if err != nil && h.err == nil {
		h.err = err
	}
	return res > 0
}

func (h *topNHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

func (h *topNHeap) Push(x interface{}) { h.rows = append(h.rows, x.(*Row)) }

func (h *topNHeap) Pop() interface{} {
	old := h.rows
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	h.rows = old[:n-1]
	return x
}

// readAndSortTopN reads all inner rows into a bounded max-heap of size
// topNLimit, then extracts them in ascending sort order. This avoids the
// disk-spill path for queries like ORDER BY col LIMIT 10.
func (sr *sortRowReader) readAndSortTopN(ctx context.Context) (resultReader, error) {
	h := &topNHeap{
		cmp:  sr.sorter.cmp,
		rows: make([]*Row, 0, sr.topNLimit+1),
	}
	heap.Init(h)

	for {
		row, err := sr.rowReader.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		if err != nil {
			return nil, err
		}

		if h.Len() < sr.topNLimit {
			heap.Push(h, row)
		} else {
			// Evict the current worst row if this one is better.
			res, cmpErr := sr.sorter.cmp(row, h.rows[0])
			if cmpErr != nil {
				return nil, cmpErr
			}
			if res < 0 { // row sorts before the heap root (current worst)
				heap.Pop(h)
				heap.Push(h, row)
			}
		}
		if h.err != nil {
			return nil, h.err
		}
	}

	// Pop from max-heap: elements come out largest-first.  Reverse to get
	// ascending (correct final) order.
	n := h.Len()
	rows := make([]*Row, n)
	for i := n - 1; i >= 0; i-- {
		rows[i] = heap.Pop(h).(*Row)
	}
	return &bufferResultReader{sortBuf: rows}, nil
}

func (sr *sortRowReader) readAll(ctx context.Context) error {
	for {
		row, err := sr.rowReader.Read(ctx)
		if err == ErrNoMoreRows {
			return nil
		}

		if err != nil {
			return err
		}

		err = sr.sorter.update(row)
		if err != nil {
			return err
		}
	}
}

func (sr *sortRowReader) Close() error {
	if cb := sr.onCloseCallback; cb != nil {
		// Fire after inner Close so the tx that owns any underlying
		// resources stays alive until everyone downstream is done.
		defer cb()
		sr.onCloseCallback = nil
	}
	var resultErr error
	if sr.resultReader != nil {
		resultErr = sr.resultReader.Close()
		sr.resultReader = nil
	}
	if err := sr.rowReader.Close(); err != nil {
		return err
	}
	return resultErr
}
