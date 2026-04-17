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
	"strings"
)

// hashJoinTable is an in-memory hash table built by scanning the inner
// DataSource exactly once. Rows are grouped by a composite join-key so that
// subsequent outer rows can probe in O(1) instead of opening a new inner scan
// (which has high per-scan overhead in immudb's B-tree store).
//
// innerColSels holds the ordered list of inner column selectors that form the
// join key. For a simple equi-join (ON a = b) there is one entry; for a
// compound equi-join (ON a = b AND c = d) there are two, and the hash key is
// the '\x01'-separated concatenation of the individual hashJoinKey values.
type hashJoinTable struct {
	rows         map[string][]*Row // composite hash key → matching inner rows
	innerColSels []string          // inner join column selectors, in key order
	cols         []ColDescriptor   // column descriptors of the inner table
}

// compositeHashKey builds a stable string key from one or more TypedValues.
// A single-value join uses hashJoinKey directly (no separator overhead).
// A multi-value join concatenates individual keys with '\x01' as separator;
// '\x01' cannot appear in any hashJoinKey output, so there are no collisions.
func compositeHashKey(vals []TypedValue) string {
	if len(vals) == 1 {
		return hashJoinKey(vals[0])
	}
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = hashJoinKey(v)
	}
	return strings.Join(parts, "\x01")
}

// buildJoinHashTable performs a single full scan of ds, grouping rows by the
// composite value of innerColSels. Rows where any join-key column is missing
// are skipped.
//
// innerWhere, when non-nil, is applied as a WHERE clause to the scan. It must
// reference only columns of ds (i.e. be inner-only and stable across outer
// rows) — used by D7 predicate pushdown to filter inner rows at scan time.
//
// The DataSource is wrapped in a SelectStmt so that genScanSpecs() produces
// valid ScanSpecs (with a concrete Index). Calling tableRef.Resolve(nil)
// directly causes ErrIllegalArguments because newRawRowReader requires
// non-nil ScanSpecs with a non-nil Index.
func buildJoinHashTable(
	ctx context.Context,
	tx *SQLTx,
	ds DataSource,
	innerColSels []string,
	innerWhere ValueExp,
	params map[string]interface{},
) (*hashJoinTable, error) {
	fullScan := &SelectStmt{ds: ds, where: innerWhere}
	reader, err := fullScan.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	cols, err := reader.Columns(ctx)
	if err != nil {
		return nil, err
	}

	ht := &hashJoinTable{
		rows:         make(map[string][]*Row),
		innerColSels: innerColSels,
		cols:         cols,
	}

	for {
		row, err := reader.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		if err != nil {
			return nil, err
		}

		vals := make([]TypedValue, 0, len(innerColSels))
		allFound := true
		for _, sel := range innerColSels {
			val, ok := row.ValuesBySelector[sel]
			if !ok {
				allFound = false
				break
			}
			vals = append(vals, val)
		}
		if !allFound {
			continue
		}

		// immudb treats NULL = NULL as equal (NullValue.Compare returns 0 when
		// both sides are nil), so we include NULL-keyed rows in the hash table.
		key := compositeHashKey(vals)
		ht.rows[key] = append(ht.rows[key], row)
	}

	return ht, nil
}

// hashJoinKey returns a string map key for a TypedValue.
// The type tag prevents collisions between, e.g., Integer(1) and Bool(true).
func hashJoinKey(val TypedValue) string {
	raw := val.RawValue()
	// []byte (Blob) is not fmt-printable in a stable way, so handle it explicitly.
	if b, ok := raw.([]byte); ok {
		return "blob\x00" + string(b)
	}
	return fmt.Sprintf("%T\x00%v", raw, raw)
}

// extractEquiJoinPairs detects whether a (reduced) join condition consists
// entirely of equi-join predicates connected by AND, each comparing a concrete
// outer TypedValue to an inner ColSelector.
//
// After reduceSelectors, outer columns become concrete TypedValues while inner
// columns remain *ColSelectors. Returns the parallel slices (outerVals,
// innerSels, true) on success, or (nil, nil, false) if any part of the
// condition is not a simple equi-join.
//
// Examples of what this recognises:
//
//	ON a.id = b.id          →  single pair
//	ON a.id = b.id AND a.x = b.x  →  two pairs (compound equi-join)
func extractEquiJoinPairs(reduced ValueExp) (outerVals []TypedValue, innerSels []string, ok bool) {
	switch e := reduced.(type) {
	case *CmpBoolExp:
		if e.op != EQ {
			return nil, nil, false
		}
		ov, is, pairOk := extractSingleEquiPair(e)
		if !pairOk {
			return nil, nil, false
		}
		return []TypedValue{ov}, []string{is}, true

	case *BinBoolExp:
		if e.op != And {
			return nil, nil, false
		}
		leftVals, leftSels, leftOk := extractEquiJoinPairs(e.left)
		if !leftOk {
			return nil, nil, false
		}
		rightVals, rightSels, rightOk := extractEquiJoinPairs(e.right)
		if !rightOk {
			return nil, nil, false
		}
		return append(leftVals, rightVals...), append(leftSels, rightSels...), true
	}

	return nil, nil, false
}

// extractEquiJoinPlan splits a (reduced) join condition into equi-pairs and
// an inner-only residual filter. innerAlias is the inner table's alias.
//
// A leaf is classified as:
//   - equi-pair: CmpBoolExp(op=EQ) where one side is a TypedValue (reduced
//     outer ref) and the other is *ColSelector (inner col).
//   - inner-only: contains only ColSelector references whose explicit table
//     == innerAlias, plus TypedValues / Params. Stable across outer rows.
//   - anything else: returns ok=false (e.g. mixed-table non-equi, outer-only
//     post-reduce, subquery, EXISTS).
//
// Returns (outerVals, innerSels) for the equi-pairs (parallel slices) and
// innerResidual as the AND of inner-only conjuncts (or nil). ok=false when
// the cond cannot be cleanly classified — caller falls back to slow path.
func extractEquiJoinPlan(reduced ValueExp, innerAlias string) (
	outerVals []TypedValue,
	innerSels []string,
	innerResidual ValueExp,
	ok bool,
) {
	var residuals []ValueExp
	if !extractEquiJoinPlanRec(reduced, innerAlias, &outerVals, &innerSels, &residuals) {
		return nil, nil, nil, false
	}
	if len(outerVals) == 0 {
		return nil, nil, nil, false
	}
	innerResidual = andConjuncts(residuals)
	return outerVals, innerSels, innerResidual, true
}

func extractEquiJoinPlanRec(
	e ValueExp,
	innerAlias string,
	outerVals *[]TypedValue,
	innerSels *[]string,
	residuals *[]ValueExp,
) bool {
	if be, ok := e.(*BinBoolExp); ok && be.op == And {
		return extractEquiJoinPlanRec(be.left, innerAlias, outerVals, innerSels, residuals) &&
			extractEquiJoinPlanRec(be.right, innerAlias, outerVals, innerSels, residuals)
	}

	if cmp, ok := e.(*CmpBoolExp); ok && cmp.op == EQ {
		if ov, is, pairOk := extractSingleEquiPair(cmp); pairOk {
			*outerVals = append(*outerVals, ov)
			*innerSels = append(*innerSels, is)
			return true
		}
	}

	tables, hasUnqualified, safe := collectColTables(e)
	if !safe || hasUnqualified {
		return false
	}
	if len(tables) == 1 {
		for t := range tables {
			if t == innerAlias {
				*residuals = append(*residuals, e)
				return true
			}
		}
	}

	return false
}

// extractSingleEquiPair extracts a single (outerVal, innerSel) pair from a
// CmpBoolExp with op == EQ where exactly one side is a concrete TypedValue
// (the reduced outer column) and the other is a *ColSelector (inner column).
func extractSingleEquiPair(cmp *CmpBoolExp) (outerVal TypedValue, innerSel string, ok bool) {
	if lv, isTV := cmp.left.(TypedValue); isTV {
		if rsel, isSel := cmp.right.(*ColSelector); isSel {
			return lv, rsel.Selector(), true
		}
	}
	if rv, isTV := cmp.right.(TypedValue); isTV {
		if lsel, isSel := cmp.left.(*ColSelector); isSel {
			return rv, lsel.Selector(), true
		}
	}
	return nil, "", false
}

// hashProbeReader is a minimal RowReader that serves pre-matched rows from a
// hash table probe. It replaces the per-outer-row inner scan in a hash join,
// eliminating the expensive Resolve() + B-tree-seek overhead for each outer row.
type hashProbeReader struct {
	rows   []*Row
	pos    int
	cols   []ColDescriptor
	tx     *SQLTx
	params map[string]interface{}
}

func newHashProbeReader(rows []*Row, cols []ColDescriptor, tx *SQLTx, params map[string]interface{}) *hashProbeReader {
	if rows == nil {
		rows = []*Row{}
	}
	return &hashProbeReader{rows: rows, cols: cols, tx: tx, params: params}
}

// hashProbeReader implements RowReader. Most methods are stubs since the reader
// only needs to serve pre-fetched rows inside jointRowReader.Read().
func (r *hashProbeReader) onClose(_ func())                   {}
func (r *hashProbeReader) Tx() *SQLTx                         { return r.tx }
func (r *hashProbeReader) TableAlias() string                 { return "" }
func (r *hashProbeReader) OrderBy() []ColDescriptor           { return nil }
func (r *hashProbeReader) ScanSpecs() *ScanSpecs              { return &ScanSpecs{} }
func (r *hashProbeReader) Close() error                       { return nil }
func (r *hashProbeReader) Parameters() map[string]interface{} { return r.params }

func (r *hashProbeReader) Columns(_ context.Context) ([]ColDescriptor, error) {
	return r.cols, nil
}

func (r *hashProbeReader) colsBySelector(_ context.Context) (map[string]ColDescriptor, error) {
	ret := make(map[string]ColDescriptor, len(r.cols))
	for _, c := range r.cols {
		ret[c.Selector()] = c
	}
	return ret, nil
}

func (r *hashProbeReader) colsByPos(_ context.Context) ([]ColDescriptor, error) {
	return r.cols, nil
}

func (r *hashProbeReader) InferParameters(_ context.Context, _ map[string]SQLValueType) error {
	return nil
}

func (r *hashProbeReader) Read(_ context.Context) (*Row, error) {
	if r.pos >= len(r.rows) {
		return nil, ErrNoMoreRows
	}
	row := r.rows[r.pos]
	r.pos++
	return row, nil
}
