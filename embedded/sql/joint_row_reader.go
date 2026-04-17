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
	"context"
	"fmt"

	"github.com/codenotary/immudb/embedded/multierr"
)

type jointRowReader struct {
	rowReader RowReader

	joins []*JoinSpec

	rowReaders                 []RowReader
	rowReadersValuesByPosition [][]TypedValue
	rowReadersValuesBySelector []map[string]TypedValue

	// outerPoppedDuringRead is set when Read() pops jointr.rowReader off
	// the active rowReaders stack at end-of-iteration. We deliberately
	// leave the outer reader open until Close() runs: wrapping readers
	// such as groupedRowReader.emitCurrentRow → zeroRow → colsBySelector
	// reach back into jointr.rowReader after iteration ends to introspect
	// the join's column list, and closing the outer mid-iteration trips
	// "already closed" on that path for empty-result COUNT(*) over JOIN.
	outerPoppedDuringRead bool

	// hashJoin caches per-join hash-table state. Populated lazily on the
	// first outer row reaching each join level. nil entry means hash join
	// is not in use for that join (either disabled, unsupported shape, or
	// build failed — fall back to the per-row Resolve path).
	hashTables      []*hashJoinTable
	hashJoinChecked []bool
}

func newJointRowReader(rowReader RowReader, joins []*JoinSpec) (*jointRowReader, error) {
	if rowReader == nil || len(joins) == 0 {
		return nil, ErrIllegalArguments
	}

	for _, jspec := range joins {
		switch jspec.joinType {
		case InnerJoin, LeftJoin, CrossJoin, FullOuterJoin:
		default:
			return nil, ErrUnsupportedJoinType
		}
	}

	return &jointRowReader{
		rowReader:                  rowReader,
		joins:                      joins,
		rowReaders:                 []RowReader{rowReader},
		rowReadersValuesByPosition: make([][]TypedValue, 1+len(joins)),
		rowReadersValuesBySelector: make([]map[string]TypedValue, 1+len(joins)),
		hashTables:                 make([]*hashJoinTable, len(joins)),
		hashJoinChecked:            make([]bool, len(joins)),
	}, nil
}

func (jointr *jointRowReader) onClose(callback func()) {
	jointr.rowReader.onClose(callback)
}

func (jointr *jointRowReader) Tx() *SQLTx {
	return jointr.rowReader.Tx()
}

func (jointr *jointRowReader) TableAlias() string {
	return jointr.rowReader.TableAlias()
}

func (jointr *jointRowReader) OrderBy() []ColDescriptor {
	return jointr.rowReader.OrderBy()
}

func (jointr *jointRowReader) ScanSpecs() *ScanSpecs {
	return jointr.rowReader.ScanSpecs()
}

func (jointr *jointRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return jointr.colsByPos(ctx)
}

func (jointr *jointRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	colDescriptors, err := jointr.rowReader.colsBySelector(ctx)
	if err != nil {
		return nil, err
	}

	jointDescriptors := make(map[string]ColDescriptor, len(colDescriptors))
	for sel, desc := range colDescriptors {
		jointDescriptors[sel] = desc
	}

	for _, jspec := range jointr.joins {
		// TODO (byo) optimize this by getting selector list only or opening all joint readers
		//            on jointRowReader creation,
		// Note: We're using a dummy ScanSpec object that is only used during read, we're only interested
		//       in column list though
		rr, err := jspec.ds.Resolve(ctx, jointr.Tx(), nil, &ScanSpecs{Index: &Index{}})
		if err != nil {
			return nil, err
		}
		defer rr.Close()

		cd, err := rr.colsBySelector(ctx)
		if err != nil {
			return nil, err
		}

		for sel, des := range cd {
			if _, exists := jointDescriptors[sel]; exists {
				return nil, fmt.Errorf(
					"error resolving '%s' in a join: %w, "+
						"use aliasing to assign unique names "+
						"for all tables, sub-queries and columns",
					sel,
					ErrAmbiguousSelector,
				)
			}
			jointDescriptors[sel] = des
		}
	}
	return jointDescriptors, nil
}

func (jointr *jointRowReader) colsByPos(ctx context.Context) ([]ColDescriptor, error) {
	colDescriptors, err := jointr.rowReader.Columns(ctx)
	if err != nil {
		return nil, err
	}

	for _, jspec := range jointr.joins {

		// TODO (byo) optimize this by getting selector list only or opening all joint readers
		//            on jointRowReader creation,
		// Note: We're using a dummy ScanSpec object that is only used during read, we're only interested
		//       in column list though
		rr, err := jspec.ds.Resolve(ctx, jointr.Tx(), nil, &ScanSpecs{Index: &Index{}})
		if err != nil {
			return nil, err
		}
		defer rr.Close()

		cd, err := rr.Columns(ctx)
		if err != nil {
			return nil, err
		}

		colDescriptors = append(colDescriptors, cd...)
	}

	return colDescriptors, nil
}

func (jointr *jointRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	err := jointr.rowReader.InferParameters(ctx, params)
	if err != nil {
		return err
	}

	cols, err := jointr.colsBySelector(ctx)
	if err != nil {
		return err
	}

	for _, join := range jointr.joins {
		err = join.ds.inferParameters(ctx, jointr.Tx(), params)
		if err != nil {
			return err
		}

		_, err = join.cond.inferType(cols, params, jointr.TableAlias())
		if err != nil {
			return err
		}
	}
	return err
}

func (jointr *jointRowReader) Parameters() map[string]interface{} {
	return jointr.rowReader.Parameters()
}

func (jointr *jointRowReader) Read(ctx context.Context) (row *Row, err error) {
	for {
		row := &Row{
			ValuesByPosition: make([]TypedValue, 0),
			ValuesBySelector: make(map[string]TypedValue),
		}

		for len(jointr.rowReaders) > 0 {
			lastReader := jointr.rowReaders[len(jointr.rowReaders)-1]

			r, err := lastReader.Read(ctx)
			if err == ErrNoMoreRows {
				// previous reader will need to read next row
				jointr.rowReaders = jointr.rowReaders[:len(jointr.rowReaders)-1]

				// Close inner join readers immediately to release their
				// resources, but leave the outer reader (== jointr.rowReader)
				// open until jointRowReader.Close runs. Wrapping readers
				// (groupedRowReader.emitCurrentRow → zeroRow → colsBySelector)
				// read back the outer's columns after iteration exhausts,
				// and an early Close there trips "already closed" on the
				// empty-result COUNT(*) over JOIN path that Gitea's
				// GetIssueStats hits on the issues page.
				if lastReader == jointr.rowReader {
					jointr.outerPoppedDuringRead = true
				} else {
					err = lastReader.Close()
					if err != nil {
						return nil, err
					}
				}

				continue
			}
			if err != nil {
				return nil, err
			}

			// override row data
			jointr.rowReadersValuesByPosition[len(jointr.rowReaders)-1] = r.ValuesByPosition
			jointr.rowReadersValuesBySelector[len(jointr.rowReaders)-1] = r.ValuesBySelector

			break
		}

		if len(jointr.rowReaders) == 0 {
			return nil, ErrNoMoreRows
		}

		// append values from readers
		for i := 0; i < len(jointr.rowReaders); i++ {
			row.ValuesByPosition = append(row.ValuesByPosition, jointr.rowReadersValuesByPosition[i]...)

			for c, v := range jointr.rowReadersValuesBySelector[i] {
				row.ValuesBySelector[c] = v
			}
		}

		unsolvedFK := false

		for i := len(jointr.rowReaders) - 1; i < len(jointr.joins); i++ {
			jspec := jointr.joins[i]

			ds := jspec.ds
			where := jspec.cond.reduceSelectors(row, jointr.TableAlias())

			// For LATERAL joins, reduce the subquery's internal WHERE with outer row values
			if jspec.lateral {
				if selStmt, ok := ds.(*SelectStmt); ok {
					lateralDS := *selStmt
					if lateralDS.where != nil {
						lateralDS.where = lateralDS.where.reduceSelectors(row, jointr.TableAlias())
					}
					ds = &lateralDS
				}
			}

			// Hash-join fast path: for non-correlated INNER equi-joins, build the
			// inner hash table once on first outer row, then probe per outer row
			// instead of opening a fresh inner scan. Falls back transparently to
			// the per-row Resolve path on any unsupported shape, build error, or
			// if jointr.tryHashProbe returns false.
			if probed, reader, r, err := jointr.tryHashProbe(ctx, i, jspec, ds, where); err != nil {
				return nil, err
			} else if probed {
				if r == nil {
					// Hash table existed but no inner row matched this outer.
					// INNER JOIN: backtrack so the outer reader advances.
					unsolvedFK = true
					break
				}

				jointr.rowReaders = append(jointr.rowReaders, reader)
				jointr.rowReadersValuesByPosition[i+1] = r.ValuesByPosition
				jointr.rowReadersValuesBySelector[i+1] = r.ValuesBySelector

				row.ValuesByPosition = append(row.ValuesByPosition, r.ValuesByPosition...)
				for c, v := range r.ValuesBySelector {
					row.ValuesBySelector[c] = v
				}
				continue
			}

			jointq := &SelectStmt{
				ds:      ds,
				where:   where,
				indexOn: jspec.indexOn,
			}

			reader, err := jointq.Resolve(ctx, jointr.Tx(), jointr.Parameters(), nil)
			if err != nil {
				return nil, err
			}

			r, err := reader.Read(ctx)
			if err == ErrNoMoreRows {
				if jspec.joinType == InnerJoin {
					// previous reader will need to read next row
					unsolvedFK = true

					err = reader.Close()
					if err != nil {
						return nil, err
					}

					break
				} else { // LEFT JOIN: fill column values with NULLs
					cols, err := reader.Columns(ctx)
					if err != nil {
						return nil, err
					}

					r = &Row{
						ValuesByPosition: make([]TypedValue, len(cols)),
						ValuesBySelector: make(map[string]TypedValue, len(cols)),
					}

					for i, col := range cols {
						nullValue := NewNull(col.Type)

						r.ValuesByPosition[i] = nullValue
						r.ValuesBySelector[col.Selector()] = nullValue
					}
				}
			} else if err != nil {
				reader.Close()
				return nil, err
			}

			// progress with the joint readers
			// append the reader and kept the values for following rows
			jointr.rowReaders = append(jointr.rowReaders, reader)
			jointr.rowReadersValuesByPosition[i+1] = r.ValuesByPosition
			jointr.rowReadersValuesBySelector[i+1] = r.ValuesBySelector

			row.ValuesByPosition = append(row.ValuesByPosition, r.ValuesByPosition...)

			for c, v := range r.ValuesBySelector {
				row.ValuesBySelector[c] = v
			}
		}

		// all readers have a valid read
		if !unsolvedFK {
			return row, nil
		}
	}
}

// tryHashProbe attempts the hash-join fast path for join index i with the
// already-reduced join condition for the current outer row. It returns
// (probed=true, reader, row, nil) when a hashProbeReader was built and a
// first row pulled, (probed=true, nil, nil, nil) when the outer row has no
// matching inner rows, and (probed=false, nil, nil, nil) when the join is
// not eligible (LATERAL, NATURAL, non-INNER, non-equi cond, etc.) so the
// caller falls back to the existing Resolve-per-row path.
//
// The hash table is built once per join level on first eligible call and
// cached on jointr.hashTables[i]. Build errors degrade to the slow path
// without surfacing — correctness must always win over the optimization.
//
// When the join cond was enriched by D7 with inner-only residual predicates,
// extractEquiJoinPlan separates them out and they are applied as a WHERE
// filter at hash-build time so D1 + D7 compose correctly.
func (jointr *jointRowReader) tryHashProbe(
	ctx context.Context,
	i int,
	jspec *JoinSpec,
	ds DataSource,
	reducedWhere ValueExp,
) (probed bool, reader RowReader, r *Row, err error) {
	if jspec.lateral || jspec.natural || jspec.joinType != InnerJoin {
		return false, nil, nil, nil
	}

	// We need the inner table's alias to classify ColSelector references in
	// extractEquiJoinPlan. Currently we only support simple *tableRef
	// sources; subqueries / values rows fall back to the slow path.
	tref, ok := ds.(*tableRef)
	if !ok {
		return false, nil, nil, nil
	}
	innerAlias := tref.Alias()

	if !jointr.hashJoinChecked[i] {
		jointr.hashJoinChecked[i] = true

		_, innerSels, innerResidual, planOk := extractEquiJoinPlan(reducedWhere, innerAlias)
		if !planOk {
			return false, nil, nil, nil
		}

		ht, buildErr := buildJoinHashTable(ctx, jointr.Tx(), ds, innerSels, innerResidual, jointr.Parameters())
		if buildErr != nil {
			// Build failure is non-fatal; fall back to the per-row path.
			return false, nil, nil, nil
		}
		jointr.hashTables[i] = ht
	}

	ht := jointr.hashTables[i]
	if ht == nil {
		return false, nil, nil, nil
	}

	outerVals, _, _, planOk := extractEquiJoinPlan(reducedWhere, innerAlias)
	if !planOk {
		// Should not happen if first call succeeded, but stay defensive.
		return false, nil, nil, nil
	}

	matched := ht.rows[compositeHashKey(outerVals)]
	if len(matched) == 0 {
		return true, nil, nil, nil
	}

	probe := newHashProbeReader(matched, ht.cols, jointr.Tx(), jointr.Parameters())

	first, readErr := probe.Read(ctx)
	if readErr != nil {
		probe.Close()
		return false, nil, nil, readErr
	}
	return true, probe, first, nil
}

func (jointr *jointRowReader) Close() error {
	merr := multierr.NewMultiErr()

	// Closing joint readers backwards - the first reader executes the onClose callback
	// thus it must be closed at the end
	for i := len(jointr.rowReaders) - 1; i >= 0; i-- {
		err := jointr.rowReaders[i].Close()
		merr.Append(err)
	}

	// Read() leaves jointr.rowReader open (not Close'd) when popping the
	// outermost reader from the active stack so wrapping readers can
	// still introspect its columns post-exhaustion. Close it here so
	// the tx state is released.
	if jointr.outerPoppedDuringRead {
		merr.Append(jointr.rowReader.Close())
	}

	return merr.Reduce()
}
