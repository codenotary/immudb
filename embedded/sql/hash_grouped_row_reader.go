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

import "context"

// hashGroupedRowReader implements GROUP BY aggregation using a hash map
// rather than requiring sorted input.  It reads all inner rows in a single
// pass, accumulates aggregations per group, then streams results in insertion
// order.
//
// Used when the scan index does not provide GROUP BY order (i.e.
// scanSpecs.groupBySortExps is non-empty), replacing the sort+groupedRowReader
// pipeline with a single hash-aggregate phase.
type hashGroupedRowReader struct {
	// Embeds groupedRowReader to reuse column-descriptor setup,
	// aggregation init/update helpers, and all RowReader interface methods.
	// Read() is overridden below and takes precedence over the promoted one.
	*groupedRowReader

	groups  []*Row // aggregated groups, in insertion order
	nextIdx int
	built   bool
}

func newHashGroupedRowReader(
	rowReader RowReader,
	allAggregations bool,
	selectors []*AggColSelector,
	groupBy []*ColSelector,
) (*hashGroupedRowReader, error) {
	gr, err := newGroupedRowReader(rowReader, allAggregations, selectors, groupBy)
	if err != nil {
		return nil, err
	}
	return &hashGroupedRowReader{groupedRowReader: gr}, nil
}

// groupKey encodes the GROUP BY column values from row into a byte string
// used as the hash-map key.  NULL values are distinguished from non-NULL by a
// leading 0/1 byte; variable-length types carry a length prefix inside
// EncodeValue, making the concatenation collision-free.
func (h *hashGroupedRowReader) groupKey(row *Row) (string, error) {
	tableAlias := h.rowReader.TableAlias()
	var key []byte
	for _, col := range h.groupByCols {
		encSel := EncodeSelector(col.resolve(tableAlias))
		val := row.ValuesBySelector[encSel]
		if val == nil || val.IsNull() {
			key = append(key, 0) // NULL sentinel
			continue
		}
		key = append(key, 1) // non-NULL
		b, err := EncodeValue(val, val.Type(), 0)
		if err != nil {
			return "", err
		}
		key = append(key, b...)
	}
	return string(key), nil
}

// buildGroups performs one full scan of the inner reader and populates h.groups.
func (h *hashGroupedRowReader) buildGroups(ctx context.Context) error {
	order := make([]string, 0)
	groups := make(map[string]*Row)

	for {
		row, err := h.rowReader.Read(ctx)
		if err == ErrNoMoreRows {
			break
		}
		if err != nil {
			return err
		}

		key, err := h.groupKey(row)
		if err != nil {
			return err
		}

		if existing, ok := groups[key]; !ok {
			// First row for this group — initialise aggregation state.
			if err := h.initAggregations(row); err != nil {
				return err
			}
			groups[key] = row
			order = append(order, key)
		} else {
			// Subsequent row — update aggregation accumulators.
			if err := updateRow(existing, row); err != nil {
				return err
			}
		}
	}

	// When the inner scan is empty and the query is a pure aggregation
	// (no GROUP BY), return a single zero-value row — e.g. COUNT(*) → 0.
	if len(groups) == 0 && h.allAggregations && len(h.groupByCols) == 0 {
		zr, err := h.zeroRow(ctx)
		if err != nil {
			return err
		}
		h.groups = []*Row{zr}
		return nil
	}

	h.groups = make([]*Row, len(order))
	for i, key := range order {
		h.groups[i] = groups[key]
	}
	return nil
}

// Read overrides groupedRowReader.Read.  On the first call it builds the
// complete hash-aggregate result; subsequent calls stream the groups.
func (h *hashGroupedRowReader) Read(ctx context.Context) (*Row, error) {
	if !h.built {
		if err := h.buildGroups(ctx); err != nil {
			return nil, err
		}
		h.built = true
	}
	if h.nextIdx >= len(h.groups) {
		return nil, ErrNoMoreRows
	}
	row := h.groups[h.nextIdx]
	h.nextIdx++
	return row, nil
}
