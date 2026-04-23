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

// splitAndConjuncts decomposes a (possibly nested) AND expression into a
// flat slice of conjuncts. Non-AND expressions are returned as a single-
// element slice. Used for predicate pushdown so each conjunct can be
// classified and re-located independently of the others.
func splitAndConjuncts(e ValueExp) []ValueExp {
	if e == nil {
		return nil
	}
	if be, ok := e.(*BinBoolExp); ok && be.op == And {
		return append(splitAndConjuncts(be.left), splitAndConjuncts(be.right)...)
	}
	return []ValueExp{e}
}

// andConjuncts re-assembles a slice of conjuncts into a single AND-tree.
// Returns nil for empty input.
func andConjuncts(parts []ValueExp) ValueExp {
	if len(parts) == 0 {
		return nil
	}
	res := parts[0]
	for _, p := range parts[1:] {
		res = &BinBoolExp{op: And, left: res, right: p}
	}
	return res
}

// collectColTables walks a ValueExp tree, recording the set of explicit
// table aliases referenced by *ColSelector nodes.
//
// Returns:
//
//	tables         — set of distinct non-empty table aliases referenced
//	hasUnqualified — true if any ColSelector has empty table (ambiguous in joins)
//	safe           — false on any unrecognized node type. Callers MUST treat
//	                 a non-safe expression as un-classifiable and refrain
//	                 from pushdown/relocation: a constant-folder, subquery,
//	                 aggregate, etc. could otherwise be silently moved across
//	                 a join boundary and produce wrong results.
//
// Recognized node types cover the bulk of WHERE/ON predicates seen in
// practice: comparisons, AND/OR/NOT, LIKE, IN-list, arithmetic, casts,
// column refs, parameters, and concrete TypedValues. Subqueries, EXISTS,
// function calls and aggregates intentionally fall through to safe=false.
func collectColTables(e ValueExp) (tables map[string]struct{}, hasUnqualified bool, safe bool) {
	tables = make(map[string]struct{})
	safe = collectColTablesRec(e, tables, &hasUnqualified)
	return
}

func collectColTablesRec(e ValueExp, tables map[string]struct{}, hasUnqualified *bool) bool {
	switch n := e.(type) {
	case nil:
		return true
	case *ColSelector:
		if n.table == "" {
			*hasUnqualified = true
		} else {
			tables[n.table] = struct{}{}
		}
		return true
	case *CmpBoolExp:
		return collectColTablesRec(n.left, tables, hasUnqualified) &&
			collectColTablesRec(n.right, tables, hasUnqualified)
	case *BinBoolExp:
		return collectColTablesRec(n.left, tables, hasUnqualified) &&
			collectColTablesRec(n.right, tables, hasUnqualified)
	case *NotBoolExp:
		return collectColTablesRec(n.exp, tables, hasUnqualified)
	case *LikeBoolExp:
		return collectColTablesRec(n.val, tables, hasUnqualified) &&
			collectColTablesRec(n.pattern, tables, hasUnqualified)
	case *NumExp:
		return collectColTablesRec(n.left, tables, hasUnqualified) &&
			collectColTablesRec(n.right, tables, hasUnqualified)
	case *Cast:
		return collectColTablesRec(n.val, tables, hasUnqualified)
	case *InListExp:
		if !collectColTablesRec(n.val, tables, hasUnqualified) {
			return false
		}
		for _, v := range n.values {
			if !collectColTablesRec(v, tables, hasUnqualified) {
				return false
			}
		}
		return true
	case *Param:
		return true
	case TypedValue:
		// All concrete value types (Integer, Number, Varchar, Bool,
		// NullValue, Blob, JSON, Timestamp, etc.) implement TypedValue
		// and reference no columns.
		return true
	default:
		// Unknown node type — bail. Subqueries, EXISTS clauses, function
		// calls and aggregates land here intentionally.
		return false
	}
}

// pushdownInnerOnlyConjuncts splits stmt.where, attaches conjuncts that
// reference only an INNER join's inner table to that join's cond, and
// returns the residual WHERE plus the (possibly modified) joins slice.
//
// The original stmt.where and stmt.joins are NOT mutated — a local copy
// of any modified JoinSpec is returned in joinsOut. Callers that care
// only about the WHERE/joins for this Resolve call should use the returned
// values; for cases with no eligible pushdown, this function returns the
// originals unchanged (joinsOut == joins).
//
// Eligibility per conjunct:
//   - References exactly one explicit table alias (no unqualified columns).
//   - That alias matches the *tableRef alias of an INNER, non-LATERAL,
//     non-NATURAL join's ds.
//   - All sub-expressions are recognised by collectColTables (so subqueries
//     and other unsupported forms stay in WHERE).
//
// LEFT/RIGHT/CROSS/FULL OUTER joins are skipped: pushing an inner-only
// predicate into the inner of a LEFT JOIN changes semantics (an inner
// row that fails the predicate would NULL-extend the outer row instead
// of being filtered out by the outer WHERE).
func pushdownInnerOnlyConjuncts(where ValueExp, joins []*JoinSpec) (whereOut ValueExp, joinsOut []*JoinSpec) {
	if where == nil || len(joins) == 0 {
		return where, joins
	}

	// Map inner table alias → join index for eligible (INNER, non-LATERAL,
	// non-NATURAL, *tableRef ds) joins only.
	joinByAlias := make(map[string]int, len(joins))
	for i, jspec := range joins {
		if jspec.joinType != InnerJoin || jspec.lateral || jspec.natural {
			continue
		}
		tref, ok := jspec.ds.(*tableRef)
		if !ok {
			continue
		}
		// First-write-wins on alias collision (shouldn't happen — duplicate
		// aliases are a parse-time error — but be defensive).
		if _, exists := joinByAlias[tref.Alias()]; !exists {
			joinByAlias[tref.Alias()] = i
		}
	}

	if len(joinByAlias) == 0 {
		return where, joins
	}

	conjuncts := splitAndConjuncts(where)
	residual := make([]ValueExp, 0, len(conjuncts))
	pushPerJoin := make(map[int][]ValueExp)

	for _, c := range conjuncts {
		tables, hasUnqualified, safe := collectColTables(c)
		if !safe || hasUnqualified || len(tables) != 1 {
			residual = append(residual, c)
			continue
		}
		var only string
		for t := range tables {
			only = t
		}
		if joinIdx, ok := joinByAlias[only]; ok {
			pushPerJoin[joinIdx] = append(pushPerJoin[joinIdx], c)
		} else {
			residual = append(residual, c)
		}
	}

	if len(pushPerJoin) == 0 {
		return where, joins
	}

	joinsOut = make([]*JoinSpec, len(joins))
	for i, jspec := range joins {
		extras, ok := pushPerJoin[i]
		if !ok {
			joinsOut[i] = jspec
			continue
		}
		cp := *jspec
		merged := jspec.cond
		for _, e := range extras {
			if merged == nil {
				merged = e
			} else {
				merged = &BinBoolExp{op: And, left: merged, right: e}
			}
		}
		cp.cond = merged
		joinsOut[i] = &cp
	}

	return andConjuncts(residual), joinsOut
}
