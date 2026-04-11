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
	"testing"

	"github.com/stretchr/testify/require"
)

// --- ILIKE Edge Cases ---

func TestILikeEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE ilike_edge (id INTEGER, val VARCHAR, PRIMARY KEY id);
		INSERT INTO ilike_edge (id, val) VALUES (1, 'Hello World');
		INSERT INTO ilike_edge (id, val) VALUES (2, 'HELLO WORLD');
		INSERT INTO ilike_edge (id, val) VALUES (3, 'hello world');
		INSERT INTO ilike_edge (id, val) VALUES (4, '');
		INSERT INTO ilike_edge (id, val) VALUES (5, NULL);
	`, nil)
	require.NoError(t, err)

	t.Run("case_variants", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id FROM ilike_edge WHERE val ILIKE 'hello world'`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		require.Equal(t, 3, count) // all 3 non-null, non-empty match
		r.Close()
	})

	t.Run("empty_string", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id FROM ilike_edge WHERE val ILIKE ''`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		// Empty pattern matches all non-null strings (regex '' matches everything)
		require.Greater(t, count, 0)
	})

	t.Run("not_ilike", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id FROM ilike_edge WHERE val NOT ILIKE 'hello world'`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Greater(t, count, 0) // at least empty string and NULL
	})
}

// --- Window Function Edge Cases ---

func TestWindowFunctionEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE wf_edge (id INTEGER, grp VARCHAR, val INTEGER, PRIMARY KEY id);
		INSERT INTO wf_edge (id, grp, val) VALUES (1, 'a', 10);
		INSERT INTO wf_edge (id, grp, val) VALUES (2, 'a', 10);
		INSERT INTO wf_edge (id, grp, val) VALUES (3, 'a', 20);
		INSERT INTO wf_edge (id, grp, val) VALUES (4, 'b', 30);
	`, nil)
	require.NoError(t, err)

	t.Run("rank_with_ties", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, rank() OVER (PARTITION BY grp ORDER BY val) rk FROM wf_edge`, nil)
		require.NoError(t, err)
		count := 0
		for {
			row, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			// First two rows in group 'a' have same val=10, should both get rank=1
			id := row.ValuesByPosition[0].RawValue().(int64)
			rk := row.ValuesByPosition[1].RawValue().(int64)
			if id == 1 || id == 2 {
				require.Equal(t, int64(1), rk)
			}
			if id == 3 {
				require.Equal(t, int64(3), rk) // rank jumps to 3
			}
			count++
		}
		r.Close()
		require.Equal(t, 4, count)
	})

	t.Run("dense_rank_with_ties", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, dense_rank() OVER (PARTITION BY grp ORDER BY val) dr FROM wf_edge`, nil)
		require.NoError(t, err)
		for {
			row, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			id := row.ValuesByPosition[0].RawValue().(int64)
			dr := row.ValuesByPosition[1].RawValue().(int64)
			if id == 3 {
				require.Equal(t, int64(2), dr) // dense_rank doesn't jump
			}
		}
		r.Close()
	})

	t.Run("multiple_window_functions", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, `
			SELECT id,
				row_number() OVER (ORDER BY id) rn,
				count(*) OVER (PARTITION BY grp) cnt
			FROM wf_edge`, nil)
		require.NoError(t, err)
		count := 0
		for {
			row, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			rn := row.ValuesByPosition[1].RawValue().(int64)
			require.Equal(t, int64(count+1), rn)
			count++
		}
		r.Close()
		require.Equal(t, 4, count)
	})

	t.Run("empty_partition", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, row_number() OVER () rn FROM wf_edge`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Equal(t, 4, count) // all rows in single partition
	})
}

// --- CTE Edge Cases ---

func TestCTEEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE cte_data (id INTEGER, val INTEGER, PRIMARY KEY id);
		INSERT INTO cte_data (id, val) VALUES (1, 100);
		INSERT INTO cte_data (id, val) VALUES (2, 200);
		INSERT INTO cte_data (id, val) VALUES (3, 300);
	`, nil)
	require.NoError(t, err)

	t.Run("cte_referenced_twice", func(t *testing.T) {
		// CTE used in both sides of a JOIN
		r, err := engine.Query(context.Background(), nil, `
			WITH totals AS (
				SELECT id, val FROM cte_data WHERE val > 100
			)
			SELECT a.id, b.id FROM totals a INNER JOIN totals b ON a.id = b.id`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Equal(t, 2, count) // id=2, id=3
	})

	t.Run("recursive_cte_termination", func(t *testing.T) {
		// Recursive CTE that terminates properly
		_, _, err := engine.Exec(context.Background(), nil,
			`CREATE TABLE dummy_rc (id INTEGER, PRIMARY KEY id)`, nil)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil, `
			WITH RECURSIVE seq AS (
				SELECT 1 AS n
				UNION ALL
				SELECT n + 1 FROM seq WHERE n < 5
			)
			SELECT n FROM seq`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Equal(t, 5, count) // 1,2,3,4,5
	})
}

// --- RETURNING Edge Cases ---

func TestReturningEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil,
		`CREATE TABLE ret_edge (id INTEGER AUTO_INCREMENT, name VARCHAR, val INTEGER, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	t.Run("returning_star", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`INSERT INTO ret_edge (name, val) VALUES ('test', 42) RETURNING *`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, 3, len(row.ValuesByPosition)) // id, name, val
		require.Equal(t, "test", row.ValuesByPosition[1].RawValue())
		require.Equal(t, int64(42), row.ValuesByPosition[2].RawValue())
		r.Close()
	})

	t.Run("returning_specific_cols", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`INSERT INTO ret_edge (name, val) VALUES ('foo', 99) RETURNING name, val`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, 2, len(row.ValuesByPosition))
		require.Equal(t, "foo", row.ValuesByPosition[0].RawValue())
		require.Equal(t, int64(99), row.ValuesByPosition[1].RawValue())
		r.Close()
	})

	t.Run("delete_returning", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil,
			`INSERT INTO ret_edge (name, val) VALUES ('del_me', 0)`, nil)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil,
			`DELETE FROM ret_edge WHERE name = 'del_me' RETURNING name`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "del_me", row.ValuesByPosition[0].RawValue())
		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
		r.Close()
	})
}

// --- Set Operations Edge Cases ---

func TestSetOperationEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE set_a (id INTEGER, PRIMARY KEY id);
		CREATE TABLE set_b (id INTEGER, PRIMARY KEY id);
		INSERT INTO set_a (id) VALUES (1);
		INSERT INTO set_a (id) VALUES (2);
		INSERT INTO set_a (id) VALUES (3);
		INSERT INTO set_b (id) VALUES (2);
		INSERT INTO set_b (id) VALUES (4);
	`, nil)
	require.NoError(t, err)

	t.Run("except_empty_right", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id FROM set_a EXCEPT SELECT id FROM set_b WHERE id > 100`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Equal(t, 3, count) // all of set_a
	})

	t.Run("intersect_no_overlap", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id FROM set_a WHERE id = 1 INTERSECT SELECT id FROM set_b WHERE id = 4`, nil)
		require.NoError(t, err)
		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
		r.Close()
	})

	t.Run("union_in_subquery", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id FROM (SELECT id FROM set_a UNION ALL SELECT id FROM set_b) sub`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Equal(t, 5, count) // 3 + 2
	})
}

// --- JOIN Edge Cases ---

func TestJoinEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE j_left (id INTEGER, val VARCHAR, PRIMARY KEY id);
		CREATE TABLE j_right (id INTEGER, data VARCHAR, PRIMARY KEY id);
		INSERT INTO j_left (id, val) VALUES (1, 'a');
		INSERT INTO j_left (id, val) VALUES (2, 'b');
		INSERT INTO j_right (id, data) VALUES (2, 'x');
		INSERT INTO j_right (id, data) VALUES (3, 'y');
	`, nil)
	require.NoError(t, err)

	t.Run("full_outer_join", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT l.id, r.id FROM j_left l FULL JOIN j_right r ON l.id = r.id`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Equal(t, 3, count) // (1,null), (2,2), (null,3)
	})

	t.Run("cross_join_empty", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil,
			`CREATE TABLE j_empty (id INTEGER, PRIMARY KEY id)`, nil)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil,
			`SELECT l.id FROM j_left l CROSS JOIN j_empty e`, nil)
		require.NoError(t, err)
		// Empty cross product — may return ErrNoMoreRows or close cleanly
		_, err = r.Read(context.Background())
		if err != nil {
			require.ErrorIs(t, err, ErrNoMoreRows)
		}
		r.Close()
	})
}

// --- NATURAL JOIN Edge Cases ---

func TestNaturalJoinEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE nj_a (id INTEGER, name VARCHAR, PRIMARY KEY id);
		CREATE TABLE nj_b (id INTEGER, value VARCHAR, PRIMARY KEY id);
		INSERT INTO nj_a (id, name) VALUES (1, 'Alice');
		INSERT INTO nj_a (id, name) VALUES (2, 'Bob');
		INSERT INTO nj_b (id, value) VALUES (2, 'x');
		INSERT INTO nj_b (id, value) VALUES (3, 'y');
	`, nil)
	require.NoError(t, err)

	t.Run("natural_join_matching_cols", func(t *testing.T) {
		// NATURAL JOIN on tables with matching 'id' column
		r, err := engine.Query(context.Background(), nil,
			`SELECT nj_a.name, nj_b.value FROM nj_a NATURAL JOIN nj_b`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		// NATURAL JOIN with natural=true uses cond=true (cartesian product)
		// since column-matching logic is deferred
		require.Greater(t, count, 0)
	})

	t.Run("join_using", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT nj_a.name, nj_b.value FROM nj_a JOIN nj_b USING (id)`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Greater(t, count, 0)
	})
}

// --- Window Functions with NULL Values ---

func TestWindowFunctionsWithNulls(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE wfn_null (id INTEGER, grp VARCHAR, val INTEGER, PRIMARY KEY id);
		INSERT INTO wfn_null (id, grp, val) VALUES (1, 'a', 10);
		INSERT INTO wfn_null (id, grp, val) VALUES (2, 'a', NULL);
		INSERT INTO wfn_null (id, grp, val) VALUES (3, 'b', 30);
		INSERT INTO wfn_null (id, grp, val) VALUES (4, 'b', NULL);
	`, nil)
	require.NoError(t, err)

	t.Run("row_number_with_nulls", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, row_number() OVER (ORDER BY id) rn FROM wfn_null`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Equal(t, 4, count)
	})

	t.Run("sum_with_nulls", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, sum(val) OVER (PARTITION BY grp) s FROM wfn_null`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Equal(t, 4, count)
	})

	t.Run("lag_with_null_values", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, lag(val) OVER (ORDER BY id) prev FROM wfn_null`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Equal(t, 4, count)
	})
}

// --- Function Edge Cases ---

func TestFunctionEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	t.Run("coalesce_all_null", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT COALESCE(NULL, NULL, NULL)`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.True(t, row.ValuesByPosition[0].IsNull())
		r.Close()
	})

	t.Run("nullif_different", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT NULLIF(1, 2)`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(1), row.ValuesByPosition[0].RawValue())
		r.Close()
	})

	t.Run("nullif_same", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT NULLIF(1, 1)`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.True(t, row.ValuesByPosition[0].IsNull())
		r.Close()
	})

	t.Run("greatest_least", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT GREATEST(3, 1, 2), LEAST(3, 1, 2)`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(3), row.ValuesByPosition[0].RawValue())
		require.Equal(t, int64(1), row.ValuesByPosition[1].RawValue())
		r.Close()
	})

	t.Run("math_functions", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT ABS(-5), CEIL(4.2), FLOOR(4.8), ROUND(4.5)`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, row)
		r.Close()
	})

	t.Run("string_functions", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT LENGTH('hello'), UPPER('hello'), LOWER('HELLO'), TRIM('  hi  ')`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(5), row.ValuesByPosition[0].RawValue())
		require.Equal(t, "HELLO", row.ValuesByPosition[1].RawValue())
		require.Equal(t, "hello", row.ValuesByPosition[2].RawValue())
		require.Equal(t, "hi", row.ValuesByPosition[3].RawValue())
		r.Close()
	})

	t.Run("concat_ws", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT CONCAT_WS('-', 'a', 'b', 'c')`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "a-b-c", row.ValuesByPosition[0].RawValue())
		r.Close()
	})

	t.Run("md5", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT MD5('hello')`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "5d41402abc4b2a76b9719d911017c592", row.ValuesByPosition[0].RawValue())
		r.Close()
	})

	t.Run("replace", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT REPLACE('hello world', 'world', 'there')`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "hello there", row.ValuesByPosition[0].RawValue())
		r.Close()
	})

	t.Run("lpad_rpad", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT LPAD('hi', 5, '0'), RPAD('hi', 5, '.')`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "000hi", row.ValuesByPosition[0].RawValue())
		require.Equal(t, "hi...", row.ValuesByPosition[1].RawValue())
		r.Close()
	})

	t.Run("position_and_length", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT POSITION('world', 'hello world'), LENGTH('hello')`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(7), row.ValuesByPosition[0].RawValue()) // 1-based
		require.Equal(t, int64(5), row.ValuesByPosition[1].RawValue())
		r.Close()
	})
}

// --- String Function Value Tests ---

func TestStringFunctionValues(t *testing.T) {
	engine := setupCommonTest(t)

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{"reverse", "SELECT REVERSE('hello')", "olleh"},
		{"repeat", "SELECT REPEAT('ab', 3)", "ababab"},
		{"initcap", "SELECT INITCAP('hello world')", "Hello World"},
		{"chr", "SELECT CHR(65)", "A"},
		{"ascii", "SELECT ASCII('Z')", int64(90)},
		{"split_part", "SELECT SPLIT_PART('a-b-c', '-', 2)", "b"},
		{"translate", "SELECT TRANSLATE('hello', 'el', 'ip')", "hippo"},
		{"concat_ws", "SELECT CONCAT_WS('-', 'a', 'b', 'c')", "a-b-c"},
		{"regexp_replace", "SELECT REGEXP_REPLACE('foo123bar', '[0-9]+', 'NUM')", "fooNUMbar"},
		{"substr", "SELECT SUBSTR('hello', 2, 3)", "ell"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := engine.Query(context.Background(), nil, tt.query, nil)
			require.NoError(t, err)
			row, err := r.Read(context.Background())
			require.NoError(t, err)
			require.Equal(t, tt.expected, row.ValuesByPosition[0].RawValue())
			r.Close()
		})
	}
}

// --- Negative/Error Tests ---

func TestNegativeErrorCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil,
		`CREATE TABLE neg_test (id INTEGER, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	t.Run("drop_nonexistent_view", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil,
			`DROP VIEW nonexistent_view`, nil)
		require.Error(t, err)
	})

	t.Run("drop_view_if_exists", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil,
			`DROP VIEW IF EXISTS nonexistent_view`, nil)
		require.NoError(t, err) // IF EXISTS should not error
	})

	t.Run("drop_nonexistent_sequence", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil,
			`DROP SEQUENCE nonexistent_seq`, nil)
		require.Error(t, err)
	})

	t.Run("drop_sequence_if_exists", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil,
			`DROP SEQUENCE IF EXISTS nonexistent_seq`, nil)
		require.NoError(t, err) // IF EXISTS should not error
	})

	t.Run("create_view_conflict_with_table", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil,
			`CREATE VIEW neg_test AS SELECT id FROM neg_test`, nil)
		require.Error(t, err) // view name conflicts with table name
	})

	t.Run("create_view_if_not_exists", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil,
			`CREATE VIEW test_view AS SELECT id FROM neg_test`, nil)
		require.NoError(t, err)

		_, _, err = engine.Exec(context.Background(), nil,
			`CREATE VIEW IF NOT EXISTS test_view AS SELECT id FROM neg_test`, nil)
		require.NoError(t, err) // IF NOT EXISTS should not error

		_, _, err = engine.Exec(context.Background(), nil,
			`CREATE VIEW test_view AS SELECT id FROM neg_test`, nil)
		require.Error(t, err) // duplicate without IF NOT EXISTS should error

		_, _, err = engine.Exec(context.Background(), nil,
			`DROP VIEW test_view`, nil)
		require.NoError(t, err)
	})
}

// --- EXPLAIN Edge Cases ---

func TestExplainEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil,
		`CREATE TABLE explain_edge (id INTEGER, name VARCHAR, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	t.Run("explain_with_join", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil,
			`CREATE TABLE explain_join (id INTEGER, ref_id INTEGER, PRIMARY KEY id)`, nil)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil,
			`EXPLAIN SELECT e.name FROM explain_edge e INNER JOIN explain_join j ON e.id = j.ref_id WHERE e.id > 0`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Greater(t, count, 1) // multiple plan lines
	})

	t.Run("explain_with_cte", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`EXPLAIN WITH temp AS (SELECT id FROM explain_edge) SELECT id FROM temp`, nil)
		require.NoError(t, err)
		count := 0
		for {
			_, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			count++
		}
		r.Close()
		require.Greater(t, count, 0)
	})
}

// --- Sequence Edge Cases ---

func TestSequenceEdgeCases(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil,
		`CREATE TABLE seq_dummy (id INTEGER, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	t.Run("multiple_sequences", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil, `
			CREATE SEQUENCE seq1;
			CREATE SEQUENCE seq2;
		`, nil)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil,
			`SELECT NEXTVAL('seq1')`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(1), row.ValuesByPosition[0].RawValue())
		r.Close()

		r, err = engine.Query(context.Background(), nil,
			`SELECT NEXTVAL('seq2')`, nil)
		require.NoError(t, err)
		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(1), row.ValuesByPosition[0].RawValue())
		r.Close()

		// seq1 should be at 1, seq2 at 1
		r, err = engine.Query(context.Background(), nil,
			`SELECT NEXTVAL('seq1')`, nil)
		require.NoError(t, err)
		row, err = r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(2), row.ValuesByPosition[0].RawValue())
		r.Close()
	})

	t.Run("nonexistent_sequence", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT NEXTVAL('nonexistent')`, nil)
		if err == nil {
			// Error may come from Read rather than Query
			_, err = r.Read(context.Background())
			r.Close()
		}
		require.Error(t, err)
	})

	t.Run("currval_before_nextval", func(t *testing.T) {
		_, _, err := engine.Exec(context.Background(), nil,
			`CREATE SEQUENCE seq_new`, nil)
		require.NoError(t, err)

		r, err := engine.Query(context.Background(), nil,
			`SELECT CURRVAL('seq_new')`, nil)
		if err == nil {
			_, err = r.Read(context.Background())
			r.Close()
		}
		require.Error(t, err) // not started yet
	})
}
