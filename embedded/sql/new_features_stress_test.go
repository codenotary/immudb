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
	"testing"

	"github.com/stretchr/testify/require"
)

// --- Stress tests for new features with larger data sets ---

func TestWindowFunctionsLargeDataset(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil,
		`CREATE TABLE wf_large (id INTEGER, dept VARCHAR, salary INTEGER, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	// Insert 50 rows across 5 departments
	for i := 1; i <= 50; i++ {
		dept := fmt.Sprintf("dept%d", (i%5)+1)
		salary := 30000 + (i * 1000)
		_, _, err := engine.Exec(context.Background(), nil,
			fmt.Sprintf("INSERT INTO wf_large (id, dept, salary) VALUES (%d, '%s', %d)", i, dept, salary), nil)
		require.NoError(t, err)
	}

	t.Run("row_number_50_rows", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, row_number() OVER (ORDER BY id) rn FROM wf_large`, nil)
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
		require.Equal(t, 50, count)
	})

	t.Run("partition_count", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, dept, count(*) OVER (PARTITION BY dept) cnt FROM wf_large`, nil)
		require.NoError(t, err)
		count := 0
		for {
			row, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			cnt := row.ValuesByPosition[2].RawValue().(int64)
			require.Equal(t, int64(10), cnt) // 50 / 5 depts = 10 each
			count++
		}
		r.Close()
		require.Equal(t, 50, count)
	})

	t.Run("rank_across_partitions", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, dept, rank() OVER (PARTITION BY dept ORDER BY salary DESC) rk FROM wf_large`, nil)
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
		require.Equal(t, 50, count)
	})

	t.Run("lag_lead_all_rows", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil, `
			SELECT id, salary,
				lag(salary) OVER (ORDER BY id) prev,
				lead(salary) OVER (ORDER BY id) next
			FROM wf_large`, nil)
		require.NoError(t, err)
		count := 0
		for {
			row, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			if count == 0 {
				require.True(t, row.ValuesByPosition[2].IsNull()) // first row has no lag
			}
			count++
		}
		r.Close()
		require.Equal(t, 50, count)
	})

	t.Run("ntile_10_buckets", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, ntile(10) OVER (ORDER BY salary) bucket FROM wf_large`, nil)
		require.NoError(t, err)
		buckets := make(map[int64]int)
		for {
			row, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			b := row.ValuesByPosition[1].RawValue().(int64)
			buckets[b]++
		}
		r.Close()
		require.Equal(t, 10, len(buckets)) // 10 buckets
		for _, cnt := range buckets {
			require.Equal(t, 5, cnt) // 50 / 10 = 5 per bucket
		}
	})
}

func TestCTELargeRecursion(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil,
		`CREATE TABLE dummy_cte (id INTEGER, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	// Generate numbers 1-100 with recursive CTE
	r, err := engine.Query(context.Background(), nil, `
		WITH RECURSIVE nums AS (
			SELECT 1 AS n
			UNION ALL
			SELECT n + 1 FROM nums WHERE n < 100
		)
		SELECT n FROM nums`, nil)
	require.NoError(t, err)

	count := 0
	sum := int64(0)
	for {
		row, err := r.Read(context.Background())
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		n := row.ValuesByPosition[0].RawValue().(int64)
		sum += n
		count++
	}
	r.Close()
	require.Equal(t, 100, count)
	require.Equal(t, int64(5050), sum) // sum of 1..100
}

func TestRecursiveCTETreeTraversal(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil,
		`CREATE TABLE org (id INTEGER, parent_id INTEGER, name VARCHAR, level INTEGER, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	// Build a 3-level org tree: 1 CEO, 3 VPs, 9 managers
	_, _, err = engine.Exec(context.Background(), nil, `
		INSERT INTO org (id, parent_id, name, level) VALUES (1, 0, 'CEO', 1);
		INSERT INTO org (id, parent_id, name, level) VALUES (2, 1, 'VP1', 2);
		INSERT INTO org (id, parent_id, name, level) VALUES (3, 1, 'VP2', 2);
		INSERT INTO org (id, parent_id, name, level) VALUES (4, 1, 'VP3', 2);
		INSERT INTO org (id, parent_id, name, level) VALUES (5, 2, 'Mgr1', 3);
		INSERT INTO org (id, parent_id, name, level) VALUES (6, 2, 'Mgr2', 3);
		INSERT INTO org (id, parent_id, name, level) VALUES (7, 2, 'Mgr3', 3);
		INSERT INTO org (id, parent_id, name, level) VALUES (8, 3, 'Mgr4', 3);
		INSERT INTO org (id, parent_id, name, level) VALUES (9, 3, 'Mgr5', 3);
		INSERT INTO org (id, parent_id, name, level) VALUES (10, 3, 'Mgr6', 3);
		INSERT INTO org (id, parent_id, name, level) VALUES (11, 4, 'Mgr7', 3);
		INSERT INTO org (id, parent_id, name, level) VALUES (12, 4, 'Mgr8', 3);
		INSERT INTO org (id, parent_id, name, level) VALUES (13, 4, 'Mgr9', 3);
	`, nil)
	require.NoError(t, err)

	// Traverse from CEO down
	r, err := engine.Query(context.Background(), nil, `
		WITH RECURSIVE reports AS (
			SELECT id, name, level FROM org WHERE id = 1
			UNION ALL
			SELECT o.id, o.name, o.level FROM org o INNER JOIN reports r ON o.parent_id = r.id
		)
		SELECT id, name, level FROM reports`, nil)
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
	require.Equal(t, 13, count) // all 13 employees

	// Traverse from VP2 down (subtree)
	r, err = engine.Query(context.Background(), nil, `
		WITH RECURSIVE reports AS (
			SELECT id, name FROM org WHERE id = 3
			UNION ALL
			SELECT o.id, o.name FROM org o INNER JOIN reports r ON o.parent_id = r.id
		)
		SELECT id, name FROM reports`, nil)
	require.NoError(t, err)

	count = 0
	for {
		_, err := r.Read(context.Background())
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		count++
	}
	r.Close()
	require.Equal(t, 4, count) // VP2 + 3 managers
}

func TestSubqueriesComplex(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE customers (id INTEGER, name VARCHAR, tier VARCHAR, PRIMARY KEY id);
		CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER, PRIMARY KEY id);
		INSERT INTO customers (id, name, tier) VALUES (1, 'Alice', 'gold');
		INSERT INTO customers (id, name, tier) VALUES (2, 'Bob', 'silver');
		INSERT INTO customers (id, name, tier) VALUES (3, 'Charlie', 'gold');
		INSERT INTO customers (id, name, tier) VALUES (4, 'Diana', 'bronze');
		INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 500);
		INSERT INTO orders (id, customer_id, amount) VALUES (2, 1, 300);
		INSERT INTO orders (id, customer_id, amount) VALUES (3, 2, 150);
		INSERT INTO orders (id, customer_id, amount) VALUES (4, 3, 700);
	`, nil)
	require.NoError(t, err)

	t.Run("correlated_exists", func(t *testing.T) {
		// Customers with at least one order
		r, err := engine.Query(context.Background(), nil,
			`SELECT c.name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)`, nil)
		require.NoError(t, err)
		var names []string
		for {
			row, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			names = append(names, row.ValuesByPosition[0].RawValue().(string))
		}
		r.Close()
		require.Equal(t, 3, len(names)) // Alice, Bob, Charlie (not Diana)
		require.Contains(t, names, "Alice")
		require.Contains(t, names, "Bob")
		require.Contains(t, names, "Charlie")
	})

	t.Run("correlated_not_exists", func(t *testing.T) {
		// Customers without orders
		r, err := engine.Query(context.Background(), nil,
			`SELECT c.name FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "Diana", row.ValuesByPosition[0].RawValue())
		_, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreRows)
		r.Close()
	})

	t.Run("in_subquery_with_filter", func(t *testing.T) {
		// Customers who have orders over 200
		r, err := engine.Query(context.Background(), nil,
			`SELECT c.name FROM customers c WHERE c.id IN (SELECT o.customer_id FROM orders o WHERE o.amount > 200)`, nil)
		require.NoError(t, err)
		var names []string
		for {
			row, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			names = append(names, row.ValuesByPosition[0].RawValue().(string))
		}
		r.Close()
		require.Equal(t, 2, len(names)) // Alice (500,300) and Charlie (700)
	})

	t.Run("not_in_subquery", func(t *testing.T) {
		// Customers NOT in orders
		r, err := engine.Query(context.Background(), nil,
			`SELECT c.name FROM customers c WHERE c.id NOT IN (SELECT o.customer_id FROM orders o)`, nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, "Diana", row.ValuesByPosition[0].RawValue())
		r.Close()
	})
}

func TestFullOuterJoinLarger(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE foj_a (id INTEGER, val VARCHAR, PRIMARY KEY id);
		CREATE TABLE foj_b (id INTEGER, data VARCHAR, PRIMARY KEY id);
	`, nil)
	require.NoError(t, err)

	for i := 1; i <= 10; i++ {
		_, _, err := engine.Exec(context.Background(), nil,
			fmt.Sprintf("INSERT INTO foj_a (id, val) VALUES (%d, 'a%d')", i, i), nil)
		require.NoError(t, err)
	}
	for i := 6; i <= 15; i++ {
		_, _, err := engine.Exec(context.Background(), nil,
			fmt.Sprintf("INSERT INTO foj_b (id, data) VALUES (%d, 'b%d')", i, i), nil)
		require.NoError(t, err)
	}

	// FULL OUTER JOIN: 1-5 from A only, 6-10 from both, 11-15 from B only = 15 rows
	r, err := engine.Query(context.Background(), nil,
		`SELECT a.id, b.id FROM foj_a a FULL JOIN foj_b b ON a.id = b.id`, nil)
	require.NoError(t, err)

	count := 0
	nullLeft := 0
	nullRight := 0
	matched := 0
	for {
		row, err := r.Read(context.Background())
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		leftNull := row.ValuesByPosition[0].IsNull()
		rightNull := row.ValuesByPosition[1].IsNull()
		if leftNull {
			nullLeft++
		} else if rightNull {
			nullRight++
		} else {
			matched++
		}
		count++
	}
	r.Close()
	require.Equal(t, 15, count)
	require.Equal(t, 5, nullRight)  // A-only rows (1-5)
	require.Equal(t, 5, nullLeft)   // B-only rows (11-15)
	require.Equal(t, 5, matched)    // matched rows (6-10)
}

func TestExceptIntersectLarger(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE ei_a (id INTEGER, PRIMARY KEY id);
		CREATE TABLE ei_b (id INTEGER, PRIMARY KEY id);
	`, nil)
	require.NoError(t, err)

	for i := 1; i <= 20; i++ {
		_, _, err := engine.Exec(context.Background(), nil,
			fmt.Sprintf("INSERT INTO ei_a (id) VALUES (%d)", i), nil)
		require.NoError(t, err)
	}
	for i := 11; i <= 30; i++ {
		_, _, err := engine.Exec(context.Background(), nil,
			fmt.Sprintf("INSERT INTO ei_b (id) VALUES (%d)", i), nil)
		require.NoError(t, err)
	}

	t.Run("except", func(t *testing.T) {
		// A EXCEPT B: 1-10
		r, err := engine.Query(context.Background(), nil,
			`SELECT id FROM ei_a EXCEPT SELECT id FROM ei_b`, nil)
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
		require.Equal(t, 10, count)
	})

	t.Run("intersect", func(t *testing.T) {
		// A INTERSECT B: 11-20
		r, err := engine.Query(context.Background(), nil,
			`SELECT id FROM ei_a INTERSECT SELECT id FROM ei_b`, nil)
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
		require.Equal(t, 10, count)
	})
}

func TestReturningMultipleRows(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil,
		`CREATE TABLE ret_multi (id INTEGER AUTO_INCREMENT, tag VARCHAR, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	// Insert 5 rows first
	for i := 0; i < 5; i++ {
		_, _, err := engine.Exec(context.Background(), nil,
			fmt.Sprintf("INSERT INTO ret_multi (tag) VALUES ('tag%d')", i), nil)
		require.NoError(t, err)
	}

	// UPDATE with RETURNING — multiple rows
	r, err := engine.Query(context.Background(), nil,
		`UPDATE ret_multi SET tag = 'updated' WHERE id >= 0 RETURNING id, tag`, nil)
	require.NoError(t, err)

	count := 0
	for {
		row, err := r.Read(context.Background())
		if err == ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		require.Equal(t, "updated", row.ValuesByPosition[1].RawValue())
		count++
	}
	r.Close()
	require.Equal(t, 5, count)
}

func TestViewWithJoin(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE v_products (id INTEGER, name VARCHAR, cat_id INTEGER, PRIMARY KEY id);
		CREATE TABLE v_categories (id INTEGER, name VARCHAR, PRIMARY KEY id);
		INSERT INTO v_categories (id, name) VALUES (1, 'Electronics');
		INSERT INTO v_categories (id, name) VALUES (2, 'Books');
		INSERT INTO v_products (id, name, cat_id) VALUES (1, 'Laptop', 1);
		INSERT INTO v_products (id, name, cat_id) VALUES (2, 'Phone', 1);
		INSERT INTO v_products (id, name, cat_id) VALUES (3, 'Novel', 2);
	`, nil)
	require.NoError(t, err)

	// Create view with JOIN
	_, _, err = engine.Exec(context.Background(), nil,
		`CREATE VIEW product_list AS SELECT p.id, p.name, c.name FROM v_products p INNER JOIN v_categories c ON p.cat_id = c.id`, nil)
	require.NoError(t, err)

	r, err := engine.Query(context.Background(), nil,
		`SELECT id, name FROM product_list`, nil)
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
	require.Equal(t, 3, count)

	_, _, err = engine.Exec(context.Background(), nil, `DROP VIEW product_list`, nil)
	require.NoError(t, err)
}

func TestNullsFirstLastWithData(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil, `
		CREATE TABLE nfl (id INTEGER, score INTEGER, PRIMARY KEY id);
		INSERT INTO nfl (id, score) VALUES (1, 80);
		INSERT INTO nfl (id, score) VALUES (2, NULL);
		INSERT INTO nfl (id, score) VALUES (3, 90);
		INSERT INTO nfl (id, score) VALUES (4, NULL);
		INSERT INTO nfl (id, score) VALUES (5, 70);
	`, nil)
	require.NoError(t, err)

	t.Run("nulls_last_asc", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, score FROM nfl ORDER BY score ASC NULLS LAST`, nil)
		require.NoError(t, err)
		var ids []int64
		for {
			row, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			ids = append(ids, row.ValuesByPosition[0].RawValue().(int64))
		}
		r.Close()
		require.Equal(t, 5, len(ids))
		// NULLs should be last
		require.Equal(t, int64(5), ids[0]) // score=70
		require.Equal(t, int64(1), ids[1]) // score=80
		require.Equal(t, int64(3), ids[2]) // score=90
		// ids[3] and ids[4] are NULLs (id=2 and id=4)
	})

	t.Run("nulls_first_desc", func(t *testing.T) {
		r, err := engine.Query(context.Background(), nil,
			`SELECT id, score FROM nfl ORDER BY score DESC NULLS FIRST`, nil)
		require.NoError(t, err)
		var ids []int64
		for {
			row, err := r.Read(context.Background())
			if err == ErrNoMoreRows {
				break
			}
			require.NoError(t, err)
			ids = append(ids, row.ValuesByPosition[0].RawValue().(int64))
		}
		r.Close()
		require.Equal(t, 5, len(ids))
		// NULLs should be first
		// Then 90, 80, 70
		require.Equal(t, int64(3), ids[2]) // score=90
		require.Equal(t, int64(1), ids[3]) // score=80
		require.Equal(t, int64(5), ids[4]) // score=70
	})
}

func TestOnConflictDoUpdateEngine(t *testing.T) {
	engine := setupCommonTest(t)

	_, _, err := engine.Exec(context.Background(), nil,
		`CREATE TABLE kv (key INTEGER, value VARCHAR, version INTEGER, PRIMARY KEY key)`, nil)
	require.NoError(t, err)

	// Initial insert
	_, _, err = engine.Exec(context.Background(), nil,
		`INSERT INTO kv (key, value, version) VALUES (1, 'v1', 1)`, nil)
	require.NoError(t, err)

	// ON CONFLICT DO UPDATE — should update
	_, _, err = engine.Exec(context.Background(), nil,
		`INSERT INTO kv (key, value, version) VALUES (1, 'v2', 2) ON CONFLICT DO UPDATE SET value = 'v2', version = 2`, nil)
	require.NoError(t, err)

	r, err := engine.Query(context.Background(), nil,
		`SELECT value, version FROM kv WHERE key = 1`, nil)
	require.NoError(t, err)
	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "v2", row.ValuesByPosition[0].RawValue())
	require.Equal(t, int64(2), row.ValuesByPosition[1].RawValue())
	r.Close()

	// ON CONFLICT DO NOTHING — should not change
	_, _, err = engine.Exec(context.Background(), nil,
		`INSERT INTO kv (key, value, version) VALUES (1, 'v3', 3) ON CONFLICT DO NOTHING`, nil)
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil,
		`SELECT value FROM kv WHERE key = 1`, nil)
	require.NoError(t, err)
	row, err = r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "v2", row.ValuesByPosition[0].RawValue()) // still v2
	r.Close()

	// Insert new key with ON CONFLICT — should just insert
	_, _, err = engine.Exec(context.Background(), nil,
		`INSERT INTO kv (key, value, version) VALUES (2, 'new', 1) ON CONFLICT DO UPDATE SET value = 'should_not_happen'`, nil)
	require.NoError(t, err)

	r, err = engine.Query(context.Background(), nil,
		`SELECT value FROM kv WHERE key = 2`, nil)
	require.NoError(t, err)
	row, err = r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "new", row.ValuesByPosition[0].RawValue())
	r.Close()
}
