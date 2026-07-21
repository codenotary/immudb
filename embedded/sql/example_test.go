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

package sql_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
)

// ExampleNewEngine runs the embedded SQL engine in-process on top of a local
// key-value store: no server, no container. Note the store MUST be opened with
// WithMultiIndexing(true), otherwise NewEngine returns ErrMultiIndexingNotEnabled.
func ExampleNewEngine() {
	dir, err := os.MkdirTemp("", "immudb-embedded-sql")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	st, err := store.Open(dir, store.DefaultOptions().WithMultiIndexing(true))
	if err != nil {
		panic(err)
	}
	defer st.Close()

	engine, err := sql.NewEngine(st, sql.DefaultOptions().WithPrefix([]byte("sql")))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	_, _, err = engine.Exec(ctx, nil,
		"CREATE TABLE users (id INTEGER, name VARCHAR, PRIMARY KEY id)", nil)
	if err != nil {
		panic(err)
	}

	_, _, err = engine.Exec(ctx, nil,
		"INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')", nil)
	if err != nil {
		panic(err)
	}

	reader, err := engine.Query(ctx, nil, "SELECT id, name FROM users ORDER BY id", nil)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	for {
		row, err := reader.Read(ctx)
		if errors.Is(err, sql.ErrNoMoreRows) {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v %v\n",
			row.ValuesByPosition[0].RawValue(),
			row.ValuesByPosition[1].RawValue(),
		)
	}

	// Output:
	// 1 Alice
	// 2 Bob
}
