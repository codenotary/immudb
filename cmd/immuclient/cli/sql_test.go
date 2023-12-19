/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSqlFloat(t *testing.T) {
	cli := setupTest(t)

	_, err := cli.sqlExec([]string{
		"CREATE TABLE t1(id INTEGER AUTO_INCREMENT, val FLOAT, PRIMARY KEY(id))",
	})
	require.NoError(t, err)

	_, err = cli.sqlExec([]string{
		"INSERT INTO t1(val) VALUES(1.1)",
	})
	require.NoError(t, err)

	s, err := cli.sqlQuery([]string{
		"SELECT id, val FROM t1",
	})
	require.NoError(t, err)
	require.Regexp(t, `(?m)^\|\s+\d+\s+\|\s+1\.1\s+\|$`, s)
}
