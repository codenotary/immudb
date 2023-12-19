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

package stdlib

import (
	"database/sql"
	"testing"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/stretchr/testify/require"
)

func TestDriverConnector_Connect(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	connStr := RegisterConnConfig(client.DefaultOptions().WithPort(port).WithDir(t.TempDir()))

	db, err := sql.Open("immudb", connStr)
	require.NoError(t, err)
	require.NotNil(t, db)
}

func TestDriverConnector_ConnectParseError(t *testing.T) {
	conn, err := immuDriver.Open("not parsable string")
	require.ErrorIs(t, err, ErrBadQueryString)
	require.Nil(t, conn)
}

func TestDriverConnector_Driver(t *testing.T) {
	c := driverConnector{
		driver: immuDriver,
	}
	d := c.Driver()
	require.IsType(t, &Driver{}, d)
}
