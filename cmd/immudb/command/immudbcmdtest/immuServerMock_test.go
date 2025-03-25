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

package immudbcmdtest

import (
	"testing"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	pgsqlsrv "github.com/codenotary/immudb/pkg/pgsql/server"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/stretchr/testify/require"
)

type ssMock struct{}

func (ss *ssMock) Sign(state *schema.ImmutableState) error { return nil }

func TestImmuServerMock(t *testing.T) {
	mock := &ImmuServerMock{}

	psqlServer := pgsqlsrv.New()
	mock.WithPgsqlServer(psqlServer)
	require.Same(t, psqlServer, mock.PgsqlSrv)

	opts := &server.Options{}
	mock.WithOptions(opts)
	require.Same(t, opts, mock.Options)

	logger := &logger.SimpleLogger{}
	mock.WithLogger(logger)
	require.Same(t, logger, mock.Logger)

	ss := &ssMock{}
	mock.WithStateSigner(ss)
	require.Same(t, ss, mock.StateSigner)

	ssf := stream.NewStreamServiceFactory(1)
	mock.WithStreamServiceFactory(ssf)
	require.Same(t, ssf, mock.Ssf)

	list := database.NewDatabaseList(nil)
	mock.WithDbList(list)
	require.Same(t, list, mock.DbList)

	// Test if calls do not panic
	mock.Initialize()
	mock.Start()
	mock.Stop()
}
