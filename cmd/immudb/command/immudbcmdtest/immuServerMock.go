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
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/database"
	pgsqlsrv "github.com/codenotary/immudb/pkg/pgsql/server"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/stream"
)

type ImmuServerMock struct {
	Options     *server.Options
	Logger      logger.Logger
	StateSigner server.StateSigner
	Ssf         stream.ServiceFactory
	PgsqlSrv    pgsqlsrv.PGSQLServer
	DbList      database.DatabaseList
}

func (s *ImmuServerMock) WithPgsqlServer(psrv pgsqlsrv.PGSQLServer) server.ImmuServerIf {
	s.PgsqlSrv = psrv
	return s
}

func (s *ImmuServerMock) WithOptions(options *server.Options) server.ImmuServerIf {
	s.Options = options
	return s
}
func (s *ImmuServerMock) WithLogger(logger logger.Logger) server.ImmuServerIf {
	s.Logger = logger
	return s
}

func (s *ImmuServerMock) WithStateSigner(stateSigner server.StateSigner) server.ImmuServerIf {
	s.StateSigner = stateSigner
	return s
}

func (s *ImmuServerMock) WithStreamServiceFactory(ssf stream.ServiceFactory) server.ImmuServerIf {
	s.Ssf = ssf
	return s
}

func (s *ImmuServerMock) WithDbList(dbList database.DatabaseList) server.ImmuServerIf {
	s.DbList = dbList
	return s
}

func (s *ImmuServerMock) Start() error {
	return nil
}

func (s *ImmuServerMock) Stop() error {
	return nil
}

func (s *ImmuServerMock) Initialize() error {
	return nil
}
