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

package server

import (
	"crypto/tls"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/database"
)

type Option func(s *pgsrv)

func Host(host string) Option {
	return func(args *pgsrv) {
		args.host = host
	}
}

func Port(port int) Option {
	return func(args *pgsrv) {
		args.port = port
	}
}

func ImmudbPort(port int) Option {
	return func(args *pgsrv) {
		args.immudbPort = port
	}
}

func Logger(logger logger.Logger) Option {
	return func(args *pgsrv) {
		args.logger = logger
	}
}

func TLSConfig(tlsConfig *tls.Config) Option {
	return func(args *pgsrv) {
		args.tlsConfig = tlsConfig
	}
}

func DatabaseList(dbList database.DatabaseList) Option {
	return func(args *pgsrv) {
		args.dbList = dbList
	}
}
