/*
Copyright 2022 Codenotary Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

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

type Option func(s *srv)

func Address(addr string) Option {
	return func(args *srv) {
		args.Address = addr
	}
}

func Port(port int) Option {
	return func(args *srv) {
		args.Port = port
	}
}

func Logger(logger logger.Logger) Option {
	return func(args *srv) {
		args.Logger = logger
	}
}

func DatabaseList(dbList database.DatabaseList) Option {
	return func(args *srv) {
		args.dbList = dbList
	}
}

func SysDb(sysdb database.DB) Option {
	return func(args *srv) {
		args.sysDb = sysdb
	}
}

func TlsConfig(tlsConfig *tls.Config) Option {
	return func(args *srv) {
		args.tlsConfig = tlsConfig
	}
}

func SessFactory(sf SessionFactory) Option {
	return func(args *srv) {
		args.SessionFactory = sf
	}
}
