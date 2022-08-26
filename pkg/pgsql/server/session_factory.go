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
	"net"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/database"
)

type sessionFactory struct{}

type SessionFactory interface {
	NewSession(conn net.Conn, log logger.Logger, sysDb database.DB, tlsConfig *tls.Config) Session
}

func NewSessionFactory() sessionFactory {
	return sessionFactory{}
}

func (sm sessionFactory) NewSession(conn net.Conn, log logger.Logger, sysDb database.DB, tlsConfig *tls.Config) Session {
	return NewSession(conn, log, sysDb, tlsConfig)
}
