/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"fmt"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"net"
	"os"
)

type srv struct {
	tlsConfig      *tls.Config
	SessionFactory SessionFactory
	Logger         logger.Logger
	Host           string
	Port           int
	dbList         database.DatabaseList
	sysDb          database.DB
}

type Server interface {
	Serve() error
}

func New(setters ...Option) *srv {

	// Default Options
	cli := &srv{
		tlsConfig:      &tls.Config{},
		SessionFactory: NewSessionFactory(),
		Host:           "localhost",
		Port:           5432,
		Logger:         logger.NewSimpleLogger("sqlSrv", os.Stderr),
	}

	for _, setter := range setters {
		setter(cli)
	}

	return cli
}

func (s *srv) Serve() error {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.Host, s.Port))
	if err != nil {
		return err
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			// @todo better handle log error
			// @todo do not stop the server
			// @todo add some logic to protect this server
			return err
		}
		go s.handleRequest(conn)
	}
}
