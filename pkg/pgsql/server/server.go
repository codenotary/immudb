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
	"errors"
	"fmt"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"golang.org/x/net/netutil"
	"net"
	"os"
)

type srv struct {
	maxConnections int
	tlsConfig      *tls.Config
	SessionFactory SessionFactory
	Logger         logger.Logger
	Host           string
	Port           int
	dbList         database.DatabaseList
	sysDb          database.DB
	listener       net.Listener
}

type Server interface {
	Initialize() error
	Serve() error
	GetPort() int
}

func New(setters ...Option) *srv {

	// Default Options
	cli := &srv{
		maxConnections: 1000,
		tlsConfig:      &tls.Config{},
		SessionFactory: NewSessionFactory(),
		Logger:         logger.NewSimpleLogger("sqlSrv", os.Stderr),
		Host:           "localhost",
		Port:           5432,
	}

	for _, setter := range setters {
		setter(cli)
	}

	return cli
}

// Initialize initialize listener. If provided port is zero os auto assign a free one.
func (s *srv) Initialize() (err error) {
	s.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", s.Host, s.Port))
	if err != nil {
		return err
	}
	return nil
}

func (s *srv) Serve() (err error) {
	if s.listener == nil {
		return errors.New("no listener found for pgsql server")
	}
	defer s.listener.Close()

	s.listener = netutil.LimitListener(s.listener, s.maxConnections)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.Logger.Errorf("%v", err)
		}
		go s.handleRequest(conn)
	}
}

func (s *srv) GetPort() int {
	if s.listener != nil {
		return s.listener.Addr().(*net.TCPAddr).Port
	}
	return 0
}
