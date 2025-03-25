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

package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/database"
	"golang.org/x/net/netutil"
)

type pgsrv struct {
	m                  sync.RWMutex
	running            bool
	maxConnections     int
	tlsConfig          *tls.Config
	logger             logger.Logger
	logRequestMetadata bool
	host               string
	port               int
	immudbPort         int
	dbList             database.DatabaseList
	listener           net.Listener
}

type PGSQLServer interface {
	Initialize() error
	Serve() error
	Stop() error
	GetPort() int
}

func New(setters ...Option) *pgsrv {
	// Default Options
	srv := &pgsrv{
		running:        true,
		maxConnections: 1000,
		tlsConfig:      &tls.Config{},
		logger:         logger.NewSimpleLogger("pgsqlSrv", os.Stderr),
		host:           "0.0.0.0",
		immudbPort:     3322,
		port:           5432,
	}

	for _, setter := range setters {
		setter(srv)
	}

	return srv
}

// Initialize initialize listener. If provided port is zero os auto assign a free one.
func (s *pgsrv) Initialize() (err error) {
	s.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", s.host, s.port))
	if err != nil {
		return err
	}
	return nil
}

func (s *pgsrv) Serve() (err error) {
	s.m.Lock()
	if s.listener == nil {
		return errors.New("no listener found for pgsql server")
	}
	s.listener = netutil.LimitListener(s.listener, s.maxConnections)
	s.m.Unlock()

	for {
		s.m.Lock()
		if !s.running {
			s.m.Unlock()
			return nil
		}
		s.m.Unlock()

		conn, err := s.listener.Accept()
		if err != nil {
			s.logger.Errorf("%v", err)
		} else {
			go s.handleRequest(context.Background(), conn)
		}
	}
}

func (s *pgsrv) newSession(conn net.Conn) Session {
	return newSession(conn, s.host, s.immudbPort, s.logger, s.tlsConfig, s.logRequestMetadata, s.dbList)
}

func (s *pgsrv) Stop() (err error) {
	s.m.Lock()
	defer s.m.Unlock()

	s.running = false

	if s.listener != nil {
		return s.listener.Close()
	}

	return nil
}

func (s *pgsrv) GetPort() int {
	s.m.Lock()
	defer s.m.Unlock()

	if s.listener != nil {
		return s.listener.Addr().(*net.TCPAddr).Port
	}

	return 0
}
