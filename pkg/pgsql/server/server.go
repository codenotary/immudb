/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"sync/atomic"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"golang.org/x/net/netutil"
)

type pgsrv struct {
	m sync.RWMutex
	// running is read without taking m on the Accept hot loop, so it's kept
	// as an atomic.Bool. Stop writes via Store(false); Serve reads via Load.
	running            atomic.Bool
	maxConnections     int
	tlsConfig          *tls.Config
	logger             logger.Logger
	logRequestMetadata bool
	host               string
	port               int
	immudbPort         int
	dbList             database.DatabaseList
	sessManager        sessions.Manager
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
		maxConnections: 1000,
		tlsConfig:      &tls.Config{},
		logger:         logger.NewSimpleLogger("pgsqlSrv", os.Stderr),
		host:           "0.0.0.0",
		immudbPort:     3322,
		port:           5432,
	}
	srv.running.Store(true)

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
		s.m.Unlock()
		// Previously returned without unlocking, leaking the mutex and
		// deadlocking any subsequent Stop/GetPort call.
		return errors.New("no listener found for pgsql server")
	}
	s.listener = netutil.LimitListener(s.listener, s.maxConnections)
	listener := s.listener
	s.m.Unlock()

	if s.tlsConfig == nil || len(s.tlsConfig.Certificates) == 0 {
		s.logger.Warningf("pgsql server is running WITHOUT TLS. " +
			"Client passwords will be transmitted in cleartext. " +
			"Configure TLS or disable the pgsql server (--pgsql-server=false) in production.")
	}

	for {
		if !s.running.Load() {
			return nil
		}

		conn, err := listener.Accept()
		if err != nil {
			// Stop() closes the listener, which is the expected path out of
			// Accept. Report an error only if we were still supposed to be
			// running — otherwise the error is simply the consequence of Stop.
			if !s.running.Load() {
				return nil
			}
			s.logger.Errorf("%v", err)
			continue
		}
		go s.handleRequest(context.Background(), conn)
	}
}

func (s *pgsrv) newSession(conn net.Conn) Session {
	return newSession(conn, s.host, s.immudbPort, s.logger, s.tlsConfig, s.logRequestMetadata, s.dbList, s.sessManager)
}

func (s *pgsrv) Stop() (err error) {
	s.running.Store(false)

	s.m.Lock()
	defer s.m.Unlock()

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
