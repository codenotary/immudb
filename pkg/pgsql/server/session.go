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
	"strings"

	"net"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/pgsql/errors"
	"github.com/codenotary/immudb/pkg/server/sessions"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
)

// stmtCacheSize is the maximum number of parsed statement lists cached
// per session. When the cache is full the oldest entry is evicted (FIFO).
const stmtCacheSize = 64

type session struct {
	immudbHost         string
	immudbPort         int
	tlsConfig          *tls.Config
	log                logger.Logger
	logRequestMetadata bool

	dbList      database.DatabaseList
	sessManager sessions.Manager

	client client.ImmuClient

	ctx    context.Context
	user   string
	ipAddr string
	db     database.DB
	tx     *sql.SQLTx

	mr MessageReader

	connParams      map[string]string
	protocolVersion string

	statements map[string]*statement
	portals    map[string]*portal

	// stmtCache is a bounded per-session cache of parsed SQL statement lists,
	// keyed on the post-normalization SQL string (after removePGCatalogReferences).
	// Avoids repeated lex+parse overhead for queries repeated by dashboards or ORMs.
	// Eviction is FIFO; stmtCacheKeys tracks insertion order for eviction.
	stmtCache     map[string][]sql.SQLStmt
	stmtCacheKeys []string
}

type Session interface {
	InitializeSession() error
	HandleStartup(context.Context) error
	QueryMachine() error
	HandleError(error)
	Close() error
}

func newSession(
	c net.Conn,
	immudbHost string,
	immudbPort int,
	log logger.Logger,
	tlsConfig *tls.Config,
	logRequestMetadata bool,
	dbList database.DatabaseList,
	sessManager sessions.Manager,
) *session {
	addr := c.RemoteAddr().String()
	i := strings.Index(addr, ":")
	if i >= 0 {
		addr = addr[:i]
	}

	return &session{
		immudbHost:         immudbHost,
		immudbPort:         immudbPort,
		tlsConfig:          tlsConfig,
		log:                log,
		logRequestMetadata: logRequestMetadata,
		dbList:             dbList,
		sessManager:        sessManager,
		ipAddr:             addr,
		mr:                 NewMessageReader(c),
		statements:         make(map[string]*statement),
		portals:            make(map[string]*portal),
		stmtCache:          make(map[string][]sql.SQLStmt, stmtCacheSize),
		stmtCacheKeys:      make([]string, 0, stmtCacheSize),
	}
}

func (s *session) HandleError(e error) {
	pgerr := errors.MapPgError(e)

	_, err := s.writeMessage(pgerr.Encode())
	if err != nil {
		s.log.Errorf("unable to write error on wire: %v", err)
	}
}

func (s *session) nextMessage() (interface{}, bool, error) {
	msg, err := s.mr.ReadRawMessage()
	if err != nil {
		return nil, false, err
	}

	s.log.Debugf("received %s - %s message", string(msg.t), pgmeta.MTypes[msg.t])

	extQueryMode := false

	i, err := s.parseRawMessage(msg)
	if msg.t == 'P' ||
		msg.t == 'B' ||
		msg.t == 'D' ||
		msg.t == 'E' ||
		msg.t == 'H' {
		extQueryMode = true
	}

	return i, extQueryMode, err
}

func (s *session) parseRawMessage(msg *rawMessage) (interface{}, error) {
	switch msg.t {
	case 'p':
		return fm.ParsePasswordMsg(msg.payload)
	case 'Q':
		return fm.ParseQueryMsg(msg.payload)
	case 'X':
		return fm.ParseTerminateMsg(msg.payload)
	case 'P':
		return fm.ParseParseMsg(msg.payload)
	case 'B':
		return fm.ParseBindMsg(msg.payload)
	case 'D':
		return fm.ParseDescribeMsg(msg.payload)
	case 'S':
		return fm.ParseSyncMsg(msg.payload)
	case 'E':
		return fm.ParseExecuteMsg(msg.payload)
	case 'H':
		return fm.ParseFlushMsg(msg.payload)
	case 'd':
		return fm.ParseCopyDataMsg(msg.payload)
	case 'c':
		return fm.ParseCopyDoneMsg(msg.payload)
	case 'f':
		return fm.ParseCopyFailMsg(msg.payload)
	default:
		return nil, errors.ErrUnknowMessageType
	}
}

func (s *session) writeMessage(msg []byte) (int, error) {
	if len(msg) > 0 {
		s.log.Debugf("write %s - %s message", string(msg[0]), pgmeta.MTypes[msg[0]])
	}

	return s.mr.Write(msg)
}

// refreshSessionActivity updates the server-side session activity timestamp
// to prevent the session guard from killing long-running operations like COPY.
func (s *session) refreshSessionActivity() {
	if s.sessManager != nil && s.client != nil {
		sessionID := s.client.GetSessionID()
		if sessionID != "" {
			s.sessManager.UpdateSessionActivityTime(sessionID)
			s.log.Infof("pgcompat: refreshed session activity for %s", sessionID)
		}
	} else {
		s.log.Warningf("pgcompat: cannot refresh session: sessManager=%v client=%v", s.sessManager != nil, s.client != nil)
	}
}

func (s *session) sqlTx() (*sql.SQLTx, error) {
	if s.tx != nil || !s.logRequestMetadata {
		return s.tx, nil
	}

	md := schema.Metadata{
		schema.UserRequestMetadataKey: s.user,
		schema.IpRequestMetadataKey:   s.ipAddr,
	}

	// create transaction explicitly to inject request metadata
	ctx := schema.ContextWithMetadata(s.ctx, md)
	return s.db.NewSQLTx(ctx, sql.DefaultTxOptions())
}

// useDatabase rebinds this pgsql session to a different immudb database
// without forcing the client to reconnect. Permission enforcement is
// delegated to the embedded gRPC client (s.client.UseDatabase), which
// runs the same checks as the initial connect: the logged-in user must
// have Admin / R / RW on the target database, otherwise the gRPC layer
// returns PermissionDenied and we surface that to the pgsql client.
//
// Any in-flight SQL transaction is rolled back, and prepared statements
// and portals are dropped, since their plans reference the old catalog
// and would silently bind to wrong tables on the new database.
func (s *session) useDatabase(name string) error {
	dbHandle, err := s.dbList.GetByName(name)
	if err != nil {
		return err
	}

	if s.client != nil {
		if _, err := s.client.UseDatabase(s.ctx, &schema.Database{DatabaseName: name}); err != nil {
			return err
		}
	}

	if s.tx != nil {
		_ = s.tx.Cancel()
		s.tx = nil
	}
	for k := range s.statements {
		delete(s.statements, k)
	}
	for k := range s.portals {
		delete(s.portals, k)
	}

	s.db = dbHandle
	s.log.Infof("pgcompat: session for user %q switched to database %q", s.user, name)
	return nil
}
