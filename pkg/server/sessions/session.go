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

package sessions

import (
	"context"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server/sessions/internal/transactions"
	"github.com/rs/xid"
	"google.golang.org/grpc/metadata"
	"sync"
	"time"
)

type Status int64

const (
	ACTIVE Status = iota
	IDLE
	DEAD
)

type Session struct {
	sync.RWMutex
	id                 string
	state              Status
	user               *auth.User
	databaseID         int64
	creationTime       time.Time
	lastActivityTime   time.Time
	lastHeartBeat      time.Time
	readWriteTxOngoing bool
	transactions       map[string]transactions.Transaction
	log                logger.Logger
}

func NewSession(sessionID string, user *auth.User, databaseID int64, log logger.Logger) *Session {
	now := time.Now()
	return &Session{
		id:               sessionID,
		state:            ACTIVE,
		user:             user,
		databaseID:       databaseID,
		creationTime:     now,
		lastActivityTime: now,
		lastHeartBeat:    now,
		transactions:     make(map[string]transactions.Transaction),
		log:              log,
	}
}

func (s *Session) NewTransaction(sqlTx *sql.SQLTx, mode schema.TxMode, db database.DB) (transactions.Transaction, error) {
	s.Lock()
	defer s.Unlock()
	if mode == schema.TxMode_READ_WRITE {
		if s.readWriteTxOngoing {
			return nil, ErrReadWriteTxOngoing
		}
		s.readWriteTxOngoing = true
	}
	transactionID := xid.New().String()
	tx := transactions.NewTransaction(sqlTx, transactionID, mode, db, s.id)
	s.transactions[transactionID] = tx
	return tx, nil
}

func (s *Session) RemoveTransaction(transactionID string) {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.transactions[transactionID]; ok {
		s.removeTransaction(transactionID)
	}
}

// not thread safe
func (s *Session) removeTransaction(transactionID string) {
	if tx, ok := s.transactions[transactionID]; ok {
		if tx.GetMode() == schema.TxMode_READ_WRITE {
			s.readWriteTxOngoing = false
		}
		delete(s.transactions, transactionID)
	}
}

func (s *Session) RollbackTransactions() {
	s.Lock()
	defer s.Unlock()
	for _, tx := range s.transactions {
		s.log.Debugf("Deleting transaction %s", tx.GetID())
		err := tx.Rollback()
		s.log.Errorf("transaction %s cancelled with error %v", tx.GetID(), err)
		s.removeTransaction(tx.GetID())
	}
}

func (s *Session) GetTransaction(transactionID string) (transactions.Transaction, error) {
	s.RLock()
	defer s.RUnlock()
	tx, ok := s.transactions[transactionID]
	if !ok {
		return nil, transactions.ErrTransactionNotFound
	}
	return tx, nil
}

func GetSessionIDFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrNoSessionAuthDataProvided
	}
	authHeader, ok := md["sessionid"]
	if !ok || len(authHeader) < 1 {
		return "", ErrNoSessionAuthDataProvided
	}
	sessionID := authHeader[0]
	if sessionID == "" {
		return "", ErrNoSessionIDPresent
	}
	return sessionID, nil
}

func GetTransactionIDFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", ErrNoTransactionAuthDataProvided
	}
	authHeader, ok := md["transactionid"]
	if !ok || len(authHeader) < 1 {
		return "", ErrNoTransactionAuthDataProvided
	}
	sessionID := authHeader[0]
	if sessionID == "" {
		return "", ErrNoTransactionIDPresent
	}
	return sessionID, nil
}

func (s *Session) GetUser() *auth.User {
	s.RLock()
	defer s.RUnlock()
	return s.user
}

func (s *Session) GetDatabaseID() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.databaseID
}

func (s *Session) SetDatabaseID(databaseID int64) {
	s.Lock()
	defer s.Unlock()
	s.databaseID = databaseID
}

func (s *Session) SetStatus(st Status) {
	s.Lock()
	defer s.Unlock()
	s.state = st
}

func (s *Session) GetStatus() Status {
	s.RLock()
	defer s.RUnlock()
	return s.state
}

func (s *Session) GetLastActivityTime() time.Time {
	s.RLock()
	defer s.RUnlock()
	return s.lastActivityTime
}

func (s *Session) SetLastActivityTime(t time.Time) {
	s.Lock()
	defer s.Unlock()
	s.lastActivityTime = t
}

func (s *Session) GetLastHeartBeat() time.Time {
	s.RLock()
	defer s.RUnlock()
	return s.lastHeartBeat
}

func (s *Session) SetLastHeartBeat(t time.Time) {
	s.Lock()
	defer s.Unlock()
	s.lastHeartBeat = t
}

func (s *Session) GetCreationTime() time.Time {
	s.RLock()
	defer s.RUnlock()
	return s.creationTime
}

func (s *Session) GetReadWriteTxOngoing() bool {
	s.RLock()
	defer s.RUnlock()
	return s.readWriteTxOngoing
}
