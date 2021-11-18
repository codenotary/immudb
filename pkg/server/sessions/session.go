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
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/server/transactions"
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
	sync.Mutex
	state              Status
	user               *auth.User
	databaseID         int64
	creationTime       time.Time
	lastActivityTime   time.Time
	lastHeartBeat      time.Time
	readWriteTxOngoing bool
	transactions       map[string]*transactions.Transaction
}

func NewSession(user *auth.User, databaseID int64) *Session {
	now := time.Now()
	return &Session{
		state:            ACTIVE,
		user:             user,
		databaseID:       databaseID,
		creationTime:     now,
		lastActivityTime: now,
		lastHeartBeat:    now,
		transactions:     make(map[string]*transactions.Transaction),
	}
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

func (s *Session) GetUser() *auth.User {
	s.Lock()
	defer s.Unlock()
	return s.user
}

func (s *Session) GetDatabaseID() int64 {
	s.Lock()
	defer s.Unlock()
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
	s.Lock()
	defer s.Unlock()
	return s.state
}

func (s *Session) GetReadWriteTxOngoing() bool {
	s.Lock()
	defer s.Unlock()
	return s.readWriteTxOngoing
}

func (s *Session) SetReadWriteTxOngoing(ongoing bool) {
	s.Lock()
	defer s.Unlock()
	s.readWriteTxOngoing = ongoing
}

func (s *Session) TransactionPresent(transactionID string) bool {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.transactions[transactionID]; ok {
		return true
	}
	return false
}

func (s *Session) RemoveTransaction(transactionID string) {
	s.Lock()
	defer s.Unlock()
	delete(s.transactions, transactionID)
}

func (s *Session) AddTransaction(transactionID string, readWrite bool) {
	s.Lock()
	defer s.Unlock()
	s.transactions[transactionID] = &transactions.Transaction{ReadWrite: readWrite}
}

func (s *Session) GetTransaction(transactionID string) *transactions.Transaction {
	s.Lock()
	defer s.Unlock()
	return s.transactions[transactionID]
}
