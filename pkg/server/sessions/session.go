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

package sessions

import (
	"context"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/embedded/sql"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/codenotary/immudb/pkg/server/sessions/internal/transactions"
	"google.golang.org/grpc/metadata"
)

// DefaultMaxDocumentReadersCacheSize is the default maximum number of document readers to keep in cache
const DefaultMaxDocumentReadersCacheSize = 1

var (
	ErrPaginatedDocumentReaderNotFound = errors.New("document reader not found")
)

type PaginatedDocumentReader struct {
	Reader         document.DocumentReader // reader to read from
	Query          *protomodel.Query
	LastPageNumber uint32 // last read page number
	LastPageSize   uint32 // number of items per page
}

type Session struct {
	mux              sync.RWMutex
	id               string
	user             *auth.User
	database         database.DB
	creationTime     time.Time
	lastActivityTime time.Time
	transactions     map[string]transactions.Transaction
	documentReaders  *cache.LRUCache // track searchID to document.DocumentReader
	log              logger.Logger
}

func NewSession(sessionID string, user *auth.User, db database.DB, log logger.Logger) *Session {
	now := time.Now()
	lruCache, _ := cache.NewLRUCache(DefaultMaxDocumentReadersCacheSize)

	return &Session{
		id:               sessionID,
		user:             user,
		database:         db,
		creationTime:     now,
		lastActivityTime: now,
		transactions:     make(map[string]transactions.Transaction),
		log:              log,
		documentReaders:  lruCache,
	}
}

func (s *Session) NewTransaction(ctx context.Context, opts *sql.TxOptions) (transactions.Transaction, error) {
	s.mux.Lock()
	defer s.mux.Unlock()

	tx, err := transactions.NewTransaction(ctx, opts, s.database, s.id)
	if err != nil {
		return nil, err
	}

	s.transactions[tx.GetID()] = tx
	return tx, nil
}

func (s *Session) RemoveTransaction(transactionID string) error {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.removeTransaction(transactionID)
}

// not thread safe
func (s *Session) removeTransaction(transactionID string) error {
	if _, ok := s.transactions[transactionID]; ok {
		delete(s.transactions, transactionID)
		return nil
	}
	return ErrTransactionNotFound
}

func (s *Session) CloseDocumentReaders() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	merr := multierr.NewMultiErr()

	searchIDs := make([]string, 0)
	if err := s.documentReaders.Apply(func(k, v interface{}) error {
		searchIDs = append(searchIDs, k.(string))
		return nil
	}); err != nil {
		s.log.Errorf("Error while removing paginated reader: %v", err)
		merr.Append(err)
	}

	for _, searchID := range searchIDs {
		if err := s.deleteDocumentReader(searchID); err != nil {
			s.log.Errorf("Error while removing paginated reader: %v", err)
			merr.Append(err)
		}
	}

	return merr.Reduce()
}

func (s *Session) RollbackTransactions() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	merr := multierr.NewMultiErr()

	for _, tx := range s.transactions {
		s.log.Debugf("Deleting transaction %s", tx.GetID())

		if err := tx.Rollback(); err != nil {
			s.log.Errorf("Error while rolling back transaction %s: %v", tx.GetID(), err)
			merr.Append(err)
			continue
		}

		if err := s.removeTransaction(tx.GetID()); err != nil {
			s.log.Errorf("Error while removing transaction %s: %v", tx.GetID(), err)
			merr.Append(err)
			continue
		}
	}

	return merr.Reduce()
}

func (s *Session) GetID() string {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.id
}

func (s *Session) GetTransaction(transactionID string) (transactions.Transaction, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()

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
	transactionID := authHeader[0]
	if transactionID == "" {
		return "", ErrNoTransactionIDPresent
	}
	return transactionID, nil
}

func (s *Session) GetUser() *auth.User {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return s.user
}

func (s *Session) GetDatabase() database.DB {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return s.database
}

func (s *Session) SetDatabase(db database.DB) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.database = db
}

func (s *Session) GetLastActivityTime() time.Time {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return s.lastActivityTime
}

func (s *Session) SetLastActivityTime(t time.Time) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.lastActivityTime = t
}

func (s *Session) GetCreationTime() time.Time {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return s.creationTime
}

func (s *Session) SetPaginatedDocumentReader(searchID string, reader *PaginatedDocumentReader) {
	s.mux.Lock()
	defer s.mux.Unlock()

	// add the reader to the documentReaders map
	s.documentReaders.Put(searchID, reader)
}

func (s *Session) GetDocumentReader(searchID string) (*PaginatedDocumentReader, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()

	// get the io.Reader object for the specified searchID
	val, err := s.documentReaders.Get(searchID)
	if err != nil {
		return nil, ErrPaginatedDocumentReaderNotFound
	}

	reader := val.(*PaginatedDocumentReader)

	return reader, nil
}

func (s *Session) deleteDocumentReader(searchID string) error {
	// get the io.Reader object for the specified searchID
	val, err := s.documentReaders.Get(searchID)
	if err != nil {
		return ErrPaginatedDocumentReaderNotFound
	}

	reader := val.(*PaginatedDocumentReader)

	// close the reader
	err = reader.Reader.Close()
	s.documentReaders.Pop(searchID)
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) DeleteDocumentReader(searchID string) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	return s.deleteDocumentReader(searchID)
}

func (s *Session) UpdatePaginatedDocumentReader(searchID string, lastPage uint32, lastPageSize uint32) error {
	s.mux.Lock()
	defer s.mux.Unlock()

	// get the io.Reader object for the specified searchID
	val, err := s.documentReaders.Get(searchID)
	if err != nil {
		return ErrPaginatedDocumentReaderNotFound
	}

	reader := val.(*PaginatedDocumentReader)
	reader.LastPageNumber = lastPage
	reader.LastPageSize = lastPageSize

	return nil
}

func (s *Session) GetDocumentReadersCount() int {
	s.mux.RLock()
	defer s.mux.RUnlock()

	return s.documentReaders.EntriesCount()
}
