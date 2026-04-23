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

package sessions

import (
	"context"
	"encoding/base64"
	"hash/fnv"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/server/sessions/internal/transactions"
)

const infinity = time.Duration(math.MaxInt64)

// sessionShardCount is the number of independent shards the session map is
// split into. 32 was chosen empirically as a balance between contention
// reduction (each shard's RWMutex is independent) and per-shard footprint
// (the shards array is inlined into the manager struct).
const sessionShardCount = 32

// sessionShard is one bucket of the sharded session store. Each shard has
// its own RWMutex so that lookups, inserts and deletes against different
// session IDs proceed in parallel — the previous single-mutex design
// serialized every session-authed RPC through the manager even after the
// Lock→RLock fix in commit 1b653d42.
type sessionShard struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

type manager struct {
	// runningMu guards the lifecycle fields (running/ticker/done). It is
	// independent of the per-shard locks so the guard goroutine starting
	// or stopping does not contend with hot-path session operations.
	runningMu sync.Mutex
	running   bool
	ticker    *time.Ticker
	done      chan bool

	shards [sessionShardCount]sessionShard

	// sessionsCount is the total number of live sessions across all shards.
	// Maintained as an atomic counter so SessionCount() and the MaxSessions
	// admission check in NewSession do not need to lock all shards.
	sessionsCount atomic.Int64

	logger  logger.Logger
	options Options
}

type Manager interface {
	NewSession(user *auth.User, db database.DB) (*Session, error)
	SessionPresent(sessionID string) bool
	DeleteSession(sessionID string) error
	UpdateSessionActivityTime(sessionID string)
	StartSessionsGuard() error
	StopSessionsGuard() error
	GetSession(sessionID string) (*Session, error)
	SessionCount() int
	GetTransactionFromContext(ctx context.Context) (transactions.Transaction, error)
	GetSessionFromContext(ctx context.Context) (*Session, error)
	DeleteTransaction(transactions.Transaction) error
	CommitTransaction(ctx context.Context, transaction transactions.Transaction) ([]*sql.SQLTx, error)
	RollbackTransaction(transaction transactions.Transaction) error
	CloseSessionsForUser(username string) error
}

func NewManager(options *Options) (*manager, error) {
	if options == nil {
		return nil, ErrInvalidOptionsProvided
	}

	err := options.Validate()
	if err != nil {
		return nil, err
	}

	guard := &manager{
		ticker:  time.NewTicker(options.SessionGuardCheckInterval),
		done:    make(chan bool),
		logger:  logger.NewSimpleLogger("immudb session guard", os.Stdout),
		options: *options,
	}
	for i := range guard.shards {
		guard.shards[i].sessions = make(map[string]*Session)
	}

	guard.options.Normalize()

	return guard, nil
}

// shardFor returns the shard responsible for the given sessionID. Uses
// FNV-1a 64-bit hash for low-cost, well-distributed bucketing.
func (sm *manager) shardFor(sessionID string) *sessionShard {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sessionID))
	return &sm.shards[h.Sum64()%sessionShardCount]
}

func (sm *manager) NewSession(user *auth.User, db database.DB) (*Session, error) {
	// Admission check via the atomic counter — stale by at most a few
	// concurrent NewSession calls, which is acceptable for a soft cap.
	if int(sm.sessionsCount.Load()) >= sm.options.MaxSessions {
		sm.logger.Warningf("max sessions reached")
		return nil, ErrMaxSessionsReached
	}

	randomBytes := make([]byte, 32)
	n, err := sm.options.RandSource.Read(randomBytes)
	if err != nil {
		sm.logger.Errorf("cant create session id: %v", err)
		return nil, ErrCantCreateSessionID
	}
	if n < len(randomBytes) {
		sm.logger.Errorf("cant create session id: could produce enough random data")
		return nil, ErrCantCreateSessionID
	}

	sessionID := base64.URLEncoding.EncodeToString(randomBytes)
	sess := NewSession(sessionID, user, db, sm.logger)

	shard := sm.shardFor(sessionID)
	shard.mu.Lock()
	shard.sessions[sessionID] = sess
	shard.mu.Unlock()

	sm.sessionsCount.Add(1)
	sm.logger.Debugf("created session %s", sessionID)

	return sess, nil
}

func (sm *manager) SessionPresent(sessionID string) bool {
	shard := sm.shardFor(sessionID)
	shard.mu.RLock()
	_, isPresent := shard.sessions[sessionID]
	shard.mu.RUnlock()
	return isPresent
}

func (sm *manager) GetSession(sessionID string) (*Session, error) {
	shard := sm.shardFor(sessionID)
	shard.mu.RLock()
	sess, ok := shard.sessions[sessionID]
	shard.mu.RUnlock()

	if !ok {
		return nil, ErrSessionNotFound
	}
	return sess, nil
}

// DeleteSession atomically detaches the session from its shard and then
// releases its resources out of lock — a slow CloseDocumentReaders or
// RollbackTransactions call must not block other sessions on the same
// shard from being looked up.
func (sm *manager) DeleteSession(sessionID string) error {
	shard := sm.shardFor(sessionID)
	shard.mu.Lock()
	sess, ok := shard.sessions[sessionID]
	if !ok {
		shard.mu.Unlock()
		return ErrSessionNotFound
	}
	delete(shard.sessions, sessionID)
	shard.mu.Unlock()

	sm.sessionsCount.Add(-1)
	return releaseSession(sess)
}

// releaseSession runs the per-session close/rollback work assumed to be
// expensive enough to warrant doing out-of-lock. Returns the combined
// result of CloseDocumentReaders and RollbackTransactions.
func releaseSession(sess *Session) error {
	merr := multierr.NewMultiErr()
	if err := sess.CloseDocumentReaders(); err != nil {
		merr.Append(err)
	}
	if err := sess.RollbackTransactions(); err != nil {
		merr.Append(err)
	}
	return merr.Reduce()
}

// CloseSessionsForUser deletes every session owned by username across all
// shards. Each shard is processed independently: the deletion list is
// captured under the shard lock, then sessions are released out of lock.
func (sm *manager) CloseSessionsForUser(username string) error {
	merr := multierr.NewMultiErr()
	for i := range sm.shards {
		shard := &sm.shards[i]

		shard.mu.Lock()
		var toClose []*Session
		for id, sess := range shard.sessions {
			if sess.GetUser().Username == username {
				toClose = append(toClose, sess)
				delete(shard.sessions, id)
			}
		}
		shard.mu.Unlock()

		for _, sess := range toClose {
			sm.sessionsCount.Add(-1)
			if err := releaseSession(sess); err != nil {
				merr.Append(err)
			}
		}
	}
	return merr.Reduce()
}

func (sm *manager) UpdateSessionActivityTime(sessionID string) {
	// RLock on the shard is sufficient: this function only reads to resolve
	// the sessionID, and the actual lastActivityTime write is serialised
	// inside (*Session).SetLastActivityTime by the session's own mutex.
	shard := sm.shardFor(sessionID)
	shard.mu.RLock()
	sess, ok := shard.sessions[sessionID]
	shard.mu.RUnlock()

	if !ok {
		return
	}

	now := time.Now()
	sess.SetLastActivityTime(now)
	sm.logger.Debugf("updated last activity time for %s at %s", sessionID, now.Format(time.UnixDate))
}

func (sm *manager) SessionCount() int {
	return int(sm.sessionsCount.Load())
}

func (sm *manager) StartSessionsGuard() error {
	sm.runningMu.Lock()
	defer sm.runningMu.Unlock()

	if sm.running {
		return ErrGuardAlreadyRunning
	}

	// Recreate ticker and done channel so a Stop→Start cycle gets a live
	// ticker (Stop() calls ticker.Stop() which permanently disables it) and
	// a fresh done channel (the previous one was consumed by StopSessionsGuard).
	sm.ticker = time.NewTicker(sm.options.SessionGuardCheckInterval)
	sm.done = make(chan bool)
	sm.running = true

	go func(ticker *time.Ticker, done chan bool) {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				sm.expireSessions(time.Now())
			}
		}
	}(sm.ticker, sm.done)

	return nil
}

func (sm *manager) IsRunning() bool {
	sm.runningMu.Lock()
	defer sm.runningMu.Unlock()
	return sm.running
}

// StopSessionsGuard halts the expiry goroutine and clears all shards.
// The done-channel send must happen with runningMu unheld so that an
// expireSessions iteration in flight (which does not take runningMu)
// can complete before Stop tears the rest down.
func (sm *manager) StopSessionsGuard() error {
	sm.runningMu.Lock()
	if !sm.running {
		sm.runningMu.Unlock()
		return ErrGuardNotRunning
	}
	sm.running = false
	sm.ticker.Stop()
	done := sm.done
	sm.runningMu.Unlock()

	done <- true

	// Delete all sessions across shards. Phase split (collect under lock,
	// release out of lock) mirrors expireSessions to avoid stalling shard
	// lookups behind per-session I/O during shutdown.
	for i := range sm.shards {
		shard := &sm.shards[i]
		shard.mu.Lock()
		toClose := make([]*Session, 0, len(shard.sessions))
		for id, sess := range shard.sessions {
			toClose = append(toClose, sess)
			delete(shard.sessions, id)
		}
		shard.mu.Unlock()

		for _, sess := range toClose {
			sm.sessionsCount.Add(-1)
			if err := releaseSession(sess); err != nil {
				sm.logger.Errorf("releasing session on shutdown: %v", err)
			}
		}
	}

	sm.logger.Debugf("shutdown")
	return nil
}

// expireSessions iterates every shard and applies the same two-phase
// (collect-under-lock, release-out-of-lock) discipline that the previous
// single-map version used. Each shard is processed independently so
// concurrent GetSession/NewSession against other shards never block on
// expiry I/O.
func (sm *manager) expireSessions(now time.Time) (sessionsCount, inactiveSessCount, deletedSessCount int, err error) {
	// Cheap running check — guard can race with StopSessionsGuard but the
	// lifecycle shutdown drains the done channel, so a stale "running"
	// here is harmless.
	sm.runningMu.Lock()
	if !sm.running {
		sm.runningMu.Unlock()
		return 0, 0, 0, ErrGuardNotRunning
	}
	sm.runningMu.Unlock()

	sm.logger.Debugf("checking at %s", now.Format(time.UnixDate))

	type expiredSession struct {
		id     string
		sess   *Session
		reason string
	}

	var expired []expiredSession
	var remaining int

	for i := range sm.shards {
		shard := &sm.shards[i]
		shard.mu.Lock()
		for ID, sess := range shard.sessions {
			createdAt := sess.GetCreationTime()
			lastActivity := sess.GetLastActivityTime()

			switch {
			case now.Sub(createdAt) > sm.options.MaxSessionAgeTime:
				expired = append(expired, expiredSession{ID, sess, "MaxSessionAgeTime"})
				delete(shard.sessions, ID)
			case now.Sub(lastActivity) > sm.options.Timeout:
				expired = append(expired, expiredSession{ID, sess, "Timeout"})
				delete(shard.sessions, ID)
			case now.Sub(lastActivity) > sm.options.MaxSessionInactivityTime:
				inactiveSessCount++
			}
		}
		remaining += len(shard.sessions)
		shard.mu.Unlock()
	}

	for _, e := range expired {
		sm.logger.Debugf("removing session %s - exceeded %s", e.id, e.reason)
		sm.sessionsCount.Add(-1)
		if err := e.sess.CloseDocumentReaders(); err != nil {
			sm.logger.Errorf("closing document readers for %s: %v", e.id, err)
		}
		if err := e.sess.RollbackTransactions(); err != nil {
			sm.logger.Errorf("rolling back transactions for %s: %v", e.id, err)
		}
	}

	deletedSessCount = len(expired)

	sm.logger.Debugf("Open sessions count: %d\n", remaining)
	sm.logger.Debugf("Inactive sessions count: %d\n", inactiveSessCount)
	sm.logger.Debugf("Deleted sessions count: %d\n", deletedSessCount)

	return remaining, inactiveSessCount, deletedSessCount, nil
}

func (sm *manager) GetTransactionFromContext(ctx context.Context) (transactions.Transaction, error) {
	sessionID, err := GetSessionIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	sess, err := sm.GetSession(sessionID)
	if err != nil {
		return nil, err
	}

	transactionID, err := GetTransactionIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return sess.GetTransaction(transactionID)
}

func (sm *manager) GetSessionFromContext(ctx context.Context) (*Session, error) {
	sessionID, err := GetSessionIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return sm.GetSession(sessionID)
}

func (sm *manager) DeleteTransaction(tx transactions.Transaction) error {
	sessionID := tx.GetSessionID()
	sess, err := sm.GetSession(sessionID)
	if err != nil {
		return err
	}
	return sess.RemoveTransaction(tx.GetID())
}

func (sm *manager) CommitTransaction(ctx context.Context, tx transactions.Transaction) ([]*sql.SQLTx, error) {
	err := sm.DeleteTransaction(tx)
	if err != nil {
		return nil, err
	}
	cTxs, err := tx.Commit(ctx)
	if err != nil {
		return nil, err
	}
	return cTxs, nil
}

func (sm *manager) RollbackTransaction(tx transactions.Transaction) error {
	err := sm.DeleteTransaction(tx)
	if err != nil {
		return err
	}
	return tx.Rollback()
}
