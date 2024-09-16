/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package replication

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/codenotary/immudb/cmd/version"
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/rs/xid"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrInvalidOptions = fmt.Errorf("%w: invalid options", ErrIllegalArguments)
var ErrAlreadyRunning = errors.New("already running")
var ErrAlreadyStopped = errors.New("already stopped")
var ErrReplicaDivergedFromPrimary = errors.New("replica diverged from primary")
var ErrNoSynchronousReplicationOnPrimary = errors.New("primary is not running with synchronous replication")
var ErrInvalidReplicationMetadata = errors.New("invalid replication metadata retrieved")
var ErrPrimaryServerVersionMismatch = errors.New("primary server version does not match")

type prefetchTxEntry struct {
	data    []byte
	addedAt time.Time
}

type TxReplicator struct {
	uuid xid.ID

	db   database.DB
	opts *Options

	_primaryDB string // just a string denoting primary database i.e. db@host:port

	logger logger.Logger

	context    context.Context
	cancelFunc context.CancelFunc

	client client.ImmuClient

	streamSrvFactory stream.ServiceFactory

	exportTxStream         schema.ImmuService_StreamExportTxClient
	exportTxStreamReceiver stream.MsgReceiver

	lastTx uint64

	prefetchTxBuffer       chan prefetchTxEntry // buffered channel of exported txs
	replicationConcurrency int

	allowTxDiscarding  bool
	skipIntegrityCheck bool
	waitForIndexing    bool

	delayer             Delayer
	consecutiveFailures int

	running bool
	err     error

	mutex sync.Mutex

	metrics metrics
}

func NewTxReplicator(uuid xid.ID, db database.DB, opts *Options, logger logger.Logger) (*TxReplicator, error) {
	if db == nil || logger == nil {
		return nil, fmt.Errorf("%w: no database or logger provided", ErrIllegalArguments)
	}

	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	return &TxReplicator{
		uuid:                   uuid,
		db:                     db,
		opts:                   opts,
		logger:                 logger,
		_primaryDB:             fullAddress(opts.primaryDatabase, opts.primaryHost, opts.primaryPort),
		streamSrvFactory:       stream.NewStreamServiceFactory(opts.streamChunkSize),
		prefetchTxBuffer:       make(chan prefetchTxEntry, opts.prefetchTxBufferSize),
		replicationConcurrency: opts.replicationCommitConcurrency,
		allowTxDiscarding:      opts.allowTxDiscarding,
		skipIntegrityCheck:     opts.skipIntegrityCheck,
		waitForIndexing:        opts.waitForIndexing,
		delayer:                opts.delayer,
		metrics:                metricsForDb(db.GetName()),
	}, nil
}

func (txr *TxReplicator) handleError(err error) (terminate bool) {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	if err == nil {
		txr.consecutiveFailures = 0
		return false
	}

	if errors.Is(err, ErrAlreadyStopped) || errors.Is(err, ErrReplicaDivergedFromPrimary) || errors.Is(err, ErrPrimaryServerVersionMismatch) {
		return true
	}

	txr.consecutiveFailures++

	txr.logger.Infof("Replication error on database '%s' from '%s' (%d consecutive failures). Reason: %s",
		txr.db.GetName(),
		txr._primaryDB,
		txr.consecutiveFailures,
		err.Error())

	timer := time.NewTimer(txr.delayer.DelayAfter(txr.consecutiveFailures))
	defer timer.Stop()

	select {
	case <-txr.context.Done():
		timer.Stop()
		return true
	case <-timer.C:
	}

	retryableError := !strings.Contains(err.Error(), "no session found")

	if txr.consecutiveFailures >= 3 || !retryableError {
		txr.disconnect()
	}

	return false
}

func (txr *TxReplicator) Start() error {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	if txr.running {
		return ErrAlreadyRunning
	}

	txr.logger.Infof("Initializing replication from '%s' to '%s'...", txr._primaryDB, txr.db.GetName())

	txr.context, txr.cancelFunc = context.WithCancel(context.Background())

	txr.running = true

	go txr.replicationLoop()

	txr.metrics.reset()

	for i := 0; i < txr.replicationConcurrency; i++ {
		go func() {
			txr.metrics.replicators.Inc()
			defer txr.metrics.replicators.Dec()

			for etx := range txr.prefetchTxBuffer {
				txr.metrics.txWaitQueueHistogram.Observe(time.Since(etx.addedAt).Seconds())

				if !txr.replicateSingleTx(etx.data) {
					break
				}
			}
		}()
	}

	txr.logger.Infof("Replication from '%s' to '%s' successfully initialized", txr._primaryDB, txr.db.GetName())

	return nil
}

func (txr *TxReplicator) replicationLoop() {
	txr.logger.Infof("Replication for '%s' started fetching transaction from '%s'...", txr.db.GetName(), txr._primaryDB)

	var err error
	for {
		err = txr.fetchNextTx()
		if txr.handleError(err) {
			break
		}
	}

	txr.logger.Infof("Replication for '%s' stopped fetching transaction from '%s'", txr.db.GetName(), txr._primaryDB)

	if errors.Is(err, ErrReplicaDivergedFromPrimary) || errors.Is(err, ErrPrimaryServerVersionMismatch) {
		txr.stopWithErr(err)
	}
}

func (txr *TxReplicator) replicateSingleTx(data []byte) bool {
	txr.metrics.replicatorsActive.Inc()
	defer txr.metrics.replicatorsActive.Dec()
	defer txr.metrics.replicationTimeHistogramTimer().ObserveDuration()

	consecutiveFailures := 0

	// replication must be retried as many times as necessary
	for {
		_, err := txr.db.ReplicateTx(txr.context, data, txr.skipIntegrityCheck, txr.waitForIndexing)
		if err == nil {
			break // transaction successfully replicated
		}
		if errors.Is(err, ErrAlreadyStopped) {
			return false
		}

		if strings.Contains(err.Error(), "tx already committed") {
			break // transaction successfully replicated
		}

		txr.logger.Infof("Failed to replicate transaction from '%s' to '%s'. Reason: %s", txr._primaryDB, txr.db.GetName(), err.Error())

		consecutiveFailures++

		if !txr.replicationFailureDelay(consecutiveFailures) {
			return false
		}
	}

	return true
}

func (txr *TxReplicator) replicationFailureDelay(consecutiveFailures int) bool {
	txr.metrics.replicationRetries.Inc()

	txr.metrics.replicatorsInRetryDelay.Inc()
	defer txr.metrics.replicatorsInRetryDelay.Dec()

	timer := time.NewTimer(txr.delayer.DelayAfter(consecutiveFailures))
	defer timer.Stop()

	select {
	case <-txr.context.Done():
		timer.Stop()
		return false
	case <-timer.C:
		return true
	}
}

func fullAddress(db, address string, port int) string {
	return fmt.Sprintf("%s@%s:%d", db, address, port)
}

func (txr *TxReplicator) connect() error {
	txr.logger.Infof("Connecting to '%s':'%d' for database '%s'...",
		txr.opts.primaryHost,
		txr.opts.primaryPort,
		txr.db.GetName())

	txr.client = txr.opts.clientFactory(txr.opts.primaryHost, txr.opts.primaryPort)

	err := txr.client.OpenSession(
		txr.context, []byte(txr.opts.primaryUsername), []byte(txr.opts.primaryPassword), txr.opts.primaryDatabase)
	if err != nil {
		return err
	}

	info, err := txr.client.ServerInfo(txr.context, &schema.ServerInfoRequest{})
	if err != nil {
		txr.logger.Errorf("Connection to %s failed: unable to get primary server info: %w", txr.db.GetName(), err)
		return err
	}
	if info.Version != version.Version {
		return ErrPrimaryServerVersionMismatch
	}

	txr.logger.Infof("Connection to '%s':'%d' for database '%s' successfully established",
		txr.opts.primaryHost,
		txr.opts.primaryPort,
		txr.db.GetName())

	txr.exportTxStream, err = txr.client.StreamExportTx(txr.context)
	if err != nil {
		return err
	}

	txr.exportTxStreamReceiver = txr.streamSrvFactory.NewMsgReceiver(txr.exportTxStream)

	return nil
}

func (txr *TxReplicator) disconnect() {
	if txr.client == nil {
		return
	}

	txr.logger.Infof("Disconnecting from '%s':'%d' for database '%s'...", txr.opts.primaryHost, txr.opts.primaryPort, txr.db.GetName())

	if txr.exportTxStream != nil {
		txr.exportTxStream.CloseSend()
		txr.exportTxStream = nil
	}

	txr.client.CloseSession(txr.context)
	txr.client = nil

	txr.logger.Infof("Disconnected from '%s':'%d' for database '%s'", txr.opts.primaryHost, txr.opts.primaryPort, txr.db.GetName())
}

func (txr *TxReplicator) fetchNextTx() error {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	if !txr.running {
		return ErrAlreadyStopped
	}

	if txr.exportTxStream == nil {
		err := txr.connect()
		if err != nil {
			return err
		}
	}

	commitState, err := txr.db.CurrentState()
	if err != nil {
		return err
	}

	if txr.lastTx == 0 {
		txr.lastTx = commitState.PrecommittedTxId
	}

	nextTx := txr.lastTx + 1

	var state *schema.ReplicaState

	syncReplicationEnabled := txr.db.IsSyncReplicationEnabled()
	if syncReplicationEnabled {
		state = &schema.ReplicaState{
			UUID:             txr.uuid.String(),
			CommittedTxID:    commitState.TxId,
			CommittedAlh:     commitState.TxHash,
			PrecommittedTxID: commitState.PrecommittedTxId,
			PrecommittedAlh:  commitState.PrecommittedTxHash,
		}
	}

	req := &schema.ExportTxRequest{
		Tx:                 nextTx,
		ReplicaState:       state,
		AllowPreCommitted:  syncReplicationEnabled,
		SkipIntegrityCheck: txr.skipIntegrityCheck,
	}

	txr.exportTxStream.Send(req)

	etx, emd, err := txr.exportTxStreamReceiver.ReadFully()
	if err != nil {
		defer txr.disconnect()
	}

	if err != nil && !errors.Is(err, io.EOF) {
		if strings.Contains(err.Error(), "replica commit state diverged from primary") {
			txr.logger.Errorf("replica commit state at '%s' diverged from primary's", txr.db.GetName())
			return ErrReplicaDivergedFromPrimary
		}

		if strings.Contains(err.Error(), "replica precommit state diverged from primary") {
			if !txr.allowTxDiscarding {
				txr.logger.Errorf("replica precommit state at '%s' diverged from primary's", txr.db.GetName())
				return ErrReplicaDivergedFromPrimary
			}

			txr.logger.Infof("discarding precommit txs since %d from '%s'. Reason: %s", nextTx, txr.db.GetName(), err.Error())

			err = txr.db.DiscardPrecommittedTxsSince(commitState.TxId + 1)
			if err != nil {
				return err
			}

			txr.lastTx = commitState.TxId

			txr.logger.Infof("precommit txs successfully discarded from '%s'", txr.db.GetName())

			return nil
		}

		return err
	}

	if syncReplicationEnabled {
		bMayCommitUpToTxID, ok := emd["may-commit-up-to-txid-bin"]
		if !ok {
			return ErrNoSynchronousReplicationOnPrimary
		}

		bmayCommitUpToAlh, ok := emd["may-commit-up-to-alh-bin"]
		if !ok {
			return ErrNoSynchronousReplicationOnPrimary
		}

		bCommittedTxID, ok := emd["committed-txid-bin"]
		if !ok {
			return ErrNoSynchronousReplicationOnPrimary
		}

		if len(bMayCommitUpToTxID) != 8 ||
			len(bmayCommitUpToAlh) != sha256.Size ||
			len(bCommittedTxID) != 8 {
			return ErrInvalidReplicationMetadata
		}

		mayCommitUpToTxID := binary.BigEndian.Uint64(bMayCommitUpToTxID)
		committedTxID := binary.BigEndian.Uint64(bCommittedTxID)

		var mayCommitUpToAlh [sha256.Size]byte
		copy(mayCommitUpToAlh[:], bmayCommitUpToAlh)

		txr.metrics.primaryCommittedTxID.Set(float64(committedTxID))
		txr.metrics.allowCommitUpToTxID.Set(float64(mayCommitUpToTxID))

		if mayCommitUpToTxID > commitState.TxId {
			err = txr.db.AllowCommitUpto(mayCommitUpToTxID, mayCommitUpToAlh)
			if err != nil {
				if strings.Contains(err.Error(), "replica commit state diverged from") {
					txr.logger.Errorf("replica commit state at '%s' diverged from primary's", txr.db.GetName())
					return ErrReplicaDivergedFromPrimary
				}

				return err
			}
		}
	}

	if len(etx) > 0 {
		// in some cases the transaction is not provided but only the primary commit state
		txr.prefetchTxBuffer <- prefetchTxEntry{
			data:    etx,
			addedAt: time.Now(),
		}
		txr.lastTx++
	}
	return nil
}

func (txr *TxReplicator) Stop() error {
	return txr.stopWithErr(nil)
}

func (txr *TxReplicator) stopWithErr(err error) error {
	if txr.cancelFunc != nil {
		txr.cancelFunc()
	}

	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	if !txr.running {
		return ErrAlreadyStopped
	}

	txr.logger.Infof("Stopping replication of database '%s'...", txr.db.GetName())

	close(txr.prefetchTxBuffer)

	txr.disconnect()

	txr.running = false
	txr.err = err

	txr.logger.Infof("Replication of database '%s' successfully stopped", txr.db.GetName())

	return nil
}

func (txr *TxReplicator) Error() error {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	return txr.err
}
