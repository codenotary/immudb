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

package replication

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/rs/xid"
	"google.golang.org/grpc/metadata"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrAlreadyRunning = errors.New("already running")
var ErrAlreadyStopped = errors.New("already stopped")

type TxReplicator struct {
	uuid xid.ID

	db   database.DB
	opts *Options

	logger logger.Logger

	mainContext context.Context
	cancelFunc  context.CancelFunc

	streamSrvFactory stream.ServiceFactory
	client           client.ImmuClient
	clientContext    context.Context

	delayer        Delayer
	failedAttempts int

	running bool

	mutex sync.Mutex
}

func NewTxReplicator(uuid xid.ID, db database.DB, opts *Options, logger logger.Logger) (*TxReplicator, error) {
	if db == nil || logger == nil || opts == nil || !opts.Valid() {
		return nil, ErrIllegalArguments
	}

	return &TxReplicator{
		uuid:             uuid,
		db:               db,
		opts:             opts,
		logger:           logger,
		streamSrvFactory: stream.NewStreamServiceFactory(opts.streamChunkSize),
		delayer:          opts.delayer,
	}, nil
}

func (txr *TxReplicator) Start() error {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	if txr.running {
		return ErrAlreadyRunning
	}

	masterDB := fullAddress(txr.opts.masterDatabase, txr.opts.masterAddress, txr.opts.masterPort)

	txr.logger.Infof("Initializing replication from '%s' to '%s'...", masterDB, txr.db.GetName())

	txr.mainContext, txr.cancelFunc = context.WithCancel(context.Background())

	txr.running = true

	go func() {
		defer func() {
			if txr.client != nil {
				txr.disconnect()
			}
		}()

		for {
			if txr.client == nil {
				err := txr.connect()
				if err == nil {
					txr.failedAttempts = 0
					continue
				}

				txr.failedAttempts++

				txr.logger.Infof("Failed to connect with '%s' for database '%s' (%d failed attempts). Reason: %v",
					masterDB,
					txr.db.GetName(),
					txr.failedAttempts,
					err)

				timer := time.NewTimer(txr.delayer.DelayAfter(txr.failedAttempts))
				select {
				case <-txr.mainContext.Done():
					timer.Stop()
					return
				case <-timer.C:
				}

				continue
			}

			err := txr.nextTx()
			if err != nil {
				txr.logger.Infof("Failed to replicate transaction from '%s' to '%s'. Reason: %v", masterDB, txr.db.GetName(), err)

				txr.failedAttempts++
				if txr.failedAttempts == 3 {
					txr.disconnect()
				}

				timer := time.NewTimer(txr.delayer.DelayAfter(txr.failedAttempts))
				select {
				case <-txr.mainContext.Done():
					timer.Stop()
					return
				case <-timer.C:
				}

				continue
			}
		}
	}()

	txr.logger.Infof("Replication from '%s' to '%s' succesfully initialized", masterDB, txr.db.GetName())

	return nil
}

func fullAddress(db, address string, port int) string {
	return fmt.Sprintf("%s@%s:%d", db, address, port)
}

func (txr *TxReplicator) connect() error {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	txr.logger.Infof("Connecting to '%s':'%d' for database '%s'...",
		txr.opts.masterAddress,
		txr.opts.masterPort,
		txr.db.GetName())

	opts := client.DefaultOptions().WithAddress(txr.opts.masterAddress).WithPort(txr.opts.masterPort)
	client, err := client.NewImmuClient(opts)
	if err != nil {
		return err
	}

	login, err := client.Login(txr.mainContext, []byte(txr.opts.followerUsername), []byte(txr.opts.followerPassword))
	if err != nil {
		return err
	}

	txr.clientContext = metadata.NewOutgoingContext(txr.mainContext, metadata.Pairs("authorization", login.GetToken()))

	udr, err := client.UseDatabase(txr.clientContext, &schema.Database{DatabaseName: txr.opts.masterDatabase})
	if err != nil {
		return err
	}

	txr.clientContext = metadata.NewOutgoingContext(txr.clientContext, metadata.Pairs("authorization", udr.GetToken()))

	txr.client = client

	txr.logger.Infof("Connection to '%s':'%d' for database '%s' successfully established",
		txr.opts.masterAddress,
		txr.opts.masterPort,
		txr.db.GetName())

	return nil
}

func (txr *TxReplicator) disconnect() {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	txr.logger.Infof("Disconnecting from '%s':'%d' for database '%s'...", txr.opts.masterAddress, txr.opts.masterPort, txr.db.GetName())

	txr.client.Logout(txr.clientContext)
	txr.client.Disconnect()

	txr.client = nil

	txr.logger.Infof("Disconnected from '%s':'%d' for database '%s'", txr.opts.masterAddress, txr.opts.masterPort, txr.db.GetName())
}

func (txr *TxReplicator) nextTx() error {
	commitState, err := txr.db.CurrentCommitState()
	if err != nil {
		return err
	}

	precommitState, err := txr.db.CurrentPreCommitState()
	if err != nil {
		return err
	}

	state := &schema.FollowerState{
		UUID:             txr.uuid.String(),
		CommittedTxID:    commitState.TxId,
		CommittedAlh:     commitState.TxHash,
		PrecommittedTxID: precommitState.TxId,
		PrecommittedAlh:  precommitState.TxHash,
	}

	exportTxStream, err := txr.client.ExportTx(txr.clientContext, &schema.ExportTxRequest{
		Tx:                  precommitState.TxId + 1,
		FollowerState:       state,
		IncludePreCommitted: true,
	})
	if err != nil {
		return err
	}

	receiver := txr.streamSrvFactory.NewMsgReceiver(exportTxStream)
	txbs, err := receiver.ReadFully()

	if err != nil && err != io.EOF {
		return err
	}

	if len(txbs) > 0 {
		_, err = txr.db.ReplicateTx(txbs)
		if err != nil {
			return err
		}
	}

	md := exportTxStream.Trailer()

	if len(md.Get("may-commit-up-to-txid-bin")) > 0 && len(md.Get("may-commit-up-to-alh-bin")) > 0 {
		mayCommitUpToTxID := binary.BigEndian.Uint64([]byte(md.Get("may-commit-up-to-txid-bin")[0]))

		var mayCommitUpToAlh [sha256.Size]byte
		copy(mayCommitUpToAlh[:], []byte(md.Get("may-commit-up-to-alh-bin")[0]))

		err = txr.db.AllowCommitUpto(mayCommitUpToTxID, mayCommitUpToAlh)
		if err != nil {
			return err
		}
	} else {
		// backward compatibility with older immudb servers which only export committed transactions
		precommitState, err := txr.db.CurrentPreCommitState()
		if err != nil {
			return err
		}

		err = txr.db.AllowCommitUpto(precommitState.TxId, schema.DigestFromProto(precommitState.TxHash))
		if err != nil {
			return err
		}
	}

	return nil
}

func (txr *TxReplicator) Stop() error {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	txr.logger.Infof("Stopping replication of database '%s'...", txr.db.GetName())

	if !txr.running {
		return ErrAlreadyStopped
	}

	if txr.cancelFunc != nil {
		txr.cancelFunc()
	}

	txr.running = false

	txr.logger.Infof("Replication of database '%s' successfully stopped", txr.db.GetName())

	return nil
}
