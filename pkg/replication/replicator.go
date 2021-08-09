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

package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/stream"
	"google.golang.org/grpc/metadata"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrAlreadyRunning = errors.New("already running")
var ErrAlreadyStopped = errors.New("already stopped")

type TxReplicator struct {
	db   database.DB
	opts *Options

	logger logger.Logger

	context    context.Context
	cancelFunc context.CancelFunc

	streamSrvFactory stream.ServiceFactory
	client           client.ImmuClient

	delayer        Delayer
	failedAttempts int

	nextTx uint64

	running bool

	mutex sync.Mutex
}

func NewTxReplicator(db database.DB, opts *Options, logger logger.Logger) (*TxReplicator, error) {
	if db == nil || opts == nil || !opts.Valid() {
		return nil, ErrIllegalArguments
	}

	return &TxReplicator{
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

	srcDB := fullAddress(txr.opts.srcDatabase, txr.opts.srcAddress, txr.opts.srcPort)

	txr.logger.Infof("Initializing replication from '%s' to '%s'...", srcDB, txr.db.GetName())

	st, err := txr.db.CurrentState()
	if err != nil {
		return err
	}

	txr.context, txr.cancelFunc = context.WithCancel(context.Background())

	txr.nextTx = st.TxId + 1

	txr.running = true

	go func() {
		for {
			if txr.client == nil {
				err = txr.connect()
				if err == nil {
					txr.failedAttempts = 0
					continue
				}

				txr.failedAttempts++

				txr.logger.Warningf("Failed to connect with '%s' (%d failed attempts). Reason: %v", srcDB, txr.failedAttempts, err)

				timer := time.NewTimer(txr.delayer.DelayAfter(txr.failedAttempts))
				select {
				case <-txr.context.Done():
					timer.Stop()
					return
				case <-timer.C:
					break
				}
			} else {
				txr.logger.Infof("Replicating transaction %d from '%s' to '%s'...", txr.nextTx, srcDB, txr.db.GetName())

				bs, err := txr.fetchTX()
				if err != nil {
					txr.logger.Warningf("Failed to export transaction from '%s'. Reason: %v", srcDB, err)

					txr.failedAttempts++

					if txr.failedAttempts == 3 {
						txr.disconnect()
						continue
					}

					timer := time.NewTimer(txr.delayer.DelayAfter(txr.failedAttempts))
					select {
					case <-txr.context.Done():
						timer.Stop()
						return
					case <-timer.C:
						break
					}

					continue
				}

				_, err = txr.db.ReplicateTx(bs)
				if err != nil {
					txr.logger.Warningf("Failed to replicate transaction in database '%s' from '%s'. Reason: %v", txr.db.GetName(), srcDB, err)
					continue
				}

				txr.logger.Infof("Transaction %d successfully replicated from '%s' to '%s'", txr.nextTx, srcDB, txr.db.GetName())

				txr.nextTx++
				txr.failedAttempts = 0
			}
		}
	}()

	txr.logger.Infof("Replication succesfully initialized from '%s' to '%s'...", srcDB, txr.db.GetName())

	return nil
}

func fullAddress(db, address string, port int) string {
	return fmt.Sprintf("%s@%s:%d", db, address, port)
}

func (txr *TxReplicator) connect() error {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	txr.logger.Infof("Connecting to '%s':'%d'...", txr.opts.srcAddress, txr.opts.srcPort)

	opts := client.DefaultOptions().WithAddress(txr.opts.srcAddress).WithPort(txr.opts.srcPort)
	client, err := client.NewImmuClient(opts)
	if err != nil {
		return err
	}

	login, err := client.Login(txr.context, []byte(txr.opts.followerUsr), []byte(txr.opts.followerPwd))
	if err != nil {
		return err
	}

	txr.context = metadata.NewOutgoingContext(txr.context, metadata.Pairs("authorization", login.GetToken()))

	udr, err := client.UseDatabase(txr.context, &schema.Database{DatabaseName: txr.opts.srcDatabase})
	if err != nil {
		return err
	}

	txr.context = metadata.NewOutgoingContext(txr.context, metadata.Pairs("authorization", udr.GetToken()))

	txr.client = client

	txr.logger.Infof("Connection to '%s':'%d' successfully stablished", txr.opts.srcAddress, txr.opts.srcPort)

	return nil
}

func (txr *TxReplicator) disconnect() {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	txr.logger.Infof("Disconnecting from '%s':'%d'...", txr.opts.srcAddress, txr.opts.srcPort)

	txr.client.Logout(txr.context)
	txr.client.Disconnect()

	txr.client = nil

	txr.logger.Infof("Disconnected from '%s':'%d'...", txr.opts.srcAddress, txr.opts.srcPort)
}

func (txr *TxReplicator) fetchTX() ([]byte, error) {
	exportTxStream, err := txr.client.ExportTx(txr.context, &schema.TxRequest{Tx: txr.nextTx})
	if err != nil {
		return nil, err
	}

	receiver := txr.streamSrvFactory.NewMsgReceiver(exportTxStream)
	return receiver.ReadFully()
}

func (txr *TxReplicator) Stop() error {
	txr.mutex.Lock()
	defer txr.mutex.Unlock()

	txr.logger.Infof("Stopping replication for '%s'...", txr.db.GetName())

	if !txr.running {
		return ErrAlreadyStopped
	}

	if txr.client != nil {
		err := txr.client.Logout(txr.context)
		if err != nil {
			txr.logger.Warningf("Error login out from '%s:%d'. Reason: %v", txr.opts.srcAddress, txr.opts.srcPort, err)
		}
	}

	if txr.cancelFunc != nil {
		txr.cancelFunc()
	}

	if txr.client != nil {
		err := txr.client.Disconnect()
		if err != nil {
			txr.logger.Warningf("Error disconnecting from '%s:%d'. Reason: %v", txr.opts.srcAddress, txr.opts.srcPort, err)
		}
	}

	txr.running = false

	txr.logger.Infof("Replication successfully stopped for '%s'", txr.db.GetName())

	return nil
}
