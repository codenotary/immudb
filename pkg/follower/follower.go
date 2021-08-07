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

package follower

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
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

type expBackoff struct {
	retryMinDelay time.Duration
	retryMaxDelay time.Duration
	retryDelayExp float64
	retryJitter   float64
}

func (exp *expBackoff) delayAfter(retries int) time.Duration {
	return time.Duration(
		math.Min(
			float64(exp.retryMinDelay)*math.Pow(exp.retryDelayExp, float64(retries)),
			float64(exp.retryMaxDelay),
		) * (1.0 - rand.Float64()*exp.retryJitter),
	)
}

type Follower struct {
	db   database.DB
	opts *Options

	logger logger.Logger

	streamSrvFactory stream.ServiceFactory
	client           client.ImmuClient

	context    context.Context
	cancelFunc context.CancelFunc

	delayer        *expBackoff
	failedAttempts int

	currTx uint64

	running bool

	mutex sync.Mutex
}

func NewFollower(db database.DB, opts *Options, logger logger.Logger) (*Follower, error) {
	if db == nil || opts == nil || !opts.Valid() {
		return nil, ErrIllegalArguments
	}

	delayer := &expBackoff{
		retryMinDelay: opts.retryMinDelay,
		retryMaxDelay: opts.retryMaxDelay,
		retryDelayExp: opts.retryDelayExp,
		retryJitter:   opts.retryDelayJitter,
	}

	return &Follower{
		db:               db,
		opts:             opts,
		logger:           logger,
		streamSrvFactory: stream.NewStreamServiceFactory(opts.streamChunkSize),
		delayer:          delayer,
	}, nil
}

func (f *Follower) Start() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.running {
		return ErrAlreadyRunning
	}

	f.logger.Infof("Following '%s' for replication to '%s'...", fullAddress(f.opts.srcDatabase, f.opts.srcAddress, f.opts.srcPort), f.db.GetName())

	f.context, f.cancelFunc = context.WithCancel(context.Background())

	st, err := f.db.CurrentState()
	if err != nil {
		return err
	}

	f.currTx = st.TxId + 1

	f.running = true

	go func() {
		for {
			if f.client == nil {
				err = f.connect()
				if err != nil {
					f.logger.Warningf("Failed to connect with master. Reason:", err)

					f.failedAttempts++

					timer := time.NewTimer(f.delayer.delayAfter(f.failedAttempts))
					select {
					case <-f.context.Done():
						timer.Stop()
					case <-timer.C:
						return
					}

					continue
				}

				f.failedAttempts = 0
			} else {
				srcDB := fullAddress(f.opts.srcDatabase, f.opts.srcAddress, f.opts.srcPort)

				f.logger.Infof("Replicating transaction %d from '%s' to '%s'...", f.currTx, srcDB, f.db.GetName())

				bs, err := f.fetchTX()
				if err != nil {
					f.logger.Warningf("Failed to export transaction for database '%s'. Reason:", f.db.GetName(), err)

					f.failedAttempts++

					timer := time.NewTimer(f.delayer.delayAfter(f.failedAttempts))
					select {
					case <-f.context.Done():
						timer.Stop()
					case <-timer.C:
						return
					}

					continue
				}

				_, err = f.db.ReplicateTx(bs)
				if err != nil {
					f.logger.Warningf("Failed to replicate transaction in database '%s'. Reason:", f.db.GetName(), err)
					continue
				}

				f.logger.Infof("Transaction %d successfully replicated from '%s' to '%s'", f.currTx, srcDB, f.db.GetName())

				f.currTx++
				f.failedAttempts = 0
			}
		}
	}()

	return nil
}

func fullAddress(db, address string, port int) string {
	return fmt.Sprintf("%s@%s:%d", db, address, port)
}

func (f *Follower) connect() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	opts := client.DefaultOptions().WithAddress(f.opts.srcAddress).WithPort(f.opts.srcPort)
	client, err := client.NewImmuClient(opts)
	if err != nil {
		return err
	}

	login, err := client.Login(f.context, []byte(f.opts.followerUsr), []byte(f.opts.followerPwd))
	if err != nil {
		return err
	}

	f.context = metadata.NewOutgoingContext(f.context, metadata.Pairs("authorization", login.GetToken()))

	udr, err := client.UseDatabase(f.context, &schema.Database{DatabaseName: f.opts.srcDatabase})
	if err != nil {
		return err
	}

	f.context = metadata.NewOutgoingContext(f.context, metadata.Pairs("authorization", udr.GetToken()))

	f.client = client

	return nil
}

func (f *Follower) fetchTX() ([]byte, error) {
	exportTxStream, err := f.client.ExportTx(f.context, &schema.TxRequest{Tx: f.currTx})
	if err != nil {
		return nil, err
	}

	receiver := f.streamSrvFactory.NewMsgReceiver(exportTxStream)

	return receiver.ReadFully()
}

func (f *Follower) Stop() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if !f.running {
		return ErrAlreadyStopped
	}

	f.cancelFunc()

	f.running = false

	err := f.client.Logout(f.context)
	if err != nil {
		return err
	}

	f.logger.Infof("Stopped following database '%s'", f.db.GetName())

	return nil
}
