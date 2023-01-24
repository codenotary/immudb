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

package truncator

import (
	"errors"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
)

var (
	ErrTruncatorAlreadyRunning = errors.New("truncator already running")
	ErrTruncatorAlreadyStopped = errors.New("truncator already stopped")
)

func NewTruncator(
	db database.DB,
	retentionPeriod time.Duration,
	truncationFrequency time.Duration,
	logger logger.Logger) *Truncator {
	t := &Truncator{
		db:                  db,
		logger:              logger,
		retentionPeriodF:    getRetentionPeriod,
		truncators:          make([]database.Truncator, 0),
		donech:              make(chan struct{}),
		stopch:              make(chan struct{}),
		retentionPeriod:     retentionPeriod,
		truncationFrequency: truncationFrequency,
	}
	t.truncators = append(t.truncators, database.NewVlogTruncator(db))
	return t
}

type Truncator struct {
	mu sync.Mutex

	truncators []database.Truncator // specifies truncators for multiple appendable logs

	hasStarted bool

	db database.DB

	retentionPeriod     time.Duration
	truncationFrequency time.Duration

	logger           logger.Logger
	retentionPeriodF func(ts time.Time, retentionPeriod time.Duration) time.Time

	donech chan struct{}
	stopch chan struct{}
}

// runTruncator triggers periodically to truncate multiple appendable logs
func (t *Truncator) Start() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.hasStarted {
		return ErrTruncatorAlreadyRunning
	}

	t.hasStarted = true
	t.logger.Infof("starting truncator for database '%s' with retention period '%vs' and truncation frequency '%vs'", t.db.GetName(), t.retentionPeriod.Seconds(), t.truncationFrequency.Seconds())

	go func() {
		ticker := time.NewTicker(t.truncationFrequency)
		for {
			select {
			case <-t.stopch:
				ticker.Stop()
				t.donech <- struct{}{}
				return
			case <-ticker.C:
				// The timestamp ts is used to determine which transaction onwards the data
				// may be deleted from the value-log.
				//
				// Subtracting a duration from ts will add a buffer for when transactions are
				// considered safe for deletion.

				// Truncate time to the beginning of the day.
				ts := t.retentionPeriodF(time.Now(), t.retentionPeriod)
				t.logger.Infof("start truncating database '%s' {ts = %v}", t.db.GetName(), ts)
				if err := t.truncate(ts); err != nil {
					if errors.Is(err, database.ErrRetentionPeriodNotReached) {
						t.logger.Infof("retention period not reached for truncating database '%s' at {ts = %s}", t.db.GetName(), ts.String())
					} else if errors.Is(err, store.ErrTxNotFound) {
						t.logger.Infof("no transaction found beyond specified truncation timeframe for database '%s' {err = %v}", t.db.GetName(), err)
					} else {
						t.logger.Errorf("failed to truncate database '%s' {ts = %v}", t.db.GetName(), err)
					}
				} else {
					t.logger.Infof("finished truncating database '%s' {ts = %v}", t.db.GetName(), ts)
				}
			}
		}
	}()
	return nil
}

//	truncate discards an appendable log upto a given offset
//	before time ts. First, the transaction is fetched which lies
//	before the specified time period, and then the values are
//	discarded upto the specified offset.
//
//			  discard point
//					|
//					|
//					v
//	--------+-------+--------+----------
//			|       |        |
//		tn-1:vx  tn:vx   tn+1:vx
//
func (t *Truncator) truncate(ts time.Time) error {
	for _, c := range t.truncators {
		// Plan determines the transaction header before time period ts. If a
		// transaction is not found, or if an error occurs fetching the transaction,
		// then truncation does not run for the specified appendable.
		hdr, err := c.Plan(ts)
		if err != nil {
			if err != database.ErrRetentionPeriodNotReached && err != store.ErrTxNotFound {
				t.logger.Errorf("failed to plan truncation for database '%s' {err = %v}", t.db.GetName(), err)
			}
			// If no transaction is found, or if an error occurs, then continue
			return err
		}

		// Truncate discards the appendable log upto the offset
		// specified in the transaction hdr
		err = c.Truncate(hdr.ID)
		if err != nil {
			t.logger.Errorf("failed to truncate database '%s' {err = %v}", t.db.GetName(), err)
			return err
		}
	}

	return nil
}

func (t *Truncator) Stop() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.hasStarted {
		return ErrTruncatorAlreadyStopped
	}

	t.logger.Infof("Stopping truncator of database '%s'...", t.db.GetName())
	t.stopch <- struct{}{}
	<-t.donech
	t.hasStarted = false
	t.logger.Infof("Truncator for database '%s' successfully stopped", t.db.GetName())

	return nil
}

// Truncate discards all data from the database that is older than the retention period.
func (t *Truncator) Truncate(retentionPeriod time.Duration) error {
	ts := t.retentionPeriodF(time.Now(), retentionPeriod)
	t.logger.Infof("start truncating database '%s' {ts = %v}", t.db.GetName(), ts)
	if err := t.truncate(ts); err != nil {
		if errors.Is(err, database.ErrRetentionPeriodNotReached) {
			t.logger.Infof("retention period not reached for truncating database '%s' at {ts = %s}", t.db.GetName(), ts.String())
		} else if errors.Is(err, store.ErrTxNotFound) {
			t.logger.Infof("no transaction found beyond specified truncation timeframe for database '%s' {reason = %v}", t.db.GetName(), err)
		} else {
			t.logger.Errorf("failed truncating database '%s' {ts = %v}", t.db.GetName(), err)
		}
		return err
	}

	t.logger.Infof("finished truncating database '%s' {ts = %v}", t.db.GetName(), ts)
	return nil
}

// getRetentionPeriod returns the timestamp that is used to determine
// which database.transaction up to which the data may be deleted from the value-log.
func getRetentionPeriod(ts time.Time, retentionPeriod time.Duration) time.Time {
	return database.TruncateToDay(ts.Add(-1 * retentionPeriod))
}
