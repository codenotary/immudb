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

package truncator

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/database"
)

var (
	ErrTruncatorAlreadyRunning = errors.New("truncator already running")
	ErrTruncatorAlreadyStopped = errors.New("truncator already stopped")
)

type Truncator struct {
	mu sync.Mutex

	truncators []database.Truncator // specifies truncators for multiple appendable logs

	hasStarted      bool
	truncationMutex sync.Mutex

	db database.DB

	retentionPeriod     time.Duration
	truncationFrequency time.Duration

	logger logger.Logger

	donech chan struct{}
	stopch chan struct{}
}

func NewTruncator(
	db database.DB,
	retentionPeriod time.Duration,
	truncationFrequency time.Duration,
	logger logger.Logger) *Truncator {

	return &Truncator{
		db:                  db,
		logger:              logger,
		truncators:          []database.Truncator{database.NewVlogTruncator(db, logger)},
		donech:              make(chan struct{}),
		stopch:              make(chan struct{}),
		retentionPeriod:     retentionPeriod,
		truncationFrequency: truncationFrequency,
	}
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
				err := t.Truncate(context.Background(), t.retentionPeriod)
				if err != nil {
					t.logger.Errorf("failed to truncate database '%s' {ts = %v}", t.db.GetName(), err)
				}
			}
		}
	}()

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
// Truncation discards an appendable log upto a given offset
// before time ts. First, the transaction is fetched which lies
// before the specified time period, and then the values are
// discarded upto the specified offset.
//
//	  discard point
//			|
//			|
//			v
//
// --------+-------+--------+----------
//
//		|       |        |
//	tn-1:vx  tn:vx   tn+1:vx
func (t *Truncator) Truncate(ctx context.Context, retentionPeriod time.Duration) error {
	t.truncationMutex.Lock()
	defer t.truncationMutex.Unlock()

	// The timestamp ts is used to determine which transaction onwards the data
	// may be deleted from the value-log.
	//
	// Subtracting a duration from ts will add a buffer for when transactions are
	// considered safe for deletion.

	// Truncate time to the beginning of the day.
	truncationTime := getTruncationTime(time.Now(), retentionPeriod)

	t.logger.Infof("start truncating database '%s' {ts = %v}", t.db.GetName(), truncationTime)

	for _, c := range t.truncators {
		// Plan determines the transaction header before time period ts. If a
		// transaction is not found, or if an error occurs fetching the transaction,
		// then truncation does not run for the specified appendable.
		hdr, err := c.Plan(ctx, truncationTime)
		if errors.Is(err, database.ErrRetentionPeriodNotReached) {
			t.logger.Infof("retention period not reached for truncating database '%s' at {ts = %v}", t.db.GetName(), truncationTime)
		}
		if errors.Is(err, store.ErrTxNotFound) {
			t.logger.Infof("no transaction found beyond specified truncation timeframe for database '%s' {reason = %v}", t.db.GetName(), err)
		}
		if err != nil {
			return err
		}

		// Truncate discards the appendable log upto the offset
		// specified in the transaction hdr
		err = c.TruncateUptoTx(ctx, hdr.Id)
		if err != nil {
			return err
		}
	}

	t.logger.Infof("finished truncating database '%s' {ts = %v}", t.db.GetName(), truncationTime)

	return nil
}

// getTruncationTime returns the timestamp that is used to determine
// which database transaction up to which the data may be deleted from the value-log.
func getTruncationTime(t time.Time, retentionPeriod time.Duration) time.Time {
	return truncateToDay(t.Add(-1 * retentionPeriod))
}

// TruncateToDay truncates the time to the beginning of the day.
func truncateToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
}
