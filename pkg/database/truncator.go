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

package database

import (
	"context"
	"errors"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ErrTruncatorAlreadyRunning   = errors.New("truncator already running")
	ErrRetentionPeriodNotReached = errors.New("retention period has not been reached")
)

// Truncator provides truncation against an underlying storage
// of appendable data.
type Truncator interface {
	// Plan returns the latest transaction upto which the log can be truncated.
	// When resulting transaction before specified time does not exists
	//  * No transaction header is returned.
	//  * Returns nil TxHeader, and an error.
	Plan(ctx context.Context, truncationUntil time.Time) (*schema.TxHeader, error)

	// TruncateUptoTx runs truncation against the relevant appendable logs. Must
	// be called after result of Plan().
	TruncateUptoTx(ctx context.Context, txID uint64) error
}

func NewVlogTruncator(db DB, log logger.Logger) Truncator {
	return &vlogTruncator{
		db:      db,
		metrics: newTruncatorMetrics(db.GetName()),
		logger:  log,
	}
}

// vlogTruncator implements Truncator for the value-log appendable
type vlogTruncator struct {
	db      DB
	metrics *truncatorMetrics
	logger  logger.Logger
}

// Plan returns the transaction upto which the value log can be truncated.
// When resulting transaction before specified time does not exists
//   - No transaction header is returned.
//   - Returns nil TxHeader, and an error.
func (v *vlogTruncator) Plan(ctx context.Context, truncationUntil time.Time) (*schema.TxHeader, error) {
	return v.db.FindTruncationPoint(ctx, truncationUntil)
}

// TruncateUpTo runs truncation against the relevant appendable logs upto the specified transaction offset.
func (v *vlogTruncator) TruncateUptoTx(ctx context.Context, txID uint64) error {
	defer func(t time.Time) {
		v.metrics.ran.Inc()
		v.metrics.duration.Observe(time.Since(t).Seconds())
	}(time.Now())

	v.logger.Infof("copying sql catalog before truncation for database '%s' at tx %d", v.db.GetName(), txID)

	// copy sql catalogue
	sqlCatalogTxID, err := v.db.CopySQLCatalog(ctx, txID)
	if err != nil {
		v.logger.Errorf("error during truncation for database '%s' {err = %v, id = %v, type=sql_catalogue_commit}", v.db.GetName(), err, txID)
		return err
	}
	v.logger.Infof("committed sql catalog before truncation for database '%s' at tx %d", v.db.GetName(), sqlCatalogTxID)

	// truncate upto txID
	err = v.db.TruncateUptoTx(ctx, txID)
	if err != nil {
		v.logger.Errorf("error during truncation for database '%s' {err = %v, id = %v, type=truncate_upto}", v.db.GetName(), err, txID)
	}
	return err
}

type truncatorMetrics struct {
	ran      prometheus.Counter
	duration prometheus.Observer
}

func newTruncatorMetrics(db string) *truncatorMetrics {
	reg := prometheus.NewRegistry()

	m := &truncatorMetrics{}
	m.ran = promauto.With(reg).NewCounterVec(
		prometheus.CounterOpts{
			Name: "immudb_truncation_total",
			Help: "Total number of truncation that were executed for the database.",
		},
		[]string{"db"},
	).WithLabelValues(db)

	m.duration = promauto.With(reg).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "immudb_truncation_duration_seconds",
			Help:    "Duration of truncation runs",
			Buckets: prometheus.ExponentialBuckets(1, 10.0, 16),
		},
		[]string{"db"},
	).WithLabelValues(db)

	return m
}
