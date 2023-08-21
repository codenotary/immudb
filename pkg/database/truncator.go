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

package database

import (
	"context"
	"errors"
	"time"

	"github.com/codenotary/immudb/embedded/store"
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
	Plan(ctx context.Context, truncationUntil time.Time) (*store.TxHeader, error)

	// TruncateUptoTx runs truncation against the relevant appendable logs. Must
	// be called after result of Plan().
	TruncateUptoTx(ctx context.Context, txID uint64) error
}

func NewVlogTruncator(d DB) Truncator {
	return &vlogTruncator{
		db:      d.(*db),
		metrics: newTruncatorMetrics(d.GetName()),
	}
}

// vlogTruncator implements Truncator for the value-log appendable
type vlogTruncator struct {
	db      *db
	metrics *truncatorMetrics
}

// Plan returns the transaction upto which the value log can be truncated.
// When resulting transaction before specified time does not exists
//   - No transaction header is returned.
//   - Returns nil TxHeader, and an error.
func (v *vlogTruncator) Plan(ctx context.Context, truncationUntil time.Time) (*store.TxHeader, error) {
	hdr, err := v.db.st.LastTxUntil(truncationUntil)
	if errors.Is(err, store.ErrTxNotFound) {
		return nil, ErrRetentionPeriodNotReached
	}
	if err != nil {
		return nil, err
	}

	// look for the newst transaction with entries
	for err == nil {
		if hdr.NEntries > 0 {
			break
		}

		if ctx.Err() != nil {
			return nil, err
		}

		hdr, err = v.db.st.ReadTxHeader(hdr.ID-1, false, false)
	}

	return hdr, err
}

// commitCatalog commits the current sql catalogue as a new transaction.
func (v *vlogTruncator) commitCatalog(ctx context.Context, txID uint64) (*store.TxHeader, error) {
	// copy sql catalogue
	tx, err := v.db.st.NewTx(ctx, store.DefaultTxOptions())
	if err != nil {
		return nil, err
	}

	err = v.db.CopyCatalogToTx(ctx, tx)
	if err != nil {
		v.db.Logger.Errorf("error during truncation for database '%s' {err = %v, id = %v, type=sql_catalogue_copy}", v.db.name, err, txID)
		return nil, err
	}
	defer tx.Cancel()

	// setting the metadata to record the transaction upto which the log was truncated
	tx.WithMetadata(store.NewTxMetadata().WithTruncatedTxID(txID))

	tx.RequireMVCCOnFollowingTxs(true)

	// commit catalogue as a new transaction
	return tx.Commit(ctx)
}

// TruncateUpTo runs truncation against the relevant appendable logs upto the specified transaction offset.
func (v *vlogTruncator) TruncateUptoTx(ctx context.Context, txID uint64) error {
	defer func(t time.Time) {
		v.metrics.ran.Inc()
		v.metrics.duration.Observe(time.Since(t).Seconds())
	}(time.Now())

	v.db.Logger.Infof("copying sql catalog before truncation for database '%s' at tx %d", v.db.name, txID)

	// copy sql catalogue
	sqlCommitHdr, err := v.commitCatalog(ctx, txID)
	if err != nil {
		v.db.Logger.Errorf("error during truncation for database '%s' {err = %v, id = %v, type=sql_catalogue_commit}", v.db.name, err, txID)
		return err
	}
	v.db.Logger.Infof("committed sql catalog before truncation for database '%s' at tx %d", v.db.name, sqlCommitHdr.ID)

	// truncate upto txID
	err = v.db.st.TruncateUptoTx(txID)
	if err != nil {
		v.db.Logger.Errorf("error during truncation for database '%s' {err = %v, id = %v, type=truncate_upto}", v.db.name, err, txID)
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
