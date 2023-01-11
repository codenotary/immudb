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
	// ErrNoTxBeforeTime is returned when retention period is not reached.
	ErrRetentionPeriodNotReached = errors.New("retention period not reached")
)

// Truncator provides truncation against an underlying storage
// of appendable data.
type Truncator interface {
	// Plan returns the transaction upto which the log can be truncated.
	// When resulting transaction before specified time does not exists
	//  * No transaction header is returned.
	//  * Returns nil TxHeader, and an error.
	Plan(time.Time) (*store.TxHeader, error)

	// Truncate runs truncation against the relevant appendable logs. Must
	// be called after result of Plan().
	Truncate(*store.TxHeader) error
}

func newVlogTruncator(d *db) Truncator {
	return &vlogTruncator{
		db:      d,
		metrics: newTruncatorMetrics(d.name),
	}
}

// vlogTruncator implements Truncator for the value-log appendable
type vlogTruncator struct {
	db      *db
	metrics *truncatorMetrics
}

// Plan returns the transaction upto which the value log can be truncated.
// When resulting transaction before specified time does not exists
//  * No transaction header is returned.
//  * Returns nil TxHeader, and an error.
// The retentionPeriod time is truncated to the day.
func (v *vlogTruncator) Plan(retentionPeriod time.Time) (*store.TxHeader, error) {
	hdr, err := v.db.st.FirstTxSince(retentionPeriod)
	if err != nil {
		return nil, err
	}

	// if the transaction is on or before the retention period, then we can truncate upto this
	// transaction otherwise, we cannot truncate since the retention period has not been reached
	// and truncation would otherwise add an extra transaction to the log for sql catalogue.
	err = v.isRetentionPeriodReached(retentionPeriod, time.Unix(hdr.Ts, 0))
	if err != nil {
		return nil, err
	}
	return hdr, nil
}

// isRetentionPeriodReached returns an error if the retention period has not been reached.
func (v *vlogTruncator) isRetentionPeriodReached(retentionPeriod time.Time, txTimestamp time.Time) error {
	txTime := truncateToDay(txTimestamp)
	if txTime.Unix() <= retentionPeriod.Unix() {
		return nil
	}
	return ErrRetentionPeriodNotReached
}

// commitCatalog commits the current sql catalogue as a new transaction.
func (v *vlogTruncator) commitCatalog(hdr *store.TxHeader) (*store.TxHeader, error) {
	// copy sql catalogue
	tx, err := v.db.CopyCatalog(context.Background())
	if err != nil {
		v.db.Logger.Errorf("error during truncation for database '%s' {err = %v, id = %v, type=sql_catalogue_copy}", v.db.name, err, hdr.ID)
		return nil, err
	}
	defer tx.Cancel()

	// setting the metadata to record the transaction upto which the log was truncated
	tx.WithMetadata(store.NewTxMetadata().WithTruncatedTxID(hdr.ID))

	// commit catalogue as a new transaction
	return tx.Commit()
}

// Truncate runs truncation against the relevant appendable logs upto the specified transaction offset.
func (v *vlogTruncator) Truncate(hdr *store.TxHeader) error {
	defer func(t time.Time) {
		v.metrics.ran.Inc()
		v.metrics.duration.Observe(time.Since(t).Seconds())
	}(time.Now())
	v.db.Logger.Infof("copying sql catalog before truncation for database '%s' at tx %d", v.db.name, hdr.ID)
	// copy sql catalogue
	sqlCommitHdr, err := v.commitCatalog(hdr)
	if err != nil {
		v.db.Logger.Errorf("error during truncation for database '%s' {err = %v, id = %v, type=sql_catalogue_commit}", v.db.name, err, hdr.ID)
		return err
	}
	v.db.Logger.Infof("committed sql catalog before truncation for database '%s' at tx %d", v.db.name, sqlCommitHdr.ID)

	// truncate upto hdr.ID
	err = v.db.st.TruncateUptoTx(hdr.ID)
	if err != nil {
		v.db.Logger.Errorf("error during truncation for database '%s' {err = %v, id = %v, type=truncate_upto}", v.db.name, err, hdr.ID)
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
