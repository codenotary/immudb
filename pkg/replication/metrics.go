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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TODO: Metrics should be put behind abstract metrics interfaces to avoid direct dependency on prometheus SDK

var (
	_metricsTxWaitQueueHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "immudb_replication_tx_wait_queue",
		Buckets: prometheus.ExponentialBucketsRange(0.001, 10.0, 16),
		Help:    "histogram of time spent in the waiting queue before replicator picks up the transaction",
	}, []string{"db"})

	_metricsReplicationTimeHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "immudb_replication_commit_time",
		Buckets: prometheus.ExponentialBucketsRange(0.001, 10.0, 16),
		Help:    "histogram of time spent by replicators to replicate a single transaction",
	}, []string{"db"})

	_metricsReplicators = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_replication_replicators",
		Help: "number of replicators available",
	}, []string{"db"})

	_metricsReplicatorsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_replication_replicators_active",
		Help: "number of replicators actively processing transactions",
	}, []string{"db"})

	_metricsReplicatorsInRetryDelay = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_replication_replicators_retry_delay",
		Help: "number of replicators that are currently delaying the replication due to a replication error",
	}, []string{"db"})

	_metricsReplicationRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "immudb_replication_replicators_retries",
		Help: "number of retries while replicating transactions caused by errors",
	}, []string{"db"})

	_metricsReplicationPrimaryCommittedTxID = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_replication_primary_committed_tx_id",
		Help: "the latest know transaction ID committed on the primary node",
	}, []string{"db"})

	_metricsAllowCommitUpToTxID = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_replication_allow_commit_up_to_tx_id",
		Help: "most recently received confirmation up to which commit id the replica is allowed to durably commit",
	}, []string{"db"})

	_metricsReplicationLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_replication_lag",
		Help: "The difference between the last transaction committed by the primary and replicated by the replica",
	}, []string{"db"})
)

type metrics struct {
	txWaitQueueHistogram     prometheus.Observer
	replicationTimeHistogram prometheus.Observer
	replicationRetries       prometheus.Counter
	replicators              prometheus.Gauge
	replicatorsActive        prometheus.Gauge
	replicatorsInRetryDelay  prometheus.Gauge
	primaryCommittedTxID     prometheus.Gauge
	allowCommitUpToTxID      prometheus.Gauge
	replicationLag           prometheus.Gauge
}

// metricsForDb returns metrics object for particular database name
func metricsForDb(dbName string) metrics {
	return metrics{
		txWaitQueueHistogram:     _metricsTxWaitQueueHistogram.WithLabelValues(dbName),
		replicationTimeHistogram: _metricsReplicationTimeHistogram.WithLabelValues(dbName),
		replicationRetries:       _metricsReplicationRetries.WithLabelValues(dbName),
		replicators:              _metricsReplicators.WithLabelValues(dbName),
		replicatorsActive:        _metricsReplicatorsActive.WithLabelValues(dbName),
		replicatorsInRetryDelay:  _metricsReplicatorsInRetryDelay.WithLabelValues(dbName),
		primaryCommittedTxID:     _metricsReplicationPrimaryCommittedTxID.WithLabelValues(dbName),
		allowCommitUpToTxID:      _metricsAllowCommitUpToTxID.WithLabelValues(dbName),
		replicationLag:           _metricsReplicationLag.WithLabelValues(dbName),
	}
}

// reset ensures all necessary gauges are zeroed
func (m *metrics) reset() {
	m.replicators.Set(0)
	m.replicatorsActive.Set(0)
	m.replicatorsInRetryDelay.Set(0)
	m.primaryCommittedTxID.Set(0)
	m.allowCommitUpToTxID.Set(0)
	m.replicationLag.Set(0)
}

// replicationTimeHistogramTimer returns prometheus timer for replicationTimeHistogram
func (m *metrics) replicationTimeHistogramTimer() *prometheus.Timer {
	return prometheus.NewTimer(m.replicationTimeHistogram)
}
