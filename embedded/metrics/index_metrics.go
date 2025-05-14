/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type IndexMetrics interface {
	SetPagesFlushedLastCycle(n int)
	SetDepth(depth int)
	SetTotalPages(n int)
	SetStalePages(n int)
	SetTs(ts uint64)
	IncIndexedEntriesTotal()
	NewFlushProgressTracker(maxValue float64, ts uint64) ProgressTracker
}

var (
	metricsPagesFlushedLastCycle = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_btree_pages_flushed_last_cycle",
		Help: "Numbers of btree pages written to disk during the last flush process",
	}, []string{"index_id"})

	metricsBtreeDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_btree_depth",
		Help: "Btree depth",
	}, []string{"index_id"})

	metricsBTreeTotalPages = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_btree_total_pages",
		Help: "Total number of pages in the tree",
	}, []string{"index_id"})

	metricsBTreeStalePages = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_btree_stale_pages",
		Help: "Total number of stale pages in the tree",
	}, []string{"index_id"})

	metricsTs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_btree_ts",
		Help: "Highest timestamp of an entry stored in the tree",
	}, []string{"index_id"})

	metricsIndexedEntries = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_btree_indexed_entries",
		Help: "Total number of entries stored in the tree",
	}, []string{"index_id"})

	metricsIndexLastFlushProgress = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_btree_flush_progress",
		Help: "Progress of last flush operation",
	}, []string{"index_id"})
)

var _ IndexMetrics = &prometheusIndexMetrics{}

type prometheusIndexMetrics struct {
	index string
}

func NewPrometheusIndexMetrics(index string) IndexMetrics {
	return &prometheusIndexMetrics{
		index: index,
	}
}

func (m *prometheusIndexMetrics) SetPagesFlushedLastCycle(n int) {
	metricsPagesFlushedLastCycle.WithLabelValues(m.index).Set(float64(n))
}

func (m *prometheusIndexMetrics) SetDepth(depth int) {
	metricsBtreeDepth.WithLabelValues(m.index).Set(float64(depth))
}

func (m *prometheusIndexMetrics) SetTotalPages(n int) {
	metricsBTreeTotalPages.WithLabelValues(m.index).Set(float64(n))
}

func (m *prometheusIndexMetrics) SetStalePages(n int) {
	metricsBTreeStalePages.WithLabelValues(m.index).Set(float64(n))
}

func (m *prometheusIndexMetrics) SetTs(ts uint64) {
	metricsTs.WithLabelValues(m.index).Set(float64(ts))
}

func (m *prometheusIndexMetrics) IncIndexedEntriesTotal() {
	metricsIndexedEntries.WithLabelValues(m.index).Inc()
}

func (m *prometheusIndexMetrics) NewFlushProgressTracker(maxValue float64, ts uint64) ProgressTracker {
	progress := metricsIndexLastFlushProgress.WithLabelValues(m.index)

	return NewPrometheusProgressTracker(maxValue, progress)
}
