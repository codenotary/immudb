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
package tbtree

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TODO: Those should be put behind abstract metrics system to avoid dependency on
//
//	prometheus inside embedded package
var metricsFlushedNodesLastCycle = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "immudb_btree_flushed_nodes_last_cycle",
	Help: "Numbers of btree nodes written to disk during the last flush process",
}, []string{"id", "kind"})

var metricsFlushedNodesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "immudb_btree_flushed_nodes_total",
	Help: "Number of btree nodes written to disk during flush since the immudb process was started",
}, []string{"id", "kind"})

var metricsFlushedEntriesLastCycle = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "immudb_btree_flushed_entries_last_cycle",
	Help: "Numbers of btree entries written to disk during the last flush process",
}, []string{"id"})

var metricsFlushedEntriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "immudb_btree_flushed_entries_total",
	Help: "Number of btree entries written to disk during flush since the immudb process was started",
}, []string{"id"})

var metricsCompactedNodesLastCycle = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "immudb_btree_compacted_nodes_last_cycle",
	Help: "Numbers of btree nodes written to disk during the last compaction process",
}, []string{"id", "kind"})

var metricsCompactedNodesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "immudb_btree_compacted_nodes_total",
	Help: "Number of btree nodes written to disk during compaction since the immudb process was started",
}, []string{"id", "kind"})

var metricsCompactedEntriesLastCycle = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "immudb_btree_compacted_entries_last_cycle",
	Help: "Numbers of btree entries written to disk during the last compaction process",
}, []string{"id"})

var metricsCompactedEntriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "immudb_btree_compacted_entries_total",
	Help: "Number of btree entries written to disk during compaction since the immudb process was started",
}, []string{"id"})

var metricsCacheSizeStats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "immudb_btree_cache_size",
	Help: "number of entries in btree cache",
}, []string{"id"})

var metricsCacheHit = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "immudb_btree_cache_hit",
	Help: "Number of btree cache hits when getting btree node",
}, []string{"id"})

var metricsCacheMiss = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "immudb_btree_cache_miss",
	Help: "Number of btree cache misses when getting btree node",
}, []string{"id"})

var metricsCacheEvict = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "immudb_btree_cache_evict",
	Help: "Number of btree nodes evicted from cache",
}, []string{"id"})

var metricsBtreeDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "immudb_btree_depth",
	Help: "Btree depth",
}, []string{"id"})

var metricsBtreeNodeEntriesHistogramBuckets = []float64{
	1, 2, 3, 4, 5, 6, 7, 8, 9,
	10, 15, 20, 25, 30, 40, 50, 75,
	100, 200, 300, 400, 500, 600, 700, 800, 900,
	1000, 1200, 1400, 1600, 1800,
	2000, 2500, 3000, 3500, 4000,
}

var metricsBtreeInnerNodeEntries = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "immudb_btree_inner_node_entries",
	Help:    "Histogram of number of entries in as single inner btree node, calculated when visiting btree nodes",
	Buckets: metricsBtreeNodeEntriesHistogramBuckets,
}, []string{"id"})

var metricsBtreeLeafNodeEntries = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "immudb_btree_leaf_node_entries",
	Help:    "Histogram of number of entries in as single leaf btree node, calculated when visiting btree nodes",
	Buckets: metricsBtreeNodeEntriesHistogramBuckets,
}, []string{"id"})

var metricsBtreeNodesDataBeginOffset = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "immudb_btree_nodes_data_begin",
	Help: "Beginning offset for btree nodes data file",
}, []string{"id"})

var metricsBtreeNodesDataEndOffset = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "immudb_btree_nodes_data_end",
	Help: "End offset for btree nodes data appendable",
}, []string{"id"})
