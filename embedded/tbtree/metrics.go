package tbtree

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TODO: Those should be put behind abstract metrics system to avoid dependency on
//       prometheus inside embedded package
var metricsFlushingNodesProgress = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "immudb_btree_flushing_nodes_progress",
	Help: "Numbers of nodes written to disk during current flush process",
}, []string{"id"})

var metricsFlushingNodesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "immudb_btree_flushing_nodes_total",
	Help: "Number of nodes written to disk during all flush cycles",
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

var metricsBtreeInnerNodeEntries = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "immudb_btree_inner_node_entries",
	Help:    "Histogram of number of entries in as single inner btree node, calculated when visiting btree nodes",
	Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 40, 50, 75, 100, 200, 300, 500},
}, []string{"id"})

var metricsBtreeLeafNodeEntries = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "immudb_btree_leaf_node_entries",
	Help:    "Histogram of number of entries in as single leaf btree node, calculated when visiting btree nodes",
	Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30, 40, 50, 75, 100, 200, 300, 500},
}, []string{"id"})
