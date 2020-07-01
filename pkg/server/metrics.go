/*
Copyright 2019-2020 vChain, Inc.

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

package server

import (
	"context"
	"expvar"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/peer"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsCollection immudb Prometheus metrics collection
type MetricsCollection struct {
	RecordsCounter               prometheus.CounterFunc
	UptimeCounter                prometheus.CounterFunc
	RPCsPerClientCounters        *prometheus.CounterVec
	LastMessageAtPerClientGauges *prometheus.GaugeVec
}

var metricsNamespace = "immudb"

// WithRecordsCounter ...
func (mc *MetricsCollection) WithRecordsCounter(f func() float64) {
	mc.RecordsCounter = promauto.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "number_of_stored_entries",
			Help:      "Number of key-value entries currently stored by the database.",
		},
		f,
	)
}

// WithUptimeCounter ...
func (mc *MetricsCollection) WithUptimeCounter(f func() float64) {
	mc.UptimeCounter = promauto.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "uptime_hours",
			Help:      "Server uptime in hours.",
		},
		f,
	)
}

// UpdateClientMetrics ...
func (mc *MetricsCollection) UpdateClientMetrics(ctx context.Context) {
	p, ok := peer.FromContext(ctx)
	if ok && p != nil {
		ipAndPort := strings.Split(p.Addr.String(), ":")
		if len(ipAndPort) > 0 {
			mc.RPCsPerClientCounters.WithLabelValues(ipAndPort[0]).Inc()
			mc.LastMessageAtPerClientGauges.WithLabelValues(ipAndPort[0]).SetToCurrentTime()
		}
	}
}

// Metrics immudb Prometheus metrics collection
var Metrics = MetricsCollection{
	RPCsPerClientCounters: promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "number_of_rpcs_per_client",
			Help:      "Number of handled RPCs per client.",
		},
		[]string{"ip"},
	),
	LastMessageAtPerClientGauges: promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "clients_last_message_at_unix_seconds",
			Help:      "Timestamp at which clients have sent their most recent message.",
		},
		[]string{"ip"},
	),
}

func init() {
	http.Handle("/metrics", promhttp.Handler())
	expvarCollector := prometheus.NewExpvarCollector(map[string]*prometheus.Desc{
		"badger_blocked_puts_total":   prometheus.NewDesc("immudb_blocked_puts_total", "Blocked Puts", nil, nil),
		"badger_disk_reads_total":     prometheus.NewDesc("immudb_disk_reads_total", "Disk Reads", nil, nil),
		"badger_disk_writes_total":    prometheus.NewDesc("immudb_disk_writes_total", "Disk Writes", nil, nil),
		"badger_gets_total":           prometheus.NewDesc("immudb_gets_total", "Gets", nil, nil),
		"badger_puts_total":           prometheus.NewDesc("immudb_puts_total", "Puts", nil, nil),
		"badger_memtable_gets_total":  prometheus.NewDesc("immudb_memtable_gets_total", "Memtable gets", nil, nil),
		"badger_lsm_size_bytes":       prometheus.NewDesc("immudb_lsm_size_bytes", "LSM Size in bytes", []string{"database"}, nil),
		"badger_vlog_size_bytes":      prometheus.NewDesc("immudb_vlog_size_bytes", "Value Log Size in bytes", []string{"database"}, nil),
		"badger_pending_writes_total": prometheus.NewDesc("immudb_pending_writes_total", "Pending Writes", []string{"database"}, nil),
		"badger_read_bytes":           prometheus.NewDesc("immudb_read_bytes", "Read bytes", nil, nil),
		"badger_written_bytes":        prometheus.NewDesc("immudb_written_bytes", "Written bytes", nil, nil),
		"badger_lsm_bloom_hits_total": prometheus.NewDesc("immudb_lsm_bloom_hits_total", "LSM Bloom Hits", []string{"level"}, nil),
		"badger_lsm_level_gets_total": prometheus.NewDesc("immudb_lsm_level_gets_total", "LSM Level Gets", []string{"level"}, nil),
	})
	prometheus.MustRegister(expvarCollector)
}

// StartMetrics listens and servers the HTTP metrics server in a new goroutine.
// The server is then returned and can be stopped using Close().
func StartMetrics(
	addr string,
	l logger.Logger,
	recordsCounter func() float64,
	uptimeCounter func() float64,
) *http.Server {
	Metrics.WithRecordsCounter(recordsCounter)
	Metrics.WithUptimeCounter(uptimeCounter)
	// expvar package adds a handler in to the default HTTP server (which has to be started explicitly),
	// and serves up the metrics at the /debug/vars endpoint.
	// Here we're registering both expvar and promhttp handlers in our custom server.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/debug/vars", expvar.Handler())
	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				l.Debugf("Metrics http server closed")
			} else {
				l.Errorf("Metrics error: %s", err)
			}

		}
	}()

	return server
}
