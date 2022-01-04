/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"encoding/json"
	"expvar"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/peer"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Version VersionResponse

// VersionResponse ...
type VersionResponse struct {
	Component string `json:"component" example:"immudb"`
	Version   string `json:"version" example:"1.0.1-c9c6495"`
	BuildTime string `json:"buildtime" example:"1604692129"`
	BuiltBy   string `json:"builtby,omitempty"`
	Static    bool   `json:"static"`
}

// MetricsCollection immudb Prometheus metrics collection
type MetricsCollection struct {
	registry *prometheus.Registry

	UptimeCounter prometheus.CounterFunc

	computeDBSizes func() map[string]float64
	DBSizeGauges   *prometheus.GaugeVec

	computeDBEntries func() map[string]float64
	DBEntriesGauges  *prometheus.GaugeVec

	RPCsPerClientCounters        *prometheus.CounterVec
	LastMessageAtPerClientGauges *prometheus.GaugeVec

	lastIndexedTrx   *prometheus.GaugeVec
	lastCommittedTrx *prometheus.GaugeVec

	// ---- Multiapp stats ------------------------------------

	multiappCacheEvents *prometheus.CounterVec
	multiappReadEvents  *prometheus.CounterVec
	metricsReadBytes    prometheus.Counter
}

var metricsNamespace = "immudb"

// WithUptimeCounter ...
func (mc *MetricsCollection) WithUptimeCounter(f func() float64) {
	promauto := promauto.With(mc.registry)
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

// WithComputeDBSizes ...
func (mc *MetricsCollection) WithComputeDBSizes(f func() map[string]float64) {
	mc.computeDBSizes = f
}

// WithComputeDBEntries ...
func (mc *MetricsCollection) WithComputeDBEntries(f func() map[string]float64) {
	mc.computeDBEntries = f
}

// UpdateDBMetrics ...
func (mc *MetricsCollection) UpdateDBMetrics() {
	if mc.computeDBSizes != nil {
		for db, size := range mc.computeDBSizes() {
			mc.DBSizeGauges.WithLabelValues(db).Set(size)
		}
	}
	if mc.computeDBEntries != nil {
		for db, nbEntries := range mc.computeDBEntries() {
			mc.DBEntriesGauges.WithLabelValues(db).Set(nbEntries)
		}
	}
}

type storeMetrics struct {
	lastIndexedTrx   prometheus.Gauge
	lastCommittedTrx prometheus.Gauge
}

func (m *storeMetrics) LastCommittedTrx(id uint64) { m.lastCommittedTrx.Set(float64(id)) }
func (m *storeMetrics) LastIndexedTrx(id uint64)   { m.lastIndexedTrx.Set(float64(id)) }

func (mc *MetricsCollection) storeMetrics(db string) store.Metrics {
	return &storeMetrics{
		lastIndexedTrx:   mc.lastIndexedTrx.WithLabelValues(db),
		lastCommittedTrx: mc.lastCommittedTrx.WithLabelValues(db),
	}
}

// StartMetrics listens and servers the HTTP metrics server in a new goroutine.
// The server is then returned and can be stopped using Close().
func NewMetricsCollection(
	uptimeCounter func() float64,
	computeDBSizes func() map[string]float64,
	computeDBEntries func() map[string]float64,
) *MetricsCollection {

	registry := prometheus.NewRegistry()
	promauto := promauto.With(registry)

	// metrics immudb Prometheus metrics collection
	metrics := &MetricsCollection{
		registry: registry,

		RPCsPerClientCounters: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: metricsNamespace,
				Name:      "number_of_rpcs_per_client",
				Help:      "Number of handled RPCs per client.",
			},
			[]string{"ip"},
		),
		DBSizeGauges: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: metricsNamespace,
				Name:      "db_size_bytes",
				Help:      "Database size in bytes.",
			},
			[]string{"db"},
		),
		DBEntriesGauges: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: metricsNamespace,
				Name:      "number_of_stored_entries",
				Help:      "Number of key-value entries currently stored by the database.",
			},
			[]string{"db"},
		),
		LastMessageAtPerClientGauges: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: metricsNamespace,
				Name:      "clients_last_message_at_unix_seconds",
				Help:      "Timestamp at which clients have sent their most recent message.",
			},
			[]string{"ip"},
		),

		lastIndexedTrx: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "last_indexed_trx_id",
			Help:      "The id of last indexed transaction",
		}, []string{"db"}),

		lastCommittedTrx: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "last_committed_trx_id",
			Help:      "The id of last committed transaction",
		}, []string{"db"}),

		multiappCacheEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "multiapp_cache_events",
			Help:      "Immudb multiapp cache event counters",
		}, []string{"event"}),

		// 	metricsCacheEvicted = metricsCacheEvents.WithLabelValues("evicted")
		// 	metricsCacheHit     = metricsCacheEvents.WithLabelValues("hit")
		// 	metricsCacheMiss    = metricsCacheEvents.WithLabelValues("miss")

		multiappReadEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "multiapp_read_events",
			Help:      "Immudb multiapp read event counters",
		}, []string{"event"}),

		// 	metricsReads      = metricsReadEvents.WithLabelValues("total_reads")
		// 	metricsReadErrors = metricsReadEvents.WithLabelValues("errors")

		metricsReadBytes: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "multiapp_read_bytes",
			Help:      "Number of bytes read",
		}),
	}

	metrics.WithUptimeCounter(uptimeCounter)
	metrics.WithComputeDBSizes(computeDBSizes)
	metrics.WithComputeDBEntries(computeDBEntries)

	return metrics
}

func (mc *MetricsCollection) StartServer(
	updateInterval time.Duration,
	addr string,
	l logger.Logger,
) *http.Server {

	go func() {
		mc.UpdateDBMetrics()
		for range time.Tick(updateInterval) {
			mc.UpdateDBMetrics()
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", corsHandler(mc.prometheusHandler()))
	mux.Handle("/debug/vars", corsHandler(expvar.Handler()))
	mux.HandleFunc("/initz", corsHandlerFunc(ImmudbHealthHandlerFunc()))
	mux.HandleFunc("/readyz", corsHandlerFunc(ImmudbHealthHandlerFunc()))
	mux.HandleFunc("/livez", corsHandlerFunc(ImmudbHealthHandlerFunc()))
	mux.HandleFunc("/version", corsHandlerFunc(ImmudbVersionHandlerFunc))
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

func (mc *MetricsCollection) prometheusHandler() http.Handler {
	return promhttp.InstrumentMetricHandler(
		mc.registry, promhttp.HandlerFor(mc.registry, promhttp.HandlerOpts{}),
	)
}

func ImmudbHealthHandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}
}

func ImmudbVersionHandlerFunc(w http.ResponseWriter, r *http.Request) {
	writeJSONResponse(w, r, 200, &Version)
}

func corsHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		addCORSHeaders(w, r)
		handler.ServeHTTP(w, r)
	})
}

func corsHandlerFunc(handlerFunc http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		addCORSHeaders(w, r)
		handlerFunc(w, r)
	}
}

func addCORSHeaders(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers for the preflight request
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.Header().Set(
			"Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Access-Control-Allow-Origin, Access-Control-Allow-Methods, Access-Control-Allow-Credentials")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Set CORS headers for the main request.
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

func writeJSONResponse(
	w http.ResponseWriter,
	r *http.Request,
	statusCode int,
	body interface{}) {

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(body)
}
