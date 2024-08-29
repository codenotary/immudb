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

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"expvar"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/peer"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/prometheus/client_golang/prometheus"
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
	FIPS      bool   `json:"fips"`
}

// MetricsCollection immudb Prometheus metrics collection
type MetricsCollection struct {
	UptimeCounter prometheus.CounterFunc

	computeDBSizes func() map[string]float64
	DBSizeGauges   *prometheus.GaugeVec

	computeDBEntries func() map[string]float64
	DBEntriesGauges  *prometheus.GaugeVec

	RPCsPerClientCounters        *prometheus.CounterVec
	LastMessageAtPerClientGauges *prometheus.GaugeVec

	RemoteStorageKind *prometheus.GaugeVec

	computeLoadedDBSize func() float64
	LoadedDatabases     prometheus.Gauge

	computeSessionCount func() float64
	ActiveSessions      prometheus.Gauge
}

var metricsNamespace = "immudb"

// WithUptimeCounter ...
func (mc *MetricsCollection) WithUptimeCounter(f func() float64) {
	if mc.UptimeCounter != nil {
		return
	}

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

// WithLoadedDBSize ...
func (mc *MetricsCollection) WithLoadedDBSize(f func() float64) {
	mc.computeLoadedDBSize = f
}

// WithLoadedDBSize ...
func (mc *MetricsCollection) WithComputeSessionCount(f func() float64) {
	mc.computeSessionCount = f
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
	if mc.computeLoadedDBSize != nil {
		mc.LoadedDatabases.Set(mc.computeLoadedDBSize())
	}
	if mc.computeSessionCount != nil {
		mc.ActiveSessions.Set(mc.computeSessionCount())
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
	RemoteStorageKind: promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "remote_storage_kind",
			Help:      "Set to 1 for remote storage kind for given database",
		},
		[]string{"db", "kind"},
	),
	LoadedDatabases: promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "loaded_databases",
			Help:      "Numer of loaded databases",
		},
	),
	ActiveSessions: promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "active_sessions",
			Help:      "Numer of active sessions",
		},
	),
}

// StartMetrics listens and servers the HTTP metrics server in a new goroutine.
// The server is then returned and can be stopped using Close().
func StartMetrics(
	updateInterval time.Duration,
	addr string,
	tlsConfig *tls.Config,
	l logger.Logger,
	uptimeCounter func() float64,
	computeDBSizes func() map[string]float64,
	computeDBEntries func() map[string]float64,
	computeLoadedDBSize func() float64,
	computeSessionCount func() float64,
	addPProf bool,
) *http.Server {
	Metrics.WithUptimeCounter(uptimeCounter)
	Metrics.WithComputeDBSizes(computeDBSizes)
	Metrics.WithComputeDBEntries(computeDBEntries)
	Metrics.WithLoadedDBSize(computeLoadedDBSize)
	Metrics.WithComputeSessionCount(computeSessionCount)

	go func() {
		Metrics.UpdateDBMetrics()
		for range time.Tick(updateInterval) {
			Metrics.UpdateDBMetrics()
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", corsHandler(promhttp.Handler()))
	mux.Handle("/debug/vars", corsHandler(expvar.Handler()))
	if addPProf {
		mux.HandleFunc("/debug/pprof/", corsHandlerFunc(pprof.Index))
		mux.HandleFunc("/debug/pprof/cmdline", corsHandlerFunc(pprof.Cmdline))
		mux.HandleFunc("/debug/pprof/profile", corsHandlerFunc(pprof.Profile))
		mux.HandleFunc("/debug/pprof/symbol", corsHandlerFunc(pprof.Symbol))
		mux.HandleFunc("/debug/pprof/trace", corsHandlerFunc(pprof.Trace))
	}
	mux.HandleFunc("/initz", corsHandlerFunc(ImmudbHealthHandlerFunc()))
	mux.HandleFunc("/readyz", corsHandlerFunc(ImmudbHealthHandlerFunc()))
	mux.HandleFunc("/livez", corsHandlerFunc(ImmudbHealthHandlerFunc()))
	mux.HandleFunc("/version", corsHandlerFunc(ImmudbVersionHandlerFunc))

	server := &http.Server{Addr: addr, Handler: mux}
	server.TLSConfig = tlsConfig

	go func() {
		var err error
		if tlsConfig != nil && len(tlsConfig.Certificates) > 0 {
			l.Infof("metrics server enabled on %s (https)", addr)
			err = server.ListenAndServeTLS("", "")
		} else {
			l.Infof("metrics server enabled on %s (http)", addr)
			err = server.ListenAndServe()
		}

		if err == http.ErrServerClosed {
			l.Debugf("Metrics http server closed")
		} else {
			l.Errorf("Metrics error: %s", err)
		}
	}()
	return server
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
