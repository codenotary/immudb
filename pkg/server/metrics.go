/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultClientIPMetricsCap is the default maximum number of distinct
// client-IP labels emitted by per-client metrics. Beyond this cap, new IPs
// are bucketed into a single ClientIPOverflowLabel series so an attacker
// (or a noisy NAT pool) cannot create unbounded Prometheus series and
// leak memory inside Prometheus internals or the per-client maps here.
const DefaultClientIPMetricsCap = 10000

// ClientIPOverflowLabel is the IP label assigned to clients beyond the
// configured cap. Operators relying on per-client dashboards should alert
// on a non-zero value here as a sign of cardinality pressure.
const ClientIPOverflowLabel = "overflow"

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

	// computeMu guards the four compute* callback fields below. They are
	// written by StartMetrics via the With*() setters and read by the
	// ticker-driven UpdateDBMetrics loop; without this mutex, a second
	// StartMetrics call (e.g. successive tests against the global Metrics
	// collection) races the still-running goroutine started by the first.
	computeMu           sync.RWMutex
	computeDBSizes      func() map[string]float64
	computeDBEntries    func() map[string]float64
	computeLoadedDBSize func() float64
	computeSessionCount func() float64

	DBSizeGauges    *prometheus.GaugeVec
	DBEntriesGauges *prometheus.GaugeVec

	RPCsPerClientCounters        *prometheus.CounterVec
	LastMessageAtPerClientGauges *prometheus.GaugeVec

	RemoteStorageKind *prometheus.GaugeVec

	LoadedDatabases prometheus.Gauge
	ActiveSessions  prometheus.Gauge

	// Per-client cardinality control (A7): UpdateClientMetrics tracks the
	// distinct IPs it has emitted as labels. Beyond clientIPCap, new IPs
	// are folded into ClientIPOverflowLabel so the Prometheus series count
	// stays bounded.
	clientIPMu            sync.RWMutex
	clientIPs             map[string]struct{}
	clientIPCap           atomic.Int64 // 0 = unlimited
	clientMetricsDisabled atomic.Bool  // true → UpdateClientMetrics is a no-op
}

// SetClientMetricsCap updates the per-client IP cardinality cap at runtime.
// 0 disables the cap (legacy unbounded behaviour, not recommended for
// internet-exposed deployments). The default is DefaultClientIPMetricsCap.
func (mc *MetricsCollection) SetClientMetricsCap(cap int) {
	if cap < 0 {
		cap = 0
	}
	mc.clientIPCap.Store(int64(cap))
}

// SetClientMetricsDisabled toggles per-client metrics emission entirely.
// When true, UpdateClientMetrics returns immediately without touching the
// CounterVec / GaugeVec. Provides operators with a hard off-switch when
// per-client cardinality is undesirable for any reason.
func (mc *MetricsCollection) SetClientMetricsDisabled(disabled bool) {
	mc.clientMetricsDisabled.Store(disabled)
}

// classifyClientLabel returns the label to use for host: either host
// itself when within the cap (or already tracked), or
// ClientIPOverflowLabel when adding host would exceed the cap. The cap
// admission check uses an RLock fast-path; only the first sighting of a
// new host upgrades to a Lock.
func (mc *MetricsCollection) classifyClientLabel(host string) string {
	cap := mc.clientIPCap.Load()
	if cap == 0 {
		return host
	}

	mc.clientIPMu.RLock()
	if mc.clientIPs != nil {
		if _, seen := mc.clientIPs[host]; seen {
			mc.clientIPMu.RUnlock()
			return host
		}
	}
	atCap := mc.clientIPs != nil && len(mc.clientIPs) >= int(cap)
	mc.clientIPMu.RUnlock()

	if atCap {
		return ClientIPOverflowLabel
	}

	mc.clientIPMu.Lock()
	defer mc.clientIPMu.Unlock()
	// Lazy-init protects against zero-value MetricsCollection literals
	// (used in tests). The package-level Metrics is initialised in init().
	if mc.clientIPs == nil {
		mc.clientIPs = make(map[string]struct{})
	}
	// Re-check under write lock — another RPC may have added it concurrently.
	if _, seen := mc.clientIPs[host]; seen {
		return host
	}
	if len(mc.clientIPs) >= int(cap) {
		return ClientIPOverflowLabel
	}
	mc.clientIPs[host] = struct{}{}
	return host
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
	if mc.clientMetricsDisabled.Load() {
		return
	}

	p, ok := peer.FromContext(ctx)
	if !ok || p == nil {
		return
	}

	// net.SplitHostPort correctly handles IPv6 addresses like "[::1]:8080"
	// (strings.Split on ":" would mangle them). Falls through silently on
	// bufconn/mock addresses that don't carry a port.
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return
	}

	label := mc.classifyClientLabel(host)
	mc.RPCsPerClientCounters.WithLabelValues(label).Inc()
	mc.LastMessageAtPerClientGauges.WithLabelValues(label).SetToCurrentTime()
}

// WithComputeDBSizes ...
func (mc *MetricsCollection) WithComputeDBSizes(f func() map[string]float64) {
	mc.computeMu.Lock()
	mc.computeDBSizes = f
	mc.computeMu.Unlock()
}

// WithComputeDBEntries ...
func (mc *MetricsCollection) WithComputeDBEntries(f func() map[string]float64) {
	mc.computeMu.Lock()
	mc.computeDBEntries = f
	mc.computeMu.Unlock()
}

// WithLoadedDBSize ...
func (mc *MetricsCollection) WithLoadedDBSize(f func() float64) {
	mc.computeMu.Lock()
	mc.computeLoadedDBSize = f
	mc.computeMu.Unlock()
}

// WithLoadedDBSize ...
func (mc *MetricsCollection) WithComputeSessionCount(f func() float64) {
	mc.computeMu.Lock()
	mc.computeSessionCount = f
	mc.computeMu.Unlock()
}

// UpdateDBMetrics ...
func (mc *MetricsCollection) UpdateDBMetrics() {
	// Snapshot the callback set under RLock so we don't hold the mutex while
	// the (potentially slow) callbacks execute.
	mc.computeMu.RLock()
	dbSizes := mc.computeDBSizes
	dbEntries := mc.computeDBEntries
	loadedDBSize := mc.computeLoadedDBSize
	sessionCount := mc.computeSessionCount
	mc.computeMu.RUnlock()

	if dbSizes != nil {
		for db, size := range dbSizes() {
			mc.DBSizeGauges.WithLabelValues(db).Set(size)
		}
	}
	if dbEntries != nil {
		for db, nbEntries := range dbEntries() {
			mc.DBEntriesGauges.WithLabelValues(db).Set(nbEntries)
		}
	}
	if loadedDBSize != nil {
		mc.LoadedDatabases.Set(loadedDBSize())
	}
	if sessionCount != nil {
		mc.ActiveSessions.Set(sessionCount())
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

func init() {
	// Initialize the per-client tracker. Done in init() (not in the Metrics
	// struct literal above) because atomic.Int64.Store has no const form
	// and the map needs an explicit make.
	Metrics.clientIPs = make(map[string]struct{})
	Metrics.clientIPCap.Store(DefaultClientIPMetricsCap)

	// Q3: wire embedded/sql catalog cache hooks into Prometheus counters.
	// The hooks are no-op by default; replacing them here means any binary
	// linking pkg/server gets cache observability for free, while embedded
	// users (sql-only) avoid the Prometheus dependency.
	sql.CatalogCacheHitObserver = func() { CatalogCacheTotal.WithLabelValues("hit").Inc() }
	sql.CatalogCacheMissObserver = func() { CatalogCacheTotal.WithLabelValues("miss").Inc() }
}

// Q3 metrics: per-op latency histograms and cache observability. The {op}
// label is bounded by the gRPC method set (a fixed, low-cardinality
// universe known at compile time), so no extra cap is needed beyond what
// the Prometheus client library already enforces.
var (
	// QueryLatencySeconds tracks per-op gRPC handler duration. The op
	// label maps to grpc.FullMethod (e.g. "/immudb.schema.ImmuService/Get").
	// Buckets span sub-millisecond to multi-second; tail percentiles
	// (p99, p999) are the operationally interesting points for ops
	// dashboards. Differs from grpc_prometheus's default histogram in
	// that it's namespaced under "immudb_" — easier to scope alerts and
	// dashboards to the database itself rather than every gRPC server in
	// the deployment.
	QueryLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "query_latency_seconds",
			Help:      "Per-op gRPC handler latency in seconds (op = grpc.FullMethod).",
			Buckets: []float64{
				0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
			},
		},
		[]string{"op"},
	)

	// CatalogCacheTotal counts catalog-cache hits and misses on engine
	// NewTx. The hit ratio (hit / (hit + miss)) is the figure operators
	// actually track in dashboards; expose as two counters so PromQL
	// rate(...)/rate(...) works without precomputing the ratio.
	CatalogCacheTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "catalog_cache_total",
			Help:      "Catalog cache lookups by result (hit/miss). Hit ratio = hit / (hit+miss).",
		},
		[]string{"result"},
	)

	// IndexSeekLatencySeconds is reserved for future instrumentation of
	// the index seek hot path in embedded/sql + embedded/store. Histogram
	// definition lives here so the metric exists from binary start; the
	// embedded layer can populate it via a callback hook (see
	// CatalogCacheHitObserver pattern above) once the seek-time
	// accounting is added on the store side.
	IndexSeekLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "index_seek_latency_seconds",
			Help:      "Latency of individual index seek operations.",
			Buckets: []float64{
				0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1,
			},
		},
	)
)

// QueryLatencyInterceptor returns a unary gRPC interceptor that records
// handler duration into QueryLatencySeconds. Add to the unary chain in
// server.go to enable per-op latency dashboards. Cardinality is bounded
// by the static gRPC method set, so no per-label cap is needed (cf. A7).
func QueryLatencyInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		QueryLatencySeconds.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		return resp, err
	}
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

	// Q5 / pre-existing-leak fix: replace `time.Tick` (which leaked on every
	// metrics-server lifecycle) with a NewTicker tied to the server's
	// RegisterOnShutdown hook. Callers must invoke server.Shutdown(ctx)
	// (not raw Close) to terminate the ticker; pkg/server/server.go does so.
	ticker := time.NewTicker(updateInterval)
	tickerDone := make(chan struct{})
	go func() {
		Metrics.UpdateDBMetrics()
		for {
			select {
			case <-tickerDone:
				return
			case <-ticker.C:
				Metrics.UpdateDBMetrics()
			}
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
	server.RegisterOnShutdown(func() {
		ticker.Stop()
		close(tickerDone)
	})

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
