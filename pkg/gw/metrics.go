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

package gw

import (
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// LastAuditResult ...
type LastAuditResult struct {
	ServerID               string
	ServerAddress          string
	HasRunConsistencyCheck bool
	HasError               bool
	ConsistencyCheckResult bool
	PreviousRootIndex      float64
	PreviousRoot           string
	CurrentRootIndex       float64
	CurrentRoot            string
	RunAt                  time.Time
	sync.RWMutex
}

// MetricsCollection the gateway metrics collection
type MetricsCollection struct {
	lastAuditResult *LastAuditResult

	AuditResultPerServer   *prometheus.GaugeVec
	AuditPrevRootPerServer *prometheus.GaugeVec
	AuditCurrRootPerServer *prometheus.GaugeVec
	AuditRunAtPerServer    *prometheus.GaugeVec

	UptimeCounter prometheus.CounterFunc
}

var metricsNamespace = "immugw"

// WithUptimeCounter ...
func (mc *MetricsCollection) WithUptimeCounter(f func() float64) {
	mc.UptimeCounter = promauto.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "uptime_hours",
			Help:      "Immu gateway server uptime in hours.",
		},
		f,
	)
}

// UpdateAuditResult updates the metrics related to audit result
func (mc *MetricsCollection) UpdateAuditResult(
	serverID string,
	serverAddress string,
	checked bool,
	withError bool,
	result bool,
	prevRoot *schema.Root,
	currRoot *schema.Root,
) {
	var r float64
	if checked && result {
		r = 1
	} else if !checked && !withError {
		r = -1
	} else if withError {
		r = -2
	}
	prevRootIndex := -1.
	currRootIndex := -1.
	if withError {
		prevRootIndex = -2.
		currRootIndex = -2.
	}
	if prevRoot != nil {
		prevRootIndex = float64(prevRoot.GetIndex())
	}
	if currRoot != nil {
		currRootIndex = float64(currRoot.GetIndex())
	}

	mc.AuditResultPerServer.
		WithLabelValues(serverID, serverAddress).Set(r)
	mc.AuditPrevRootPerServer.
		WithLabelValues(serverID, serverAddress).Set(prevRootIndex)
	mc.AuditCurrRootPerServer.
		WithLabelValues(serverID, serverAddress).Set(currRootIndex)
	mc.AuditRunAtPerServer.
		WithLabelValues(serverID, serverAddress).SetToCurrentTime()

	mc.lastAuditResult.Lock()
	defer mc.lastAuditResult.Unlock()
	mc.lastAuditResult.ServerID = serverID
	mc.lastAuditResult.ServerAddress = serverAddress
	mc.lastAuditResult.HasRunConsistencyCheck = checked
	mc.lastAuditResult.HasError = withError
	mc.lastAuditResult.ConsistencyCheckResult = checked && !withError && result
	mc.lastAuditResult.PreviousRootIndex = prevRootIndex
	mc.lastAuditResult.PreviousRoot = fmt.Sprintf("%x", prevRoot.GetRoot())
	mc.lastAuditResult.CurrentRootIndex = currRootIndex
	mc.lastAuditResult.CurrentRoot = fmt.Sprintf("%x", currRoot.GetRoot())
	mc.lastAuditResult.RunAt = time.Now()
}

func newAuditGaugeVec(name string, help string) *prometheus.GaugeVec {
	return promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      name,
			Help:      help,
		},
		[]string{"server_id", "server_address"},
	)
}

// Metrics gateway metrics collection
var Metrics = MetricsCollection{
	lastAuditResult: &LastAuditResult{},
	AuditResultPerServer: newAuditGaugeVec(
		"audit_result_per_server",
		"Latest audit result (1 = ok, 0 = tampered).",
	),
	AuditPrevRootPerServer: newAuditGaugeVec(
		"audit_prev_root_per_server",
		"Previous root index used for the latest audit.",
	),
	AuditCurrRootPerServer: newAuditGaugeVec(
		"audit_curr_root_per_server",
		"Current root index used for the latest audit.",
	),
	AuditRunAtPerServer: newAuditGaugeVec(
		"audit_run_at_per_server",
		"Timestamp in unix seconds at which latest audit run.",
	),
}

// StartMetrics listens and servers the HTTP metrics server in a new goroutine.
// The server is then returned and can be stopped using Close().
func StartMetrics(
	addr string,
	l logger.Logger,
	uptimeCounter func() float64,
) *http.Server {
	Metrics.WithUptimeCounter(uptimeCounter)
	// expvar package adds a handler in to the default HTTP server (which has to be started explicitly),
	// and serves up the metrics at the /debug/vars endpoint.
	// Here we're registering both expvar and promhttp handlers in our custom server.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/lastaudit", func(w http.ResponseWriter, req *http.Request) {
		bs, err := json.Marshal(Metrics.lastAuditResult)
		if err != nil {
			l.Errorf("error marshaling to json the last audit result: %v", err)
			http.Error(w, fmt.Sprintf("internal error: %v", err), http.StatusInternalServerError)
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(bs); err != nil {
			l.Errorf("error writing the response with the last audit result json: %v", err)
		}
	})
	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				l.Infof("Metrics http server closed")
			} else {
				l.Errorf("Metrics error: %s", err)
			}

		}
	}()

	return server
}
