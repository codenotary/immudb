/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"net"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/peer"
)

func TestStartMetrics(t *testing.T) {
	server := StartMetrics(
		"0.0.0.0:9999",
		&mockLogger{},
		func() float64 { return 0 },
		func() map[string]float64 { return make(map[string]float64) },
		func() map[string]float64 { return make(map[string]float64) },
	)
	defer server.Close()

	assert.IsType(t, &http.Server{}, server)

}

func TestMetricsCollection_UpdateClientMetrics(t *testing.T) {
	mc := MetricsCollection{
		UptimeCounter: prometheus.NewCounterFunc(prometheus.CounterOpts{}, func() float64 {
			return 0
		}),
		RPCsPerClientCounters: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test",
			},
			[]string{"test"},
		),
		LastMessageAtPerClientGauges: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "namespace_test",
				Subsystem: "subsystem_test",
				Name:      "test",
				Help:      "test",
			},
			[]string{
				// Which user has requested the operation?
				"test",
			},
		),
	}
	ip := net.IP{}
	ip.UnmarshalText([]byte(`127.0.0.1`))
	p := &peer.Peer{
		Addr: &net.TCPAddr{
			IP:   ip,
			Port: 9999,
			Zone: "zone",
		},
	}
	ctx := peer.NewContext(context.TODO(), p)
	mc.UpdateClientMetrics(ctx)

	assert.IsType(t, MetricsCollection{}, mc)
}

func TestMetricsCollection_UpdateDBMetrics(t *testing.T) {
	mc := MetricsCollection{
		DBSizeGauges: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: metricsNamespace,
				Name:      "db_size_bytes",
				Help:      "Database size in bytes.",
			},
			[]string{"db"},
		),
		DBEntriesGauges: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: metricsNamespace,
				Name:      "number_of_stored_entries",
				Help:      "Number of key-value entries currently stored by the database.",
			},
			[]string{"db"},
		),
	}

	// update before injecting the funcs, to catch the fast-exit execution path
	mc.UpdateDBMetrics()

	mc.computeDBSizes = func() map[string]float64 {
		return map[string]float64{"db1": 111, "db2": 222}
	}
	mc.computeDBEntries = func() map[string]float64 {
		return map[string]float64{"db1": 10, "db2": 20}
	}

	// update after injecting the funcs, to catch the normal execution path
	mc.UpdateDBMetrics()

	assert.IsType(t, MetricsCollection{}, mc)
}
