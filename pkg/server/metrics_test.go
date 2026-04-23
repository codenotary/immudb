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
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
)

func TestStartMetricsHTTP(t *testing.T) {
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{},
		ClientAuth:   tls.VerifyClientCertIfGiven,
	}

	server := StartMetrics(
		100*time.Millisecond,
		"0.0.0.0:9999",
		tlsConfig,
		&mockLogger{},
		func() float64 { return 0 },
		func() map[string]float64 { return make(map[string]float64) },
		func() map[string]float64 { return make(map[string]float64) },
		func() float64 { return 1.0 },
		func() float64 { return 2.0 },
		false,
	)
	time.Sleep(200 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	require.IsType(t, &http.Server{}, server)
}

func TestStartMetricsHTTPS(t *testing.T) {
	tlsConfig := tlsConfigTest(t)

	server := StartMetrics(
		100*time.Millisecond,
		"0.0.0.0:9999",
		tlsConfig,
		&mockLogger{},
		func() float64 { return 0 },
		func() map[string]float64 { return make(map[string]float64) },
		func() map[string]float64 { return make(map[string]float64) },
		func() float64 { return 1.0 },
		func() float64 { return 2.0 },
		false,
	)
	time.Sleep(200 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	require.IsType(t, &http.Server{}, server)

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: tr}
	require.Eventually(t, func() bool {
		_, err := client.Get("https://0.0.0.0:9999")
		return err == nil
	}, 10*time.Second, 30*time.Millisecond)
}

func TestStartMetricsFail(t *testing.T) {
	save_metricsNamespace := metricsNamespace
	metricsNamespace = "failimmudb"
	defer func() { metricsNamespace = save_metricsNamespace }()

	server := StartMetrics(
		100*time.Millisecond,
		"999.999.999.999:9999",
		nil,
		&mockLogger{},
		func() float64 { return 0 },
		func() map[string]float64 { return make(map[string]float64) },
		func() map[string]float64 { return make(map[string]float64) },
		func() float64 { return 1.0 },
		func() float64 { return 2.0 },
		false,
	)
	time.Sleep(200 * time.Millisecond)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	require.IsType(t, &http.Server{}, server)
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
	ctx := peer.NewContext(context.Background(), p)
	mc.UpdateClientMetrics(ctx)

	require.IsType(t, &MetricsCollection{}, &mc)
}

// TestMetricsCollection_ClientIPCap verifies A7's cardinality cap: once
// the configured cap is reached, additional distinct IPs are folded into
// ClientIPOverflowLabel rather than creating new Prometheus series.
func TestMetricsCollection_ClientIPCap(t *testing.T) {
	mc := MetricsCollection{}
	mc.SetClientMetricsCap(3)

	require.Equal(t, "1.1.1.1", mc.classifyClientLabel("1.1.1.1"))
	require.Equal(t, "2.2.2.2", mc.classifyClientLabel("2.2.2.2"))
	require.Equal(t, "3.3.3.3", mc.classifyClientLabel("3.3.3.3"))
	// At cap — new IPs bucket into the overflow label.
	require.Equal(t, ClientIPOverflowLabel, mc.classifyClientLabel("4.4.4.4"))
	require.Equal(t, ClientIPOverflowLabel, mc.classifyClientLabel("5.5.5.5"))
	// Already-tracked IPs continue to use their own label.
	require.Equal(t, "1.1.1.1", mc.classifyClientLabel("1.1.1.1"))
}

// TestMetricsCollection_ClientMetricsDisabled verifies the off-switch.
func TestMetricsCollection_ClientMetricsDisabled(t *testing.T) {
	mc := MetricsCollection{
		RPCsPerClientCounters: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "test_disabled_counter"},
			[]string{"ip"},
		),
		LastMessageAtPerClientGauges: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "test_disabled_gauge"},
			[]string{"ip"},
		),
	}
	mc.SetClientMetricsDisabled(true)

	ip := net.IP{}
	require.NoError(t, ip.UnmarshalText([]byte("127.0.0.1")))
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: ip, Port: 9999},
	})
	mc.UpdateClientMetrics(ctx)

	// No labels should have been registered.
	require.Equal(t, 0, testCollectorCount(mc.RPCsPerClientCounters))
}

// testCollectorCount returns the number of registered child series in a
// CounterVec. Used to assert no labels are emitted when client metrics
// are disabled.
func testCollectorCount(c *prometheus.CounterVec) int {
	ch := make(chan prometheus.Metric, 16)
	c.Collect(ch)
	close(ch)
	n := 0
	for range ch {
		n++
	}
	return n
}

// BenchmarkUpdateClientMetrics exercises the per-client metric update
// path under cardinality-exhaustion conditions. With A7's cap in place,
// even a workload of 100k distinct IPs degrades to a single overflow
// label after the first DefaultClientIPMetricsCap unique entries.
func BenchmarkUpdateClientMetrics(b *testing.B) {
	mc := MetricsCollection{
		RPCsPerClientCounters: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "bench_rpcs_per_client"},
			[]string{"ip"},
		),
		LastMessageAtPerClientGauges: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "bench_last_at_per_client"},
			[]string{"ip"},
		),
	}
	mc.SetClientMetricsCap(DefaultClientIPMetricsCap)

	const distinctIPs = 100_000
	contexts := make([]context.Context, distinctIPs)
	for i := 0; i < distinctIPs; i++ {
		ip := net.IPv4(byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
		contexts[i] = peer.NewContext(context.Background(), &peer.Peer{
			Addr: &net.TCPAddr{IP: ip, Port: 1024 + i%50000},
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		mc.UpdateClientMetrics(contexts[i%distinctIPs])
	}
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

	require.IsType(t, &MetricsCollection{}, &mc)
}

func TestImmudbHealthHandlerFunc(t *testing.T) {
	req, err := http.NewRequest("GET", "/initz", nil)
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	handler := corsHandlerFunc(ImmudbHealthHandlerFunc())
	handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestImmudbVersionHandlerFunc(t *testing.T) {
	// test OPTIONS /version
	req, err := http.NewRequest("OPTIONS", "/version", nil)
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	handler := corsHandlerFunc(ImmudbVersionHandlerFunc)
	handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusNoContent, rr.Code)

	// test GET /version
	Version = VersionResponse{
		Component: "immudb",
		Version:   "1.2.3",
		BuildTime: time.Now().Format(time.RFC3339),
		BuiltBy:   "SomeBuilder",
		Static:    true,
	}
	req, err = http.NewRequest("GET", "/version", nil)
	require.NoError(t, err)
	rr = httptest.NewRecorder()
	handler = corsHandlerFunc(ImmudbVersionHandlerFunc)
	handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	expectedBody, _ := json.Marshal(&Version)
	require.Equal(t, string(expectedBody)+"\n", rr.Body.String())
}

func TestCORSHandler(t *testing.T) {
	rr := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/metrics", nil)
	require.NoError(t, err)
	handler := corsHandler(promhttp.Handler())
	handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}
