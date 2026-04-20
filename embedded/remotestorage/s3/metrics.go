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

package s3

import (
	"io"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricsUploadBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "immudb_remoteapp_s3_upload_bytes",
		Help: "Number data bytes (excluding headers) uploaded to s3",
	})

	metricsDownloadBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "immudb_remoteapp_s3_download_bytes",
		Help: "Number data bytes (excluding headers) downloaded from s3",
	})

	// Per-request latency distribution, partitioned by HTTP method.
	metricsRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "immudb_remoteapp_s3_request_duration_seconds",
		Help:    "Latency of S3 HTTP requests in seconds, from send to first byte returned.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"method"})

	// Terminal HTTP response outcomes, by method and status class (e.g. 2xx, 4xx, 5xx).
	metricsRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "immudb_remoteapp_s3_requests_total",
		Help: "Total S3 HTTP requests by method and status class (2xx/3xx/4xx/5xx/error).",
	}, []string{"method", "status_class"})

	// Incremented whenever an S3 request fails to reach a valid terminal status
	// (transport error, unexpected status code, or too-many-redirects).
	metricsRequestErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "immudb_remoteapp_s3_request_errors_total",
		Help: "Total S3 HTTP request failures by method and reason (transport, status, redirects).",
	}, []string{"method", "reason"})

	// In-flight gauge: number of signed requests currently awaiting a response.
	metricsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "immudb_remoteapp_s3_in_flight_requests",
		Help: "Number of S3 HTTP requests currently in flight.",
	})
)

// statusClass returns the Prometheus label value for an HTTP status code,
// collapsing individual codes into a bucket label (e.g. 206 -> "2xx").
func statusClass(code int) string {
	switch {
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500 && code < 600:
		return "5xx"
	default:
		return strconv.Itoa(code)
	}
}

type metricsCountingReadCloser struct {
	r io.ReadCloser
	c prometheus.Counter
}

func (m *metricsCountingReadCloser) Read(b []byte) (int, error) {
	n, err := m.r.Read(b)
	m.c.Add(float64(n))
	return n, err
}

func (m *metricsCountingReadCloser) Close() error {
	return m.r.Close()
}
