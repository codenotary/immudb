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

package remoteapp

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ---- Opening remote chunks ---------------------------------------

	metricsOpenTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "immudb_remoteapp_open_time",
		Help:    "Histogram of the total time required to open a chunk of data stored on an immudb remote storage",
		Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10, 25, 50},
	})

	metricsCorruptedMetadata = promauto.NewCounter(prometheus.CounterOpts{
		Name: "immudb_remoteapp_corrupted_metadata",
		Help: "Number of corrupted metadata detections in an immudb remote storage",
	})

	// ---- Uncached reads ---------------------------------------

	metricsUncachedReadEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "immudb_remoteapp_uncached_read_events",
		Help: "Direct (uncached) read event counters for immudb remote storage",
	}, []string{"event"})

	metricsUncachedReads      = metricsUncachedReadEvents.WithLabelValues("total_reads")
	metricsUncachedReadErrors = metricsUncachedReadEvents.WithLabelValues("errors")
	metricsUncachedReadBytes  = promauto.NewCounter(prometheus.CounterOpts{
		Name: "immudb_remoteapp_uncached_read_bytes",
		Help: "Direct (uncached) read byte counters for immudb remote storage",
	})

	// ---- Reads ---------------------------------------

	metricsReadEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "immudb_remoteapp_read_events",
		Help: "Read event counters for immudb remote storage",
	}, []string{"event"})
	metricsReads      = metricsReadEvents.WithLabelValues("total_reads")
	metricsReadErrors = metricsReadEvents.WithLabelValues("errors")
	metricsReadBytes  = promauto.NewCounter(prometheus.CounterOpts{
		Name: "immudb_remoteapp_read_bytes",
		Help: "Total number of bytes read from immudb remote storage (including cached reads)",
	})

	// ---- Uploads ---------------------------------------

	metricsUploadEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "immudb_remoteapp_upload_events",
		Help: "Immudb remote storage upload event counters",
	}, []string{"event"})

	metricsUploadStarted   = metricsUploadEvents.WithLabelValues("started")
	metricsUploadFinished  = metricsUploadEvents.WithLabelValues("finished")
	metricsUploadFailed    = metricsUploadEvents.WithLabelValues("failed")
	metricsUploadCancelled = metricsUploadEvents.WithLabelValues("cancelled")
	metricsUploadRetried   = metricsUploadEvents.WithLabelValues("retried")
	metricsUploadSucceeded = metricsUploadEvents.WithLabelValues("succeeded")

	metricsUploadTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "immudb_remoteapp_upload_time",
		Help:    "Histogram of the total time required to upload a chunk to the remote storage",
		Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10, 25, 50},
	})

	// ---- Downloads ---------------------------------------

	metricsDownloadEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "immudb_remoteapp_download_events",
		Help: "Immudb remote storage download event counters",
	}, []string{"event"})

	metricsDownloadStarted   = metricsDownloadEvents.WithLabelValues("started")
	metricsDownloadFinished  = metricsDownloadEvents.WithLabelValues("finished")
	metricsDownloadFailed    = metricsDownloadEvents.WithLabelValues("failed")
	metricsDownloadCancelled = metricsDownloadEvents.WithLabelValues("cancelled")
	metricsDownloadRetried   = metricsDownloadEvents.WithLabelValues("retried")
	metricsDownloadSucceeded = metricsDownloadEvents.WithLabelValues("succeeded")

	// ---- Chunk statistics --------------------------------

	metricsChunkCounts = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_remoteapp_chunk_count",
		Help: "Number of chunks used for immudb remote storage",
	}, []string{"path", "state"})

	metricsChunkDataBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_remoteapp_chunk_bytes",
		Help: "Total number of bytes stored in chunks",
	}, []string{"path", "state"})
)
