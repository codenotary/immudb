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
package multiapp

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ---- LRU Cache ---------------------------------------

	metricsCacheEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "immudb_multiapp_cache_events",
		Help: "Immudb multiapp cache event counters",
	}, []string{"event"})

	metricsCacheEvicted = metricsCacheEvents.WithLabelValues("evicted")
	metricsCacheHit     = metricsCacheEvents.WithLabelValues("hit")
	metricsCacheMiss    = metricsCacheEvents.WithLabelValues("miss")

	// ---- Read stats ---------------------------------------

	metricsReadEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "immudb_multiapp_read_events",
		Help: "Immudb multiapp read event counters",
	}, []string{"event"})

	metricsReads      = metricsReadEvents.WithLabelValues("total_reads")
	metricsReadErrors = metricsReadEvents.WithLabelValues("errors")
	metricsReadBytes  = promauto.NewCounter(prometheus.CounterOpts{
		Name: "immudb_multiapp_read_bytes",
		Help: "Number of bytes read",
	})
)
