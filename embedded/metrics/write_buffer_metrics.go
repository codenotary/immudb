/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type WriteBufferMetrics interface {
	IncAllocatedPages()
	ResetAllocatedPages()
}

var (
	metricsWriteBufferAllocatedPages = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_write_buffer_allocated_pages",
		Help: "Number of allocated pages in the write buffer",
	}, []string{"id"})
)

type prometheusWriteBufferMetrics struct {
	id string
}

func NewPrometheusWriteBufferMetrics(id int) WriteBufferMetrics {
	return &prometheusWriteBufferMetrics{
		id: strconv.Itoa(id),
	}
}

func (m *prometheusWriteBufferMetrics) IncAllocatedPages() {
	metricsWriteBufferAllocatedPages.WithLabelValues(m.id).Inc()
}

func (m *prometheusWriteBufferMetrics) ResetAllocatedPages() {
	metricsWriteBufferAllocatedPages.WithLabelValues(m.id).Set(0)
}

var (
	_ WriteBufferMetrics = &prometheusWriteBufferMetrics{}
	_ WriteBufferMetrics = &nopWriteBufferMetrics{}
)

type nopWriteBufferMetrics struct {
}

func NewNopWriteBufferMetrics() WriteBufferMetrics {
	return &nopWriteBufferMetrics{}
}

func (m *nopWriteBufferMetrics) IncAllocatedPages() {}

func (m *nopWriteBufferMetrics) ResetAllocatedPages() {}
