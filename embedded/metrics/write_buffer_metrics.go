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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type WriteBufferMetrics interface {
	SetMaxSize(size int)
	IncAllocatedPages()
	ResetAllocatedPages()
}

var (
	metricsWriteBufferMaxSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_write_buffer_max_size",
		Help: "Numbers of btree pages written to disk during the last flush process",
	}, []string{"index_id"})

	metricsWriteBufferAllocatedSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_write_buffer_allocated_size",
		Help: "Btree depth",
	}, []string{"index_id"})
)

var _ WriteBufferMetrics = &nopWriteBufferMetrics{}

type nopWriteBufferMetrics struct {
}

func NewNopWriteBufferMetrics() WriteBufferMetrics {
	return &nopWriteBufferMetrics{}
}

func (m *nopWriteBufferMetrics) SetMaxSize(size int) {

}

func (m *nopWriteBufferMetrics) IncAllocatedPages() {

}

func (m *nopWriteBufferMetrics) ResetAllocatedPages() {

}
