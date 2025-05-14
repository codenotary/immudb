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
)

type ProgressTracker interface {
	Add(v float64)
}

type prometheusProgressTracker struct {
	progress  prometheus.Gauge
	currValue float64
	maxValue  float64
}

func NewPrometheusProgressTracker(maxValue float64, gauge prometheus.Gauge) ProgressTracker {
	gauge.Set(0)

	return &prometheusProgressTracker{
		progress: gauge,
		maxValue: maxValue,
	}
}

func (p *prometheusProgressTracker) Add(v float64) {
	p.currValue += v
	p.progress.Set(p.currValue / p.maxValue)
}
