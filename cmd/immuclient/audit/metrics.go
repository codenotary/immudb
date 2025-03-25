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

package audit

import (
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/prometheus/client_golang/prometheus"
)

type prometheusMetrics struct {
	server_address string
	server_id      string
}

var metricsNamespace = "immuclient"

// Audit metrics
var (
	AuditResultPerServer = newAuditGaugeVec(
		"audit_result_per_server",
		"Latest audit result (1 = ok, 0 = tampered).",
	)
	AuditCurrRootPerServer = newAuditGaugeVec(
		"audit_curr_root_per_server",
		"Current root index used for the latest audit.",
	)
	AuditRunAtPerServer = newAuditGaugeVec(
		"audit_run_at_per_server",
		"Timestamp in unix seconds at which latest audit run.",
	)
	AuditPrevRootPerServer = newAuditGaugeVec(
		"audit_prev_root_per_server",
		"Previous root index used for the latest audit.",
	)
)

func (p *prometheusMetrics) init(serverid string, immudbAddress, immudbPort string) {
	p.server_address = fmt.Sprintf("%s:%s", immudbAddress, immudbPort)
	p.server_id = serverid
	prometheus.MustRegister(AuditResultPerServer, AuditCurrRootPerServer, AuditRunAtPerServer, AuditPrevRootPerServer)
	AuditResultPerServer.WithLabelValues(p.server_id, p.server_address).Set(-1)
	AuditCurrRootPerServer.WithLabelValues(p.server_id, p.server_address).Set(-1)
	AuditRunAtPerServer.WithLabelValues(p.server_id, p.server_address).SetToCurrentTime()
	AuditPrevRootPerServer.WithLabelValues(p.server_id, p.server_address).Set(-1)
}

func newAuditGaugeVec(name string, help string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      name,
			Help:      help,
		},
		[]string{"server_id", "server_address"},
	)
}

func (p *prometheusMetrics) updateMetrics(
	serverID string,
	serverAddress string,
	checked bool,
	withError bool,
	result bool,
	prevState *schema.ImmutableState,
	currState *schema.ImmutableState,
) {
	var r float64
	if checked && result {
		r = 1
	} else if !checked && !withError {
		r = -1
	} else if withError {
		r = -2
	}
	prevRootTxID := -1.
	currRootTxID := -1.
	if withError {
		prevRootTxID = -2.
		currRootTxID = -2.
	}
	if prevState != nil {
		prevRootTxID = float64(prevState.TxId)
	}
	if currState != nil {
		currRootTxID = float64(currState.TxId)
	}

	AuditResultPerServer.
		WithLabelValues(p.server_id, p.server_address).Set(r)
	AuditPrevRootPerServer.
		WithLabelValues(p.server_id, p.server_address).Set(prevRootTxID)
	AuditCurrRootPerServer.
		WithLabelValues(p.server_id, p.server_address).Set(currRootTxID)
	AuditRunAtPerServer.
		WithLabelValues(p.server_id, p.server_address).SetToCurrentTime()
}
