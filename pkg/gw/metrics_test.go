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
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/json"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestNewMetricsServer(t *testing.T) {
	server := newMetricsServer(
		"127.0.0.1",
		logger.NewSimpleLogger("metrics_test", os.Stdout),
		func() float64 { return 1 })
	require.NotNil(t, server)
}

func TestLastAuditHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/lastaudit", nil)
	require.NoError(t, err)
	rr := httptest.NewRecorder()

	ms := metricServer{
		mc:  &MetricsCollection{},
		srv: nil,
	}

	handler := http.HandlerFunc(ms.lastAuditHandler(json.DefaultJSON()))
	handler.ServeHTTP(rr, req)
	require.Equal(
		t,
		http.StatusOK,
		rr.Code,
		"lastAuditHandler returned wrong status code: expected %v, actual %v",
		http.StatusOK,
		rr.Code)
	var actual LastAuditResult
	require.NoError(
		t,
		json.DefaultJSON().Unmarshal(rr.Body.Bytes(), &actual),
		"can not unmarshal to LastAuditResult the lastAuditHandler response %s",
		rr.Body.String())
}

func TestLastAuditHandlerJSONError(t *testing.T) {
	req, err := http.NewRequest("GET", "/lastaudit", nil)
	require.NoError(t, err)
	rr := httptest.NewRecorder()

	ms := metricServer{
		mc:  &MetricsCollection{},
		srv: nil,
	}

	handlerWithJSONErr := http.HandlerFunc(ms.lastAuditHandler(newTestJSONWithMarshalErr()))
	handlerWithJSONErr.ServeHTTP(rr, req)
	require.Equal(
		t,
		http.StatusInternalServerError,
		rr.Code,
		"lastAuditHandler returned wrong status code: expected %v, actual %v",
		http.StatusInternalServerError,
		rr.Code)
	require.Contains(t, rr.Body.String(), "JSON marshal error")
}

func TestUpdateAuditResult(t *testing.T) {
	reg := prometheus.NewRegistry()

	ms := metricServer{
		mc: &MetricsCollection{
			lastAuditResult: &LastAuditResult{},
			AuditResultPerServer: newAuditGaugeVec(
				reg,
				"audit_result_per_server",
				"Latest audit result (1 = ok, 0 = tampered).",
			),
			AuditPrevRootPerServer: newAuditGaugeVec(
				reg,
				"audit_prev_root_per_server",
				"Previous root index used for the latest audit.",
			),
			AuditCurrRootPerServer: newAuditGaugeVec(
				reg,
				"audit_curr_root_per_server",
				"Current root index used for the latest audit.",
			),
			AuditRunAtPerServer: newAuditGaugeVec(
				reg,
				"audit_run_at_per_server",
				"Timestamp in unix seconds at which latest audit run.",
			),
		},
		srv: nil,
	}

	ms.mc.UpdateAuditResult(
		"server1",
		"127.0.0.1",
		true,
		false,
		true,
		&schema.Root{Index: 0, Root: []byte{1}},
		&schema.Root{Index: 1, Root: []byte{2}},
	)
	require.Equal(t, 0., ms.mc.lastAuditResult.PreviousRootIndex)
	require.Equal(t, 1., ms.mc.lastAuditResult.CurrentRootIndex)
	require.Equal(t, "server1", ms.mc.lastAuditResult.ServerID)
	require.Equal(t, "127.0.0.1", ms.mc.lastAuditResult.ServerAddress)
	require.True(t, ms.mc.lastAuditResult.HasRunConsistencyCheck)
	require.False(t, ms.mc.lastAuditResult.HasError)
	require.True(t, ms.mc.lastAuditResult.ConsistencyCheckResult)

	ms.mc.UpdateAuditResult("server1", "127.0.0.1", false, false, true, nil, nil)
	require.Equal(t, -1., ms.mc.lastAuditResult.PreviousRootIndex)
	require.Equal(t, -1., ms.mc.lastAuditResult.CurrentRootIndex)

	ms.mc.UpdateAuditResult("server1", "127.0.0.1", false, true, false, nil, nil)
	require.Equal(t, -2., ms.mc.lastAuditResult.PreviousRootIndex)
	require.Equal(t, -2., ms.mc.lastAuditResult.CurrentRootIndex)
}
