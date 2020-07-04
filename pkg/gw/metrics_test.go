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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
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
	handler := http.HandlerFunc(lastAuditHandler)
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
		json.Unmarshal(rr.Body.Bytes(), &actual),
		"can not unmarshal to LastAuditResult the lastAuditHandler response %s",
		rr.Body.String())
}
func TestUpdateAuditResult(t *testing.T) {
	Metrics.UpdateAuditResult(
		"server1",
		"127.0.0.1",
		true,
		false,
		true,
		&schema.Root{Index: 0, Root: []byte{1}},
		&schema.Root{Index: 1, Root: []byte{2}},
	)
	require.Equal(t, 0., Metrics.lastAuditResult.PreviousRootIndex)
	require.Equal(t, 1., Metrics.lastAuditResult.CurrentRootIndex)
	require.Equal(t, "server1", Metrics.lastAuditResult.ServerID)
	require.Equal(t, "127.0.0.1", Metrics.lastAuditResult.ServerAddress)
	require.True(t, Metrics.lastAuditResult.HasRunConsistencyCheck)
	require.False(t, Metrics.lastAuditResult.HasError)
	require.True(t, Metrics.lastAuditResult.ConsistencyCheckResult)

	Metrics.UpdateAuditResult("server1", "127.0.0.1", false, false, true, nil, nil)
	require.Equal(t, -1., Metrics.lastAuditResult.PreviousRootIndex)
	require.Equal(t, -1., Metrics.lastAuditResult.CurrentRootIndex)

	Metrics.UpdateAuditResult("server1", "127.0.0.1", false, true, false, nil, nil)
	require.Equal(t, -2., Metrics.lastAuditResult.PreviousRootIndex)
	require.Equal(t, -2., Metrics.lastAuditResult.CurrentRootIndex)
}
