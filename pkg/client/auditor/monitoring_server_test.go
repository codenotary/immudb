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

package auditor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestStartHTTPServerForMonitoring(t *testing.T) {
	// happy path
	httpServer := StartHTTPServerForMonitoring(
		"",
		func(hs *http.Server) error { return nil },
		nil,
		nil)
	require.NotNil(t, httpServer)

	l := logger.NewSimpleLogger("monitor_server_test", os.Stderr)

	// "server closed" error path
	StartHTTPServerForMonitoring(
		"",
		func(hs *http.Server) error { return http.ErrServerClosed },
		l,
		nil)
	require.NotNil(t, httpServer)

	// some other listen and serve error
	httpServer = StartHTTPServerForMonitoring(
		"",
		func(hs *http.Server) error { return errors.New("some unexpected error") },
		l,
		nil)
	require.NotNil(t, httpServer)
}

func TestAuditorHealthHandlerFunc(t *testing.T) {
	req, err := http.NewRequest("GET", "/initz", nil)
	require.NoError(t, err)

	testCases := []struct {
		status bool
		err    error
		code   int
		body   string
	}{
		{true, nil, http.StatusOK, "OK"},
		{false, errors.New("some health error"), http.StatusServiceUnavailable, "some health error"},
		{false, nil, http.StatusServiceUnavailable, "unhealthy"},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d %s: %s", tc.code, http.StatusText(tc.code), tc.body), func(t *testing.T) {
			rr := httptest.NewRecorder()
			immuServiceClientMock := &clienttest.ImmuServiceClientMock{
				HealthF: func(context.Context, *empty.Empty, ...grpc.CallOption) (*schema.HealthResponse, error) {
					return &schema.HealthResponse{Status: tc.status}, tc.err
				},
			}
			handler := corsHandlerFunc(AuditorHealthHandlerFunc(immuServiceClientMock))
			handler.ServeHTTP(rr, req)
			require.Equal(t, tc.code, rr.Code)
			expectedBody, _ := json.Marshal(&HealthResponse{tc.body})
			require.Equal(t, string(expectedBody)+"\n", rr.Body.String())
		})
	}
}

func TestAuditorVersionHandlerFunc(t *testing.T) {
	// test OPTIONS /version
	req, err := http.NewRequest("OPTIONS", "/version", nil)
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	handler := corsHandlerFunc(AuditorVersionHandlerFunc)
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
	handler = corsHandlerFunc(AuditorVersionHandlerFunc)
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
