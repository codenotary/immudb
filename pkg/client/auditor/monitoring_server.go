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
	"expvar"
	"net/http"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func StartHTTPServerForMonitoring(
	addr string,
	listenAndServe func(server *http.Server) error,
	l logger.Logger,
	immuServiceClient schema.ImmuServiceClient,
) *http.Server {

	mux := http.NewServeMux()
	promhttpHander := corsHandler(promhttp.Handler())
	mux.Handle("/", promhttpHander)
	mux.Handle("/metrics", promhttpHander)
	mux.Handle("/debug/vars", corsHandler(expvar.Handler()))
	mux.HandleFunc("/initz", corsHandlerFunc(AuditorHealthHandlerFunc(immuServiceClient)))
	mux.HandleFunc("/readyz", corsHandlerFunc(AuditorHealthHandlerFunc(immuServiceClient)))
	mux.HandleFunc("/livez", corsHandlerFunc(AuditorHealthHandlerFunc(immuServiceClient)))
	mux.HandleFunc("/version", corsHandlerFunc(AuditorVersionHandlerFunc))
	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		if err := listenAndServe(server); err != nil {
			if err == http.ErrServerClosed {
				l.Debugf("auditor monitoring HTTP server closed")
			} else {
				l.Errorf("auditor monitoring HTTP server error: %s", err)
			}
		}
	}()

	return server
}

type HealthResponse struct {
	Immudb string `json:"immudb"`
}

func AuditorHealthHandlerFunc(immuServiceClient schema.ImmuServiceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		httpStatus := http.StatusOK
		healthResp := HealthResponse{"OK"}
		health, err := immuServiceClient.Health(context.Background(), new(empty.Empty))
		if err != nil {
			httpStatus = http.StatusServiceUnavailable
			healthResp.Immudb = err.Error()
		} else if !health.GetStatus() {
			httpStatus = http.StatusServiceUnavailable
			healthResp.Immudb = "unhealthy"
		}
		writeJSONResponse(w, r, httpStatus, &healthResp)
	}
}

// VersionResponse ...
type VersionResponse struct {
	Component string `json:"component" example:"immudb"`
	Version   string `json:"version" example:"1.0.1-c9c6495"`
	BuildTime string `json:"buildtime" example:"1604692129"`
	BuiltBy   string `json:"builtby,omitempty"`
	Static    bool   `json:"static"`
	FIPS      bool   `json:"fips"`
}

var Version VersionResponse

func AuditorVersionHandlerFunc(w http.ResponseWriter, r *http.Request) {
	writeJSONResponse(w, r, 200, &Version)
}

func corsHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		addCORSHeaders(w, r)
		handler.ServeHTTP(w, r)
	})
}

func corsHandlerFunc(handlerFunc http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		addCORSHeaders(w, r)
		handlerFunc(w, r)
	}
}

func addCORSHeaders(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers for the preflight request
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.Header().Set(
			"Access-Control-Allow-Headers",
			"Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Access-Control-Allow-Origin, Access-Control-Allow-Methods, Access-Control-Allow-Credentials")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Set CORS headers for the main request.
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

func writeJSONResponse(
	w http.ResponseWriter,
	r *http.Request,
	statusCode int,
	body interface{}) {

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(body)
}
