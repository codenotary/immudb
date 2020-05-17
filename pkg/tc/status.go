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

package tc

import (
	"github.com/codenotary/immudb/pkg/logger"
	"net/http"
)

// StartMetrics listens and servers the HTTP metrics server in a new goroutine.
// The server is then returned and can be stopped using Close().
func StartMetrics(
	addr string,
	l logger.Logger,
) *http.Server {

	mux := http.NewServeMux()
	statusHandler := NewStatusHandler()
	mux.HandleFunc("/status", statusHandler.Get)
	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				l.Infof("Trust checker http server closed")
			} else {
				l.Errorf("Trust checker error: %s", err)
			}

		}
	}()

	return server
}

type StatusHandler interface {
	Get(w http.ResponseWriter, r *http.Request)
}

type statusHandler struct{}

func NewStatusHandler() StatusHandler {
	return &statusHandler{}
}

func (h *statusHandler) Get(w http.ResponseWriter, r *http.Request) {

}
