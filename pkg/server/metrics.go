/*
Copyright 2019 vChain, Inc.

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

package server

import (
	"expvar"
	"net/http"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	http.Handle("/metrics", promhttp.Handler())
	expvarCollector := prometheus.NewExpvarCollector(map[string]*prometheus.Desc{
		"badger_blocked_puts_total":   prometheus.NewDesc("immud_blocked_puts_total", "Blocked Puts", nil, nil),
		"badger_disk_reads_total":     prometheus.NewDesc("immud_disk_reads_total", "Disk Reads", nil, nil),
		"badger_disk_writes_total":    prometheus.NewDesc("immud_disk_writes_total", "Disk Writes", nil, nil),
		"badger_gets_total":           prometheus.NewDesc("immud_gets_total", "Gets", nil, nil),
		"badger_puts_total":           prometheus.NewDesc("immud_puts_total", "Puts", nil, nil),
		"badger_memtable_gets_total":  prometheus.NewDesc("immud_memtable_gets_total", "Memtable gets", nil, nil),
		"badger_lsm_size_bytes":       prometheus.NewDesc("immud_lsm_size_bytes", "LSM Size in bytes", []string{"database"}, nil),
		"badger_vlog_size_bytes":      prometheus.NewDesc("immud_vlog_size_bytes", "Value Log Size in bytes", []string{"database"}, nil),
		"badger_pending_writes_total": prometheus.NewDesc("immud_pending_writes_total", "Pending Writes", []string{"database"}, nil),
		"badger_read_bytes":           prometheus.NewDesc("immud_read_bytes", "Read bytes", nil, nil),
		"badger_written_bytes":        prometheus.NewDesc("immud_written_bytes", "Written bytes", nil, nil),
		"badger_lsm_bloom_hits_total": prometheus.NewDesc("immud_lsm_bloom_hits_total", "LSM Bloom Hits", []string{"level"}, nil),
		"badger_lsm_level_gets_total": prometheus.NewDesc("immud_lsm_level_gets_total", "LSM Level Gets", []string{"level"}, nil),
	})
	prometheus.MustRegister(expvarCollector)
}

// StartMetrics listens and servers the HTTP metrics server in a new goroutine.
// The server is then returned and can be stopped using Close().
func StartMetrics(addr string, l logger.Logger) *http.Server {
	// expvar package adds a handler in to the default HTTP server (which has to be started explicitly),
	// and serves up the metrics at the /debug/vars endpoint.
	// Here we're registering both expvar and promhttp handlers in our custom server.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/debug/vars", expvar.Handler())
	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				l.Infof("Metrics http server closed")
			} else {
				l.Errorf("Metrics error: %s", err)
			}

		}
	}()

	return server
}
