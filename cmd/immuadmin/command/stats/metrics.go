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

package stats

import (
	"fmt"
	"math"
	"time"

	dto "github.com/prometheus/client_model/go"
)

const SystemdbName = "systemdb"

var readers = map[string]bool{
	"ByIndex":     true,
	"ByIndexSV":   true,
	"Consistency": true,
	"Count":       true,
	"CurrentRoot": true,
	"Dump":        true,
	"Get":         true,
	"GetBatch":    true,
	"GetBatchSV":  true,
	"GetSV":       true,
	"Health":      true,
	"History":     true,
	"HistorySV":   true,
	"IScan":       true,
	"IScanSV":     true,
	"Inclusion":   true,
	"Login":       true,
	"SafeGet":     true,
	"SafeGetSV":   true,
	"Scan":        true,
	"ScanSV":      true,
	"ZScan":       true,
	"ZScanSV":     true,
}

var writers = map[string]bool{
	"Reference":     true,
	"SafeReference": true,
	"SafeSet":       true,
	"SafeSetSV":     true,
	"SafeZAdd":      true,
	"Set":           true,
	"SetBatch":      true,
	"SetBatchSV":    true,
	"SetSV":         true,
	"ZAdd":          true,
}

type rpcDuration struct {
	method        string
	counter       uint64
	totalDuration float64
	avgDuration   float64
}

type dbInfo struct {
	name       string
	totalBytes uint64
	nbEntries  uint64
}

type operations struct {
	counter     uint64
	duration    float64
	avgDuration float64
}

type memstats struct {
	sysBytes        uint64
	heapAllocBytes  uint64
	heapIdleBytes   uint64
	heapInUseBytes  uint64
	stackInUseBytes uint64
}

type metrics struct {
	durationRPCsByMethod map[string]rpcDuration
	reads                operations
	writes               operations
	nbClients            int
	nbRPCsPerClient      map[string]uint64
	lastMsgAtPerClient   map[string]uint64
	uptimeHours          float64
	dbs                  map[string]dbInfo
	memstats             memstats
}

func (ms *metrics) isHistogramsDataAvailable() bool {
	return len(ms.durationRPCsByMethod) > 0
}

func (ms *metrics) clientsActiveDuringLastHour() *map[string]time.Time {
	r := map[string]time.Time{}
	for ip, lastMsgAt := range ms.lastMsgAtPerClient {
		t := time.Unix(int64(lastMsgAt), 0)
		ago := time.Since(t)
		if ago.Hours() < 1 {
			r[ip] = t
		}
	}
	return &r
}

func (ms *metrics) populateFrom(metricsFamilies *map[string]*dto.MetricFamily) {
	ms.withDBInfo(metricsFamilies)
	ms.withClients(metricsFamilies)
	ms.withDuration(metricsFamilies)
	ms.withMemStats(metricsFamilies)
}

func (ms *metrics) withClients(metricsFamilies *map[string]*dto.MetricFamily) {
	ms.nbRPCsPerClient = map[string]uint64{}
	clientsMetrics := (*metricsFamilies)["immudb_number_of_rpcs_per_client"].GetMetric()
	ms.nbClients = len(clientsMetrics)
	for _, m := range clientsMetrics {
		var ip string
		for _, labelPair := range m.GetLabel() {
			if labelPair.GetName() == "ip" {
				ip = labelPair.GetValue()
				break
			}
		}
		ms.nbRPCsPerClient[ip] = uint64(m.GetCounter().GetValue())
	}

	ms.lastMsgAtPerClient = map[string]uint64{}
	lastMsgAtMetrics := (*metricsFamilies)["immudb_clients_last_message_at_unix_seconds"].GetMetric()
	for _, m := range lastMsgAtMetrics {
		var ip string
		for _, labelPair := range m.GetLabel() {
			if labelPair.GetName() == "ip" {
				ip = labelPair.GetValue()
				break
			}
		}
		ms.lastMsgAtPerClient[ip] = uint64(m.GetGauge().GetValue())
	}
}

func getGaugeVecPerLabel(metrics []*dto.Metric, label string, out *map[string]uint64) {
	for _, m := range metrics {
		var labelValue string
		for _, labelPair := range m.GetLabel() {
			if labelPair.GetName() == "db" {
				labelValue = labelPair.GetValue()
				break
			}
		}
		(*out)[labelValue] = uint64(m.GetGauge().GetValue())
	}
}

func (ms *metrics) withDBInfo(metricsFamilies *map[string]*dto.MetricFamily) {
	// Uptime hours
	upHoursMetricsFams := (*metricsFamilies)["immudb_uptime_hours"]
	if upHoursMetricsFams != nil && len(upHoursMetricsFams.GetMetric()) > 0 {
		ms.uptimeHours = upHoursMetricsFams.GetMetric()[0].GetCounter().GetValue()
	}

	// DB sizes
	dbSizes := make(map[string]uint64)
	getGaugeVecPerLabel(
		(*metricsFamilies)["immudb_db_size_bytes"].GetMetric(),
		"db",
		&dbSizes)

	// Number of entries
	nbsEntries := make(map[string]uint64)
	getGaugeVecPerLabel(
		(*metricsFamilies)["immudb_number_of_stored_entries"].GetMetric(),
		"db",
		&nbsEntries)

	// aggregate all metrics to db info structs
	dbInfos := make(map[string]dbInfo, int(math.Max(float64(len(dbSizes)), float64(len(nbsEntries)))))
	for db, dbSize := range dbSizes {
		currDBInfo := dbInfos[db]
		currDBInfo.name = db
		currDBInfo.totalBytes = dbSize
		dbInfos[db] = currDBInfo
	}
	for db, nbEntries := range nbsEntries {
		currDBInfo := dbInfos[db]
		currDBInfo.name = db
		currDBInfo.nbEntries = nbEntries
		dbInfos[db] = currDBInfo
	}

	ms.dbs = dbInfos
}

func (ms *metrics) withDuration(metricsFamilies *map[string]*dto.MetricFamily) {
	ms.durationRPCsByMethod = map[string]rpcDuration{}
	for _, m := range (*metricsFamilies)["grpc_server_handling_seconds"].GetMetric() {
		var method string
		for _, labelPair := range m.GetLabel() {
			if labelPair.GetName() == "grpc_method" {
				method = labelPair.GetValue()
				break
			}
		}
		h := m.GetHistogram()
		c := h.GetSampleCount()
		td := h.GetSampleSum()
		var ad float64
		if c != 0 {
			ad = td / float64(c)
		}
		d := rpcDuration{
			method:        method,
			counter:       c,
			totalDuration: td,
			avgDuration:   ad,
		}
		ms.durationRPCsByMethod[method] = d
		if _, ok := readers[method]; ok {
			ms.reads.counter += d.counter
			ms.reads.duration += d.avgDuration
		}
		if _, ok := writers[method]; ok {
			ms.writes.counter += d.counter
			ms.writes.duration += d.totalDuration
		}
	}
	if ms.reads.counter > 0 {
		ms.reads.avgDuration = ms.reads.duration / float64(ms.reads.counter)
	}
	if ms.writes.counter > 0 {
		ms.writes.avgDuration = ms.writes.duration / float64(ms.writes.counter)
	}
}

func (ms *metrics) withMemStats(metricsFamilies *map[string]*dto.MetricFamily) {
	if sysBytesMetric := (*metricsFamilies)["go_memstats_sys_bytes"]; sysBytesMetric != nil {
		ms.memstats.sysBytes = uint64(*sysBytesMetric.GetMetric()[0].GetGauge().Value)
	}
	if heapAllocMetric := (*metricsFamilies)["go_memstats_heap_alloc_bytes"]; heapAllocMetric != nil {
		ms.memstats.heapAllocBytes = uint64(*heapAllocMetric.GetMetric()[0].GetGauge().Value)
	}
	if heapIdleMetric := (*metricsFamilies)["go_memstats_heap_idle_bytes"]; heapIdleMetric != nil {
		ms.memstats.heapIdleBytes = uint64(*heapIdleMetric.GetMetric()[0].GetGauge().Value)
	}
	if heapInUseMetric := (*metricsFamilies)["go_memstats_heap_inuse_bytes"]; heapInUseMetric != nil {
		ms.memstats.heapInUseBytes = uint64(*heapInUseMetric.GetMetric()[0].GetGauge().Value)
	}
	if stackInUseMetric := (*metricsFamilies)["go_memstats_stack_inuse_bytes"]; stackInUseMetric != nil {
		ms.memstats.stackInUseBytes = uint64(*stackInUseMetric.GetMetric()[0].GetGauge().Value)
	}
}

func (ms *metrics) dbWithMostEntries() dbInfo {
	var db dbInfo
	for _, currentDB := range ms.dbs {
		if (len(db.name) == 0 || currentDB.nbEntries > db.nbEntries) &&
			// skip system db
			currentDB.name != SystemdbName {
			db = currentDB
		}
	}
	return db
}

func byteCountBinary(b uint64) (string, float64) {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b), float64(b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	v := float64(b) / float64(div)
	return fmt.Sprintf("%.1f %cB", v, "kMGTPE"[exp]), v
}
