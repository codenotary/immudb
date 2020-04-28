package statisticscmd

import dto "github.com/prometheus/client_model/go"

type rpcDuration struct {
	method          string
	counter         uint64
	totalDuration   float64
	averageDuration float64
}

type dbInfo struct {
	name       string
	lsmBytes   uint64
	vlogBytes  uint64
	totalBytes uint64
	nbEntries  uint64
}

type metrics struct {
	durationRPCsByMethod map[string]rpcDuration
	nbClients            int
	nbRPCsPerClient      map[string]uint64
	db                   dbInfo
}

func (ms *metrics) populateFrom(metricsFamilies *map[string]*dto.MetricFamily) {
	ms.withDBInfo(metricsFamilies)
	ms.withClients(metricsFamilies)
	ms.withDuration(metricsFamilies)
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
}

func (ms *metrics) withDBInfo(metricsFamilies *map[string]*dto.MetricFamily) {
	lsmSizeMetric := (*metricsFamilies)["immudb_lsm_size_bytes"].GetMetric()[0]
	lsmBytes := lsmSizeMetric.GetUntyped().GetValue()
	vlogBytes := (*metricsFamilies)["immudb_vlog_size_bytes"].GetMetric()[0].GetUntyped().GetValue()
	ms.db.lsmBytes = uint64(lsmBytes)
	ms.db.vlogBytes = uint64(vlogBytes)
	ms.db.totalBytes = uint64(lsmBytes + vlogBytes)
	for _, labelPair := range lsmSizeMetric.GetLabel() {
		if labelPair.GetName() == "database" {
			ms.db.name = labelPair.GetValue()
			break
		}
	}
	ms.db.nbEntries = uint64((*metricsFamilies)["immudb_number_of_stored_entries"].GetMetric()[0].GetCounter().GetValue())
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
			method:          method,
			counter:         c,
			totalDuration:   td,
			averageDuration: ad,
		}
		ms.durationRPCsByMethod[method] = d
	}
}
