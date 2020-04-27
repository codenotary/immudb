package statisticscmd

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"time"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/client"
	dto "github.com/prometheus/client_model/go"
	"github.com/spf13/cobra"
)

const requestTimeout = 3 * time.Second

func metricsURL(serverAddress string) string {
	return "http://" + serverAddress + ":9497/metrics"
}

func newHttpClient() *http.Client {
	return &http.Client{
		Timeout: requestTimeout,
	}
}

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

type textMetrics struct {
	durationRPCsByMethod map[string]rpcDuration
	nbClients            int
	nbRPCsPerClient      map[string]uint64
	db                   dbInfo
}

func (ms *textMetrics) withClients(metricsFamilies *map[string]*dto.MetricFamily) {
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

func (ms *textMetrics) withDBInfo(metricsFamilies *map[string]*dto.MetricFamily) {
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

func (ms *textMetrics) withDuration(metricsFamilies *map[string]*dto.MetricFamily) {
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

func showMetricsRaw(serverAddress string) error {
	resp, err := newHttpClient().Get(metricsURL(serverAddress))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Println(string(body))
	return nil
}

func showMetricsAsText(serverAddress string) error {
	loader := newMetricsLoader(metricsURL(serverAddress))
	metricsFamilies, err := loader.Load()
	if err != nil {
		return err
	}

	ms := &textMetrics{}
	ms.withDBInfo(metricsFamilies)
	ms.withClients(metricsFamilies)
	ms.withDuration(metricsFamilies)

	const labelLength = 27
	const strPattern = "%-*s:\t%s\n"
	const intPattern = "%-*s:\t%d\n"

	// print DB info
	fmt.Printf(strPattern, labelLength, "Database path", ms.db.name)
	fmt.Printf(intPattern, labelLength, "Number of entries", ms.db.nbEntries)
	fmt.Printf(intPattern, labelLength, "LSM size (bytes)", ms.db.lsmBytes)
	fmt.Printf(intPattern, labelLength, "Vlog size (bytes)", ms.db.vlogBytes)
	fmt.Printf(intPattern, labelLength, "Total size (bytes)", ms.db.totalBytes)

	// print clients
	fmt.Printf(intPattern, labelLength, "Number of clients", ms.nbClients)
	fmt.Printf(strPattern, labelLength, "Queries per client", "---")
	for k, v := range ms.nbRPCsPerClient {
		fmt.Printf("   "+intPattern, labelLength-3, k, v)
	}

	// print durations
	keys := make([]string, 0, len(ms.durationRPCsByMethod))
	for k := range ms.durationRPCsByMethod {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf(strPattern, labelLength, "Avg. latency (µsec)", "---")
	for _, k := range keys {
		rd := ms.durationRPCsByMethod[k]
		lbl := fmt.Sprintf("%s (%d calls)", rd.method, rd.counter)
		fmt.Printf("   "+intPattern, labelLength-3, lbl, uint64(rd.averageDuration*1000_000))
	}

	return nil
}

func showMetricsVisually(serverAddress string, memStats bool) error {
	return runUI(newMetricsLoader(metricsURL(serverAddress)), memStats)
}

func NewCommand(optionsFunc func(cmd *cobra.Command) (*client.Options, error)) *cobra.Command {
	cmd := cobra.Command{
		Use:     "statistics",
		Short:   fmt.Sprintf("Show statistics"),
		Aliases: []string{"s"},
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := optionsFunc(cmd)
			if err != nil {
				c.QuitToStdErr(err)
			}
			raw, err := cmd.Flags().GetBool("raw")
			if err != nil {
				c.QuitToStdErr(err)
			}
			if raw {
				if err := showMetricsRaw(options.Address); err != nil {
					c.QuitToStdErr(err)
				}
				return nil
			}
			visual, err := cmd.Flags().GetBool("visual")
			if err != nil {
				c.QuitToStdErr(err)
			}
			if !visual {
				if err := showMetricsAsText(options.Address); err != nil {
					c.QuitToStdErr(err)
				}
				return nil
			}
			memStats, err := cmd.Flags().GetBool("memory")
			if err != nil {
				c.QuitToStdErr(err)
			}
			if err := showMetricsVisually(options.Address, memStats); err != nil {
				c.QuitToStdErr(err)
			}
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.Flags().BoolP("raw", "r", false, "show raw statistics")
	cmd.Flags().BoolP("visual", "v", false, "show a visual representation of statistics as a dashboard with evolving charts")
	cmd.Flags().BoolP("memory", "m", false, "show memory statistics (works only with the 'visual' option)")
	return &cmd
}
