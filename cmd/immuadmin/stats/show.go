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

package stats

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"time"
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

func ShowMetricsRaw(serverAddress string) error {
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

func ShowMetricsAsText(serverAddress string) error {
	loader := newMetricsLoader(metricsURL(serverAddress))
	ms, err := loader.Load()
	if err != nil {
		return err
	}

	const labelLength = 27
	const strPattern = "%-*s:\t%s\n"
	const intPattern = "%-*s:\t%d\n"

	// print DB info
	fmt.Printf(strPattern, labelLength, "Database path", ms.db.name)
	uptime, _ := time.ParseDuration(fmt.Sprintf("%.4fh", ms.db.uptimeHours))
	fmt.Printf(strPattern, labelLength, "Uptime", uptime)
	fmt.Printf(intPattern, labelLength, "Number of entries", ms.db.nbEntries)
	lsmSizeS, _ := byteCountBinary(ms.db.lsmBytes)
	vlogSizeS, _ := byteCountBinary(ms.db.vlogBytes)
	totalSizeS, _ := byteCountBinary(ms.db.totalBytes)
	fmt.Printf(strPattern, labelLength, "LSM size", lsmSizeS)
	fmt.Printf(strPattern, labelLength, "VLog size", vlogSizeS)
	fmt.Printf(strPattern, labelLength, "Total size", totalSizeS)

	// print clients
	fmt.Printf(intPattern, labelLength, "Number of clients", ms.nbClients)
	fmt.Printf(strPattern, labelLength, "Queries per client", "")
	for k, v := range ms.nbRPCsPerClient {
		fmt.Printf("   "+intPattern, labelLength-3, k, v)
		if lastMsgAt, ok := ms.lastMsgAtPerClient[k]; ok {
			ago := time.Since(time.Unix(int64(lastMsgAt), 0))
			fmt.Printf("      "+strPattern, labelLength-6, "Last query", fmt.Sprintf("%s ago", ago))
		}
	}

	// print durations
	if ms.isHistogramsDataAvailable() {
		keys := make([]string, 0, len(ms.durationRPCsByMethod))
		for k := range ms.durationRPCsByMethod {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Printf(strPattern, labelLength, "Avg. duration (nb calls)", "µs")
		for _, k := range keys {
			rd := ms.durationRPCsByMethod[k]
			lbl := fmt.Sprintf("%s (%d)", rd.method, rd.counter)
			fmt.Printf("   "+strPattern, labelLength-3, lbl, fmt.Sprintf("%.0f", rd.avgDuration*1000_000))
		}
	}

	return nil
}

func ShowMetricsVisually(serverAddress string) error {
	return runUI(newMetricsLoader(metricsURL(serverAddress)))
}
