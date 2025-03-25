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
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"
)

const requestTimeout = 3 * time.Second

func metricsURL(serverAddress string) string {
	if strings.HasPrefix(serverAddress, "http") {
		return serverAddress
	}
	return "http://" + serverAddress + ":9497/metrics"
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: requestTimeout,
	}
}

// ShowMetricsRaw ...
func ShowMetricsRaw(w io.Writer, serverAddress string) error {
	resp, err := newHTTPClient().Get(metricsURL(serverAddress))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, string(body)+"\n")
	return nil
}

// ShowMetricsAsText ...
func ShowMetricsAsText(w io.Writer, serverAddress string) error {
	loader := newMetricsLoader(metricsURL(serverAddress))
	ms, err := loader.Load()
	if err != nil {
		return err
	}

	db := ms.dbWithMostEntries()

	const labelLength = 27
	const strPattern = "%-*s:\t%s\n"
	const intPattern = "%-*s:\t%d\n"

	// print DB info
	fmt.Fprintf(w, strPattern, labelLength, "Database", db.name)
	uptime, _ := time.ParseDuration(fmt.Sprintf("%.4fh", ms.uptimeHours))
	fmt.Fprintf(w, strPattern, labelLength, "Uptime", uptime)
	fmt.Fprintf(w, intPattern, labelLength, "Entries", db.nbEntries)
	totalSizeS, _ := byteCountBinary(db.totalBytes)
	fmt.Fprintf(w, strPattern, labelLength, "Size", totalSizeS)

	// print clients
	fmt.Fprintf(w, intPattern, labelLength, "Number of clients", ms.nbClients)
	fmt.Fprintf(w, strPattern, labelLength, "Queries per client", "")
	for k, v := range ms.nbRPCsPerClient {
		fmt.Fprintf(w, "   "+intPattern, labelLength-3, k, v)
		if lastMsgAt, ok := ms.lastMsgAtPerClient[k]; ok {
			ago := time.Since(time.Unix(int64(lastMsgAt), 0))
			fmt.Fprintf(w, "      "+strPattern, labelLength-6, "Last query", fmt.Sprintf("%s ago", ago))
		}
	}

	// print durations
	if ms.isHistogramsDataAvailable() {
		keys := make([]string, 0, len(ms.durationRPCsByMethod))
		for k := range ms.durationRPCsByMethod {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintf(w, strPattern, labelLength, "Avg. duration (nb calls)", "µs")
		for _, k := range keys {
			rd := ms.durationRPCsByMethod[k]
			lbl := fmt.Sprintf("%s (%d)", rd.method, rd.counter)
			fmt.Fprintf(w, "   "+strPattern, labelLength-3, lbl, fmt.Sprintf("%.0f", rd.avgDuration*1000_000))
		}
	}

	return nil
}

// ShowMetricsVisually ...
func ShowMetricsVisually(serverAddress string) error {
	su := statsui{Loader: newMetricsLoader(metricsURL(serverAddress)), Tui: tui{}}
	return su.runUI(false)
}
