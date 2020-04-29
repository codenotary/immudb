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

package statisticscmd

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"time"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	ms := &metrics{}
	ms.populateFrom(metricsFamilies)

	const labelLength = 27
	const strPattern = "%-*s:\t%s\n"
	const intPattern = "%-*s:\t%d\n"

	// print DB info
	fmt.Printf(strPattern, labelLength, "Database path", ms.db.name)
	uptime, _ := time.ParseDuration(fmt.Sprintf("%.4fh", ms.db.uptimeHours))
	fmt.Printf(strPattern, labelLength, "Uptime", uptime)
	fmt.Printf(intPattern, labelLength, "Number of entries", ms.db.nbEntries)
	fmt.Printf(strPattern, labelLength, "LSM size", byteCountBinary(ms.db.lsmBytes))
	fmt.Printf(strPattern, labelLength, "VLog size", byteCountBinary(ms.db.vlogBytes))
	fmt.Printf(strPattern, labelLength, "Total size", byteCountBinary(ms.db.totalBytes))

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
	keys := make([]string, 0, len(ms.durationRPCsByMethod))
	for k := range ms.durationRPCsByMethod {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Printf(strPattern, labelLength, "Avg. latency (nb calls)", "µs")
	for _, k := range keys {
		rd := ms.durationRPCsByMethod[k]
		lbl := fmt.Sprintf("%s (%d)", rd.method, rd.counter)
		fmt.Printf("   "+strPattern, labelLength-3, lbl, fmt.Sprintf("%.0f", rd.avgDuration*1000_000))
	}

	return nil
}

func showMetricsVisually(serverAddress string) error {
	return runUI(newMetricsLoader(metricsURL(serverAddress)))
}

func NewCommand(optionsFunc func() *client.Options, immuClient *client.ImmuClient) *cobra.Command {
	cmd := cobra.Command{
		Use:     "statistics",
		Short:   fmt.Sprintf("Show statistics"),
		Aliases: []string{"s"},
		RunE: func(cmd *cobra.Command, args []string) error {
			//--> workaround to achieve auth (this command does HTTP requests which do not go through ImmuClient)
			_, err := (*immuClient).Get(context.Background(), []byte{255})
			if err != nil {
				s, ok := status.FromError(err)
				if !ok || s == nil || s.Code() != codes.NotFound {
					c.QuitWithUserError(err)
				}
			}
			//<--
			options := optionsFunc()
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
			if err := showMetricsVisually(options.Address); err != nil {
				c.QuitToStdErr(err)
			}
			return nil
		},
		Args: cobra.NoArgs,
	}
	cmd.Flags().BoolP("raw", "r", false, "show raw statistics")
	cmd.Flags().BoolP("visual", "v", false, "show a visual representation of statistics as a dashboard with evolving charts")
	return &cmd
}
