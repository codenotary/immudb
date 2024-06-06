/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package runner

import (
	"time"

	"github.com/codenotary/immudb/test/performance-test-suite/pkg/benchmarks/writetxs"
)

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte("\"" + time.Duration(d).String() + "\""), nil
}

func (d Duration) Seconds() float64 {
	return time.Duration(d).Seconds()
}

type BenchmarkTimelineEntry struct {
	Time     time.Time   `json:"time"`
	Duration Duration    `json:"duration"`
	Probe    interface{} `json:"probe"`
}

type BenchmarkRunResult struct {
	Name              string                   `json:"name"`
	Summary           string                   `json:"summary"`
	StartTime         time.Time                `json:"startTime"`
	EndTime           time.Time                `json:"endTime"`
	Duration          Duration                 `json:"duration"`
	RequestedDuration Duration                 `json:"requestedDuration"`
	Results           *writetxs.Result         `json:"results"`
	Timeline          []BenchmarkTimelineEntry `json:"timeline"`
}

type ProcessInfo struct {
	CommandLine []string `json:"commandLine"`
	Version     string   `json:"version"`
	GitCommit   string   `json:"gitCommit"`
	BuiltBy     string   `json:"builtBy"`
	BuiltAt     string   `json:"builtAt"`
}

type SystemInfo struct {
	Hostname string `json:"hostname"`
}

type BenchmarkSuiteResult struct {
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`
	Duration  Duration  `json:"duration"`

	ProcessInfo ProcessInfo `json:"processInfo"`
	SystemInfo  SystemInfo  `json:"systemInfo"`

	Benchmarks []BenchmarkRunResult `json:"benchmarks"`
}
