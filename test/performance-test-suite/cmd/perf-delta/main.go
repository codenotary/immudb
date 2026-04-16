/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

// perf-delta compares two perf-test-suite JSON result files and reports the
// per-benchmark delta on TX/s and KV/s. It exits non-zero when any benchmark
// regresses by more than the configured tolerance, making it usable as a
// regression gate in CI.
//
// Usage:
//
//	perf-delta -baseline baseline.json -current current.json [-tolerance 0.05]
//
// The schema is defined by runner.BenchmarkSuiteResult; we intentionally
// decode just the fields we need so the tool stays stable against
// forward-compatible additions to the result format.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

type result struct {
	Txs float64 `json:"txs"`
	Kvs float64 `json:"kvs"`
}

type benchmark struct {
	Name    string  `json:"name"`
	Results *result `json:"results"`
}

type suite struct {
	Benchmarks []benchmark `json:"benchmarks"`
}

func loadSuite(path string) (*suite, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var s suite
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &s, nil
}

// delta returns the fractional change from base to curr. If base is zero it
// returns 0 so an empty baseline doesn't count every benchmark as a
// regression or improvement.
func delta(base, curr float64) float64 {
	if base == 0 {
		return 0
	}
	return (curr - base) / base
}

func main() {
	baseline := flag.String("baseline", "", "path to baseline perf-test-suite JSON result")
	current := flag.String("current", "", "path to current perf-test-suite JSON result")
	tolerance := flag.Float64("tolerance", 0.05, "max allowed fractional regression per metric (0.05 = 5%)")
	flag.Parse()

	if *baseline == "" || *current == "" {
		fmt.Fprintln(os.Stderr, "both -baseline and -current are required")
		flag.Usage()
		os.Exit(2)
	}

	base, err := loadSuite(*baseline)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}
	cur, err := loadSuite(*current)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}

	byName := make(map[string]*result, len(base.Benchmarks))
	for i := range base.Benchmarks {
		byName[base.Benchmarks[i].Name] = base.Benchmarks[i].Results
	}

	// Stable markdown output; CI can attach this directly to a PR comment.
	fmt.Println("| Benchmark | Baseline TX/s | Current TX/s | Δ TX/s | Baseline KV/s | Current KV/s | Δ KV/s |")
	fmt.Println("|-----------|--------------:|-------------:|-------:|--------------:|-------------:|-------:|")

	regressions := 0
	missingCurr := 0

	for _, b := range cur.Benchmarks {
		baseRes, ok := byName[b.Name]
		if !ok || baseRes == nil || b.Results == nil {
			fmt.Printf("| %s | (new / not in baseline) | | | | | |\n", b.Name)
			continue
		}

		dTxs := delta(baseRes.Txs, b.Results.Txs)
		dKvs := delta(baseRes.Kvs, b.Results.Kvs)

		marker := func(d float64) string {
			if d <= -*tolerance {
				return " ❌"
			}
			return ""
		}

		fmt.Printf("| %s | %.2f | %.2f | %+.2f%%%s | %.2f | %.2f | %+.2f%%%s |\n",
			b.Name,
			baseRes.Txs, b.Results.Txs, dTxs*100, marker(dTxs),
			baseRes.Kvs, b.Results.Kvs, dKvs*100, marker(dKvs),
		)

		if dTxs <= -*tolerance || dKvs <= -*tolerance {
			regressions++
		}
	}

	for name := range byName {
		found := false
		for _, b := range cur.Benchmarks {
			if b.Name == name {
				found = true
				break
			}
		}
		if !found {
			fmt.Printf("| %s | (missing in current) | | | | | |\n", name)
			missingCurr++
		}
	}

	fmt.Printf("\n%d regression(s) beyond %.1f%% tolerance; %d benchmark(s) missing in current.\n",
		regressions, *tolerance*100, missingCurr)

	if regressions > 0 {
		os.Exit(1)
	}
}
