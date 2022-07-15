/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
package runner

import (
	"log"
	"time"

	"github.com/codenotary/immudb/test/performance-test-suite/pkg/benchmarks"
	"github.com/codenotary/immudb/test/performance-test-suite/pkg/benchmarks/writetxs"
)

func getBenchmarksToRun() []benchmarks.Benchmark {
	return []benchmarks.Benchmark{
		writetxs.NewBenchmark(writetxs.DefaultConfig),
	}
}

func RunAllBenchmarks(d time.Duration) (*BenchmarkSuiteResult, error) {
	ret := &BenchmarkSuiteResult{
		StartTime:   time.Now(),
		ProcessInfo: gatherProcessInfo(),
		SystemInfo:  gatherSystemInfo(),
	}

	log.Printf("Starting immudb performance test suite")

	for _, b := range getBenchmarksToRun() {

		log.Printf("Running benchmark: %s", b.Name())

		result := BenchmarkRunResult{
			Name:     b.Name(),
			Timeline: []BenchmarkTimelineEntry{},
		}

		err := b.Warmup()
		if err != nil {
			return nil, err
		}

		result.StartTime = time.Now()

		// Start probing goroutine
		done := make(chan bool)
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			for {

				select {
				case <-done:
					return
				case <-ticker.C:
				}

				now := time.Now()
				probe := b.Probe()
				result.Timeline = append(result.Timeline, BenchmarkTimelineEntry{
					Time:     now,
					Duration: Duration(now.Sub(result.StartTime)),
					Probe:    probe,
				})

				log.Printf(
					"[%s] %v/%v %s",
					result.Name,
					now.Sub(result.StartTime).Round(time.Second),
					d,
					probe,
				)
			}
		}()

		// Run the benchmark
		res, err := b.Run(d)
		if err != nil {
			return nil, err
		}

		// Notify that we're done probing
		close(done)

		result.EndTime = time.Now()
		result.Duration = Duration(result.EndTime.Sub(result.StartTime))
		result.RequestedDuration = Duration(d)
		result.Results = res

		ret.Benchmarks = append(ret.Benchmarks, result)

		log.Printf("Benchmark %s finished", b.Name())
		log.Printf("Results: %s", res)
	}

	ret.EndTime = time.Now()
	ret.Duration = Duration(ret.EndTime.Sub(ret.StartTime))

	log.Printf("Finished immudb performance test suite")
	return ret, nil
}
