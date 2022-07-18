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
package benchmarks

import "time"

type Benchmark interface {

	// Get benchmark's name
	Name() string

	// Do a test warmup
	Warmup() error

	// Cleanup after the test
	Cleanup() error

	// Run the test, return the cumulative statistics after the whole run
	Run(duration time.Duration, seed uint64) (interface{}, error)

	// Gather current snapshot of probes
	// This should be delta since the previous probe - e.g. req/sec that happened
	// between this and previous run.
	// It will be called in parallel while the `Run` call is still ongoing
	Probe() interface{}
}
