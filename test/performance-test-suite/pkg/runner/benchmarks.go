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
	"github.com/codenotary/immudb/test/performance-test-suite/pkg/benchmarks"
	"github.com/codenotary/immudb/test/performance-test-suite/pkg/benchmarks/writetxs"
)

func getBenchmarksToRun() []benchmarks.Benchmark {
	return []benchmarks.Benchmark{
		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write TX/s async - no replicas",
			Workers:    30,
			BatchSize:  1,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: true,
			Replica:    "",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write KV/s async - no replicas",
			Workers:    30,
			BatchSize:  1000,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: true,
			Replica:    "",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write TX/s async - one async replica",
			Workers:    30,
			BatchSize:  1,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: true,
			Replica:    "async",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write KV/s async - one async replica",
			Workers:    30,
			BatchSize:  1000,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: true,
			Replica:    "async",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write TX/s async - one sync replica",
			Workers:    30,
			BatchSize:  1,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: true,
			Replica:    "sync",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write KV/s async - one sync replica",
			Workers:    30,
			BatchSize:  1000,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: true,
			Replica:    "sync",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write TX/s sync - no replicas",
			Workers:    30,
			BatchSize:  1,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: false,
			Replica:    "",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write KV/s sync - no replicas",
			Workers:    30,
			BatchSize:  1000,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: false,
			Replica:    "",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write TX/s sync - one async replica",
			Workers:    30,
			BatchSize:  1,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: false,
			Replica:    "async",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write KV/s sync - one async replica",
			Workers:    30,
			BatchSize:  1000,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: false,
			Replica:    "async",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write TX/s sync - one sync replica",
			Workers:    30,
			BatchSize:  1,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: false,
			Replica:    "sync",
		}),

		writetxs.NewBenchmark(writetxs.Config{
			Name:       "Write KV/s sync - one sync replica",
			Workers:    30,
			BatchSize:  1000,
			KeySize:    32,
			ValueSize:  128,
			AsyncWrite: false,
			Replica:    "sync",
		}),
	}
}
