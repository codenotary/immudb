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

package benchmarks

import (
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHwStatsProber(t *testing.T) {

	t.Run("check initial hw stats", func(t *testing.T) {
		sp, err := NewHWStatsProber()
		require.NoError(t, err)
		require.NotNil(t, sp)

		stats, err := sp.GetHWStats()
		require.NoError(t, err)
		require.NotNil(t, stats)

		require.GreaterOrEqual(t, stats.CPUTime, 0.0)
		require.Less(t, stats.CPUTime, 0.0001)

		require.GreaterOrEqual(t, stats.CPUKernelTimeFraction, 0.0)
		require.LessOrEqual(t, stats.CPUKernelTimeFraction, 1.0)

		str := stats.String()
		require.Regexp(t, `CPUTime: [0-9.]+`, str)
		require.Regexp(t, `VMM: [0-9.]+.?`, str)
		require.Regexp(t, `RSS: [0-9.]+.?`, str)
		require.Regexp(t, `Writes \(bytes\/calls\): [0-9.]+.?/[0-9.]+.?`, str)
		require.Regexp(t, `Reads \(bytes\/calls\): [0-9.]+.?/[0-9.]+.?`, str)
	})

	t.Run("check CPU stats during idle time", func(t *testing.T) {
		sp, err := NewHWStatsProber()
		require.NoError(t, err)
		require.NotNil(t, sp)

		time.Sleep(time.Millisecond * 20)

		stats, err := sp.GetHWStats()
		require.NoError(t, err)

		require.GreaterOrEqual(t, stats.CPUTime, 0.0)
		require.Less(t, stats.CPUTime, 0.0001)

		require.GreaterOrEqual(t, stats.CPUKernelTimeFraction, 0.0)
		require.LessOrEqual(t, stats.CPUKernelTimeFraction, 1.0)
	})

	t.Run("check CPU stats during busy time", func(t *testing.T) {
		sp, err := NewHWStatsProber()
		require.NoError(t, err)
		require.NotNil(t, sp)

		i := 0
		for t0 := time.Now(); time.Since(t0) < time.Millisecond*20; i++ {
			// Some busy loop
		}
		log.Println("Iterations count:", i)

		stats, err := sp.GetHWStats()
		require.NoError(t, err)

		require.GreaterOrEqual(t, stats.CPUTime, 0.0)
		require.GreaterOrEqual(t, stats.CPUTime, 0.01)

		require.GreaterOrEqual(t, stats.CPUKernelTimeFraction, 0.0)
		require.LessOrEqual(t, stats.CPUKernelTimeFraction, 1.0)
	})

	t.Run("check IO stats", func(t *testing.T) {
		sp, err := NewHWStatsProber()
		require.NoError(t, err)
		require.NotNil(t, sp)

		stats, err := sp.GetHWStats()
		require.NoError(t, err)
		require.LessOrEqual(t, stats.IOBytesRead, uint64(100))
		require.LessOrEqual(t, stats.IOBytesWrite, uint64(100))
		require.LessOrEqual(t, stats.IOCallsRead, uint64(10))
		require.LessOrEqual(t, stats.IOCallsWrite, uint64(10))

		dir := t.TempDir()
		fl, err := os.Create(filepath.Join(dir, "test"))
		require.NoError(t, err)

		const blockSize uint64 = 4096
		const blocks uint64 = 1000
		const blocksMargin uint64 = 100

		t.Run("test write stats", func(t *testing.T) {
			for i := uint64(0); i < blocks; i++ {
				n, err := fl.Write(make([]byte, blockSize))
				require.NoError(t, err)
				require.EqualValues(t, blockSize, n)
			}

			stats, err = sp.GetHWStats()
			require.NoError(t, err)

			require.GreaterOrEqual(t, stats.IOCallsWrite, blocks)
			require.Less(t, stats.IOCallsWrite, blocks+blocksMargin)

			require.GreaterOrEqual(t, stats.IOBytesWrite, blocks*blockSize)
			require.Less(t, stats.IOBytesWrite, (blocks+blocksMargin)*blockSize)
		})

		t.Run("test read stats", func(t *testing.T) {
			fl.Seek(0, 0)
			fl.Sync()
			for i := 0; uint64(i) < blocks; i++ {
				n, err := fl.Read(make([]byte, blockSize))
				require.NoError(t, err)
				require.EqualValues(t, blockSize, n)
			}

			stats, err = sp.GetHWStats()
			require.NoError(t, err)

			require.GreaterOrEqual(t, stats.IOCallsRead, blocks)
			require.Less(t, stats.IOCallsRead, blocks+blocksMargin)

			// We can not easily check the lower bound - most likely the whole test
			// will run entirely on page cache thus there will be 0 bytes read from the device
			require.Less(t, stats.IOBytesRead, (blocks+blocksMargin)*blockSize)
		})
	})

}
