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

package benchmarks

import (
	"fmt"

	"github.com/prometheus/procfs"
)

// HWStats contains basic information about hardware properties
type HWStats struct {
	CPUTime               float64 `json:"cpuTime"`               // Total about of CPU time used by the process in seconds
	CPUKernelTimeFraction float64 `json:"cpuTimeKernelFraction"` // Fraction of the total CPU time that was spent on the kernel side
	VMM                   uint64  `json:"vmm"`                   // Virtual memory used
	RSS                   uint64  `json:"rss"`                   // Resident memory used
	IOBytesRead           uint64  `json:"ioBytesRead"`           // Number of bytes read
	IOBytesWrite          uint64  `json:"ioBytesWrite"`          // Number of bytes written
	IOCallsRead           uint64  `json:"ioCallsRead"`           // Number of io read syscalls
	IOCallsWrite          uint64  `json:"ioCallsWrite"`          // Number of io write syscalls
}

func (h *HWStats) String() string {
	return fmt.Sprintf(
		"CPUTime: %.2f, VMM: %s, RSS: %s, Writes (bytes/calls): %s/%d, Reads (bytes/calls): %s/%d",
		h.CPUTime,
		ToHumanReadable(h.VMM),
		ToHumanReadable(h.RSS),
		ToHumanReadable(h.IOBytesWrite),
		h.IOCallsWrite,
		ToHumanReadable(h.IOBytesRead),
		h.IOCallsRead,
	)
}

type HWStatsProber struct {
	ps          procfs.Proc
	initialStat procfs.ProcStat
	initialIO   procfs.ProcIO
}

func NewHWStatsProber() (*HWStatsProber, error) {
	ps, err := procfs.Self()
	if err != nil {
		return nil, fmt.Errorf("Couldn't initialize HW stats prober: %v", err)
	}

	initialStat, err := ps.Stat()
	if err != nil {
		return nil, fmt.Errorf("Couldn't initialize HW stats prober: %v", err)
	}

	initialIO, err := ps.IO()
	if err != nil {
		return nil, fmt.Errorf("Couldn't initialize HW stats prober: %v", err)
	}

	return &HWStatsProber{
		ps:          ps,
		initialStat: initialStat,
		initialIO:   initialIO,
	}, nil
}

func (h *HWStatsProber) GetHWStats() (*HWStats, error) {
	stat, err := h.ps.Stat()
	if err != nil {
		return nil, err
	}

	uTime := stat.UTime - h.initialStat.UTime
	sTime := stat.STime - h.initialStat.STime
	ktFrac := float64(0)
	if uTime+sTime > 0 {
		ktFrac = float64(sTime) / float64(sTime+uTime)
	}

	ioStat, err := h.ps.IO()
	if err != nil {
		return nil, err
	}

	return &HWStats{
		CPUTime:               stat.CPUTime() - h.initialStat.CPUTime(),
		CPUKernelTimeFraction: ktFrac,
		VMM:                   uint64(stat.VirtualMemory()),
		RSS:                   uint64(stat.ResidentMemory()),
		IOBytesRead:           ioStat.ReadBytes - h.initialIO.ReadBytes,
		IOBytesWrite:          ioStat.WriteBytes - h.initialIO.WriteBytes,
		IOCallsRead:           ioStat.SyscR - h.initialIO.SyscR,
		IOCallsWrite:          ioStat.SyscW - h.initialIO.SyscW,
	}, nil
}
