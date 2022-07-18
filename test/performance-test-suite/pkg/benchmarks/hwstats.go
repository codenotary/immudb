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
}

func (h *HWStats) String() string {
	return fmt.Sprintf(
		"CPUTime: %.2f, CPUKernelFrac: %.2f, VMM: %d, RSS: %d",
		h.CPUTime,
		h.CPUKernelTimeFraction,
		h.VMM,
		h.RSS,
	)
}

type HWStatsGatherer struct {
	ps           procfs.Proc
	startCPUTime float64
	startUTime   uint
	startSTime   uint
}

func NewHWStatsGatherer() (*HWStatsGatherer, error) {
	ps, err := procfs.Self()
	if err != nil {
		return nil, err
	}

	initialStats, err := ps.Stat()

	return &HWStatsGatherer{
		ps:           ps,
		startCPUTime: initialStats.CPUTime(),
		startUTime:   initialStats.UTime,
		startSTime:   initialStats.STime,
	}, nil
}

func (h *HWStatsGatherer) GetHWStats() (*HWStats, error) {
	stat, err := h.ps.Stat()
	if err != nil {
		return nil, err
	}

	uTime := stat.UTime - h.startUTime
	sTime := stat.STime - h.startSTime
	ktFrac := float64(0)
	if uTime+sTime > 0 {
		ktFrac = float64(sTime) / float64(sTime+uTime)
	}

	return &HWStats{
		CPUTime:               stat.CPUTime() - h.startCPUTime,
		CPUKernelTimeFraction: ktFrac,
		VMM:                   uint64(stat.VirtualMemory()),
		RSS:                   uint64(stat.ResidentMemory()),
	}, nil
}
