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
	"container/list"
	"fmt"
	"time"

	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
	dto "github.com/prometheus/client_model/go"
)

type Controller interface {
	Render(*map[string]*dto.MetricFamily)
	Resize()
}

type statsController struct {
	Grid                *ui.Grid
	SummaryTable        *widgets.Table
	SizePie             *widgets.PieChart
	NbReadsWritesPie    *widgets.PieChart
	AvgDurationPlot     *widgets.Plot
	AvgDurationPlotData []*list.List
	MemoryPlot          *widgets.Plot
	MemoryPlotData      []*list.List
}

func (p *statsController) Resize() {
	p.resize()
	ui.Render(p.Grid)
}

func (p *statsController) resize() {
	termWidth, termHeight := ui.TerminalDimensions()
	p.Grid.SetRect(0, 0, termWidth, termHeight)
}

func enqueueDequeue(l *list.List, v interface{}, max int) {
	if l.Len() >= max {
		l.Remove(l.Front())
	}
	l.PushBack(v)
}
func toFloats(l *list.List) []float64 {
	a := make([]float64, l.Len())
	i := 0
	for e := l.Front(); e != nil; e = e.Next() {
		a[i] = e.Value.(float64)
		i++
	}
	return a
}

func (p *statsController) Render(data *map[string]*dto.MetricFamily) {
	ms := &metrics{}
	ms.populateFrom(data)

	uptime, _ := time.ParseDuration(fmt.Sprintf("%.4fh", ms.db.uptimeHours))
	p.SummaryTable.Rows = [][]string{
		[]string{"[ImmuDB stats](mod:bold)", fmt.Sprintf("[ at %s](mod:bold)", time.Now().Format("15:04:05"))},
		[]string{"Database", ms.db.name},
		[]string{"Uptime", uptime.String()},
		[]string{"Entries", fmt.Sprintf("%d", ms.db.nbEntries)},
		[]string{"No. clients", fmt.Sprintf("%d", ms.nbClients)},
		[]string{"  active < 1h ago", fmt.Sprintf("%d", len(*ms.clientsActiveDuringLastHour()))},
	}

	p.SizePie.Title = fmt.Sprintf(" DB Size: %s ", byteCountBinary(ms.db.totalBytes))
	p.SizePie.LabelFormatter = func(idx int, _ float64) string {
		return []string{
			fmt.Sprintf("VLog: %s", byteCountBinary(ms.db.vlogBytes)),
			fmt.Sprintf("LSM: %s", byteCountBinary(ms.db.lsmBytes)),
		}[idx]
	}
	p.SizePie.Data = []float64{float64(ms.db.vlogBytes), float64(ms.db.lsmBytes)}

	p.NbReadsWritesPie.Title = fmt.Sprintf(" %d reads/writes ", ms.reads.counter+ms.writes.counter)
	p.NbReadsWritesPie.LabelFormatter = func(idx int, _ float64) string {
		return []string{
			fmt.Sprintf("%d reads", ms.reads.counter),
			fmt.Sprintf("%d writes", ms.writes.counter),
		}[idx]
	}
	p.NbReadsWritesPie.Data = []float64{float64(ms.reads.counter), float64(ms.writes.counter)}

	avgDurationReads := ms.reads.avgDuration * 1000_000
	avgDurationWrites := ms.writes.avgDuration * 1000_000
	p.AvgDurationPlot.Title = fmt.Sprintf(" Avg. duration: %.0f µs read, %.0f µs write ", avgDurationReads, avgDurationWrites)
	enqueueDequeue(p.AvgDurationPlotData[0], avgDurationReads, avgDurationPlotDataLength)
	enqueueDequeue(p.AvgDurationPlotData[1], avgDurationWrites, avgDurationPlotDataLength)
	p.AvgDurationPlot.Data = [][]float64{
		toFloats(p.AvgDurationPlotData[0]),
		toFloats(p.AvgDurationPlotData[1]),
	}

	memReserved := ms.memstats.sysBytes
	memInUse := ms.memstats.heapInUseBytes + ms.memstats.stackInUseBytes
	p.MemoryPlot.Title = fmt.Sprintf(" Memory: %s reserved, %s in use ", byteCountBinary(memReserved), byteCountBinary(memInUse))
	enqueueDequeue(p.MemoryPlotData[0], float64(memReserved/1000_000), memoryPlotDataLength)
	enqueueDequeue(p.MemoryPlotData[1], float64(memInUse/1000_000), memoryPlotDataLength)
	p.MemoryPlot.Data = [][]float64{
		toFloats(p.MemoryPlotData[0]),
		toFloats(p.MemoryPlotData[1]),
	}

	ui.Render(p.Grid)
}

var avgDurationPlotWidthPercent = .6
var avgDurationPlotDataLength int
var memoryPlotWidthPercent = 1.
var memoryPlotDataLength int

func (p *statsController) initUI() {
	p.resize()

	p.SummaryTable.Title = " Exit: q, Esc or Ctrl-C "
	p.SummaryTable.PaddingTop = 1

	p.SizePie.Title = " DB Size "
	p.SizePie.LabelFormatter = func(idx int, _ float64) string { return []string{"VLog", "LSM"}[idx] }
	p.SizePie.PaddingTop = 1
	p.SizePie.PaddingBottom = 1

	p.NbReadsWritesPie.Title = " Number of reads/writes "
	p.NbReadsWritesPie.LabelFormatter = func(idx int, _ float64) string { return []string{"reads", "writes"}[idx] }
	p.NbReadsWritesPie.PaddingTop = 1
	p.NbReadsWritesPie.PaddingBottom = 1

	p.AvgDurationPlotData = []*list.List{list.New(), list.New()}
	termWidth, _ := ui.TerminalDimensions()
	avgDurationPlotDataLength = int(float64(termWidth) * (avgDurationPlotWidthPercent - .025))
	var zero float64
	for i := 0; i < avgDurationPlotDataLength; i++ {
		p.AvgDurationPlotData[0].PushBack(zero)
		p.AvgDurationPlotData[1].PushBack(zero)
	}
	p.AvgDurationPlot.Title = " Avg. duration read/write "
	p.AvgDurationPlot.PaddingTop = 1
	// xterm color reference https://jonasjacek.github.io/colors/
	// e.g. ui.Color(89) is xterm color DeepPink4
	p.AvgDurationPlot.LineColors[0] = ui.ColorGreen
	p.AvgDurationPlot.LineColors[1] = ui.ColorRed
	p.AvgDurationPlot.DataLabels = []string{"read", "write"}

	p.MemoryPlotData = []*list.List{list.New(), list.New()}
	memoryPlotDataLength = int(float64(termWidth) * (memoryPlotWidthPercent - .025))
	for i := 0; i < memoryPlotDataLength; i++ {
		p.MemoryPlotData[0].PushBack(zero)
		p.MemoryPlotData[1].PushBack(zero)
	}
	p.MemoryPlot.Title = " Memory reserved/in use "
	p.MemoryPlot.PaddingTop = 1
	p.MemoryPlot.LineColors[0] = ui.ColorGreen
	p.MemoryPlot.LineColors[1] = ui.ColorRed
	p.MemoryPlot.DataLabels = []string{"reserved", "in use"}

	p.Grid.Set(
		ui.NewRow(
			.25,
			ui.NewCol(1-avgDurationPlotWidthPercent, p.SummaryTable),
			ui.NewCol(avgDurationPlotWidthPercent, p.AvgDurationPlot),
		),
		ui.NewRow(
			.5,
			ui.NewCol(.5, p.SizePie),
			ui.NewCol(.5, p.NbReadsWritesPie),
		),
		ui.NewRow(
			.25,
			ui.NewCol(memoryPlotWidthPercent, p.MemoryPlot),
		),
	)

}

func newStatsController() Controller {
	ctl := &statsController{
		Grid:             ui.NewGrid(),
		SummaryTable:     widgets.NewTable(),
		SizePie:          widgets.NewPieChart(),
		NbReadsWritesPie: widgets.NewPieChart(),
		AvgDurationPlot:  widgets.NewPlot(),
		MemoryPlot:       widgets.NewPlot(),
	}
	ctl.initUI()
	return ctl
}
