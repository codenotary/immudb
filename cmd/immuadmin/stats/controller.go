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

package stats

import (
	"container/list"
	"fmt"
	"math"
	"time"

	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
)

type Controller interface {
	Render(*metrics)
	Resize()
}

type statsController struct {
	withDBHistograms      bool
	Grid                  *ui.Grid
	SummaryTable          *widgets.Table
	SizePlot              *widgets.Plot
	SizePlotData          []*list.List
	NbReadsWritesPlot     *widgets.Plot
	NbReadsWritesPlotData []*list.List
	AvgDurationPlot       *widgets.Plot
	AvgDurationPlotData   []*list.List
	MemoryPlot            *widgets.Plot
	MemoryPlotData        []*list.List
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

func (p *statsController) Render(ms *metrics) {
	uptime, _ := time.ParseDuration(fmt.Sprintf("%.4fh", ms.db.uptimeHours))
	p.SummaryTable.Rows = [][]string{
		[]string{"[ImmuDB stats](mod:bold)", fmt.Sprintf("[ at %s](mod:bold)", time.Now().Format("15:04:05"))},
		[]string{"Database", ms.db.name},
		[]string{"Uptime", uptime.String()},
		[]string{"Entries", fmt.Sprintf("%d", ms.db.nbEntries)},
		[]string{"No. clients", fmt.Sprintf("%d", ms.nbClients)},
		[]string{"  active < 1h ago", fmt.Sprintf("%d", len(*ms.clientsActiveDuringLastHour()))},
	}

	totalSizeS, _ := byteCountBinary(ms.db.totalBytes)
	vlogSizeS, vlogSize := byteCountBinary(ms.db.vlogBytes)
	lsmSizeS, lsmSize := byteCountBinary(ms.db.lsmBytes)
	p.SizePlot.Title =
		fmt.Sprintf(" DB Size: %s (VLog %s, LSM %s) ", totalSizeS, vlogSizeS, lsmSizeS)
	enqueueDequeue(p.SizePlotData[0], vlogSize, sizePlotDataLength)
	enqueueDequeue(p.SizePlotData[1], lsmSize, sizePlotDataLength)
	p.SizePlot.Data = [][]float64{
		toFloats(p.SizePlotData[0]),
		toFloats(p.SizePlotData[1]),
	}

	if p.withDBHistograms {
		nbReads := ms.reads.counter
		nbWrites := ms.writes.counter
		nbReadsWrites := nbReads + nbWrites
		pReads := math.Round(float64(nbReads) * 100 / float64(nbReadsWrites))
		pWrites := math.Round(float64(nbWrites) * 100 / float64(nbReadsWrites))
		p.NbReadsWritesPlot.Title = fmt.Sprintf(
			" %d reads / %d writes (%.0f%% / %.0f%%) ", nbReads, nbWrites, pReads, pWrites)
		enqueueDequeue(p.NbReadsWritesPlotData[0], float64(nbReads), nbReadsWritesPlotDataLength)
		enqueueDequeue(p.NbReadsWritesPlotData[1], float64(nbWrites), nbReadsWritesPlotDataLength)
		p.NbReadsWritesPlot.Data = [][]float64{
			toFloats(p.NbReadsWritesPlotData[0]),
			toFloats(p.NbReadsWritesPlotData[1]),
		}

		avgDurationReads := ms.reads.avgDuration * 1000_000
		avgDurationWrites := ms.writes.avgDuration * 1000_000
		p.AvgDurationPlot.Title = fmt.Sprintf(" Avg. duration: %.0f µs read, %.0f µs write ", avgDurationReads, avgDurationWrites)
		enqueueDequeue(p.AvgDurationPlotData[0], avgDurationReads, avgDurationPlotDataLength)
		enqueueDequeue(p.AvgDurationPlotData[1], avgDurationWrites, avgDurationPlotDataLength)
		p.AvgDurationPlot.Data = [][]float64{
			toFloats(p.AvgDurationPlotData[0]),
			toFloats(p.AvgDurationPlotData[1]),
		}
	}

	memReservedS, memReserved := byteCountBinary(ms.memstats.sysBytes)
	memInUseS, memInUse := byteCountBinary(ms.memstats.heapInUseBytes + ms.memstats.stackInUseBytes)
	p.MemoryPlot.Title = fmt.Sprintf(" Memory: %s reserved, %s in use ", memReservedS, memInUseS)
	enqueueDequeue(p.MemoryPlotData[0], memReserved, memoryPlotDataLength)
	enqueueDequeue(p.MemoryPlotData[1], memInUse, memoryPlotDataLength)
	p.MemoryPlot.Data = [][]float64{
		toFloats(p.MemoryPlotData[0]),
		toFloats(p.MemoryPlotData[1]),
	}

	ui.Render(p.Grid)
}

var avgDurationPlotDataLength int
var nbReadsWritesPlotDataLength int
var sizePlotDataLength int
var memoryPlotDataLength int

func (p *statsController) initUI() {
	p.resize()

	termWidth, _ := ui.TerminalDimensions()
	avgDurationPlotWidthPercent := .6
	memoryPlotWidthPercent := avgDurationPlotWidthPercent
	sizePlotWidthPercent := 1.
	nbReadsWritesWidthPercent := 0.
	if p.withDBHistograms {
		memoryPlotWidthPercent = 1.
		sizePlotWidthPercent = .5
		nbReadsWritesWidthPercent = .5
	}

	p.SummaryTable.Title = " Exit: q, Esc or Ctrl-C "
	p.SummaryTable.PaddingTop = 1

	p.SizePlotData = []*list.List{list.New(), list.New()}
	sizePlotDataLength = int(float64(termWidth) * (sizePlotWidthPercent - .025))
	for i := 0; i < sizePlotDataLength; i++ {
		p.SizePlotData[0].PushBack(0.)
		p.SizePlotData[1].PushBack(0.)
	}
	p.SizePlot.Title = " DB Size "
	p.SizePlot.PaddingTop = 1
	p.SizePlot.LineColors[0] = ui.ColorGreen
	p.SizePlot.LineColors[1] = ui.ColorRed
	p.SizePlot.DataLabels = []string{"VLog", "LSM"}

	if p.withDBHistograms {
		p.NbReadsWritesPlotData = []*list.List{list.New(), list.New()}
		nbReadsWritesPlotDataLength = int(float64(termWidth) * (nbReadsWritesWidthPercent - .025))
		for i := 0; i < nbReadsWritesPlotDataLength; i++ {
			p.NbReadsWritesPlotData[0].PushBack(0.)
			p.NbReadsWritesPlotData[1].PushBack(0.)
		}
		p.NbReadsWritesPlot.Title = " Number of reads/writes "
		p.NbReadsWritesPlot.PaddingTop = 1
		p.NbReadsWritesPlot.LineColors[0] = ui.ColorGreen
		p.NbReadsWritesPlot.LineColors[1] = ui.ColorRed
		p.NbReadsWritesPlot.DataLabels = []string{"reads", "writes"}

		p.AvgDurationPlotData = []*list.List{list.New(), list.New()}
		avgDurationPlotDataLength = int(float64(termWidth) * (avgDurationPlotWidthPercent - .025))
		for i := 0; i < avgDurationPlotDataLength; i++ {
			p.AvgDurationPlotData[0].PushBack(0.)
			p.AvgDurationPlotData[1].PushBack(0.)
		}
		p.AvgDurationPlot.Title = " Avg. duration read/write "
		p.AvgDurationPlot.PaddingTop = 1
		// xterm color reference https://jonasjacek.github.io/colors/
		// e.g. ui.Color(89) is xterm color DeepPink4
		p.AvgDurationPlot.LineColors[0] = ui.ColorGreen
		p.AvgDurationPlot.LineColors[1] = ui.ColorRed
		p.AvgDurationPlot.DataLabels = []string{"read", "write"}
	}

	p.MemoryPlotData = []*list.List{list.New(), list.New()}
	memoryPlotDataLength = int(float64(termWidth) * (memoryPlotWidthPercent - .025))
	for i := 0; i < memoryPlotDataLength; i++ {
		p.MemoryPlotData[0].PushBack(0.)
		p.MemoryPlotData[1].PushBack(0.)
	}
	p.MemoryPlot.Title = " Memory reserved/in use "
	p.MemoryPlot.PaddingTop = 1
	p.MemoryPlot.LineColors[0] = ui.ColorGreen
	p.MemoryPlot.LineColors[1] = ui.ColorRed
	p.MemoryPlot.DataLabels = []string{"reserved", "in use"}

	if p.withDBHistograms {
		p.Grid.Set(
			ui.NewRow(
				.25,
				ui.NewCol(1-avgDurationPlotWidthPercent, p.SummaryTable),
				ui.NewCol(avgDurationPlotWidthPercent, p.AvgDurationPlot),
			),
			ui.NewRow(
				.5,
				ui.NewCol(sizePlotWidthPercent, p.SizePlot),
				ui.NewCol(1-sizePlotWidthPercent, p.NbReadsWritesPlot),
			),
			ui.NewRow(
				.25,
				ui.NewCol(memoryPlotWidthPercent, p.MemoryPlot),
			),
		)
		return
	}

	p.Grid.Set(
		ui.NewRow(
			.33,
			ui.NewCol(1-memoryPlotWidthPercent, p.SummaryTable),
			ui.NewCol(memoryPlotWidthPercent, p.MemoryPlot),
		),
		ui.NewRow(
			.66,
			ui.NewCol(sizePlotWidthPercent, p.SizePlot),
		),
	)
}

func newStatsController(withDBHistograms bool) Controller {
	ctl := &statsController{
		withDBHistograms: withDBHistograms,
		Grid:             ui.NewGrid(),
		SummaryTable:     widgets.NewTable(),
		SizePlot:         widgets.NewPlot(),
		MemoryPlot:       widgets.NewPlot(),
	}
	if withDBHistograms {
		ctl.NbReadsWritesPlot = widgets.NewPlot()
		ctl.AvgDurationPlot = widgets.NewPlot()
	}
	ctl.initUI()
	return ctl
}
