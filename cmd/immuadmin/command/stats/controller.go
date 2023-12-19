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

package stats

import (
	"container/list"
	"fmt"
	"math"
	"time"

	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
)

// Controller ...
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
	tui                   Tui
}

// Tui ...
type Tui interface {
	TerminalDimensions() (int, int)
	Render(items ...ui.Drawable)
	Init() error
	Close()
	PollEvents() <-chan ui.Event
}

type tui struct{}

func (t tui) TerminalDimensions() (int, int) {
	return ui.TerminalDimensions()
}
func (t tui) Render(items ...ui.Drawable) {
	ui.Render(items...)
}
func (t tui) Init() error {
	return ui.Init()
}
func (t tui) Close() {
	ui.Close()
}
func (t tui) PollEvents() <-chan ui.Event {
	return ui.PollEvents()
}

func (p *statsController) Resize() {
	p.resize()
	p.tui.Render(p.Grid)
}

func (p *statsController) resize() {
	termWidth, termHeight := p.tui.TerminalDimensions()
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

func updatePlot(
	plot *widgets.Plot,
	data *[]*list.List,
	dataLength int,
	newData []float64,
	newTitle string,
) {
	plot.Title = newTitle
	nbLines := len(newData)
	for i := 0; i < nbLines; i++ {
		enqueueDequeue((*data)[i], newData[i], dataLength)
	}
	plot.Data = make([][]float64, nbLines)
	for i := 0; i < nbLines; i++ {
		plot.Data[i] = toFloats((*data)[i])
	}
}

func (p *statsController) Render(ms *metrics) {
	db := ms.dbWithMostEntries()

	uptime, _ := time.ParseDuration(fmt.Sprintf("%.4fh", ms.uptimeHours))
	p.SummaryTable.Rows = [][]string{
		{"[ImmuDB stats](mod:bold)", fmt.Sprintf("[ at %s](mod:bold)", time.Now().Format("15:04:05"))},
		{"Database", db.name},
		{"Uptime", uptime.String()},
		{"Entries", fmt.Sprintf("%d", db.nbEntries)},
		{"No. clients", fmt.Sprintf("%d", ms.nbClients)},
		{"  active < 1h ago", fmt.Sprintf("%d", len(*ms.clientsActiveDuringLastHour()))},
	}

	totalSizeS, totalSize := byteCountBinary(db.totalBytes)
	updatePlot(
		p.SizePlot,
		&p.SizePlotData,
		sizePlotDataLength,
		[]float64{totalSize},
		fmt.Sprintf(" DB Size: %s ", totalSizeS))

	if p.withDBHistograms {
		nbReads := ms.reads.counter
		nbWrites := ms.writes.counter
		nbReadsWrites := nbReads + nbWrites
		pReads := math.Round(float64(nbReads) * 100 / float64(nbReadsWrites))
		pWrites := math.Round(float64(nbWrites) * 100 / float64(nbReadsWrites))
		updatePlot(
			p.NbReadsWritesPlot,
			&p.NbReadsWritesPlotData,
			nbReadsWritesPlotDataLength,
			[]float64{float64(nbReads), float64(nbWrites)},
			fmt.Sprintf(
				" %d reads / %d writes (%.0f%% / %.0f%%) ", nbReads, nbWrites, pReads, pWrites),
		)

		avgDurationReads := ms.reads.avgDuration * 1000_000
		avgDurationWrites := ms.writes.avgDuration * 1000_000
		updatePlot(
			p.AvgDurationPlot,
			&p.AvgDurationPlotData,
			avgDurationPlotDataLength,
			[]float64{avgDurationReads, avgDurationWrites},
			fmt.Sprintf(
				" Avg. duration: %.0f µs read, %.0f µs write ", avgDurationReads, avgDurationWrites),
		)
	}

	memReservedS, memReserved := byteCountBinary(ms.memstats.sysBytes)
	memInUseS, memInUse := byteCountBinary(ms.memstats.heapInUseBytes + ms.memstats.stackInUseBytes)
	updatePlot(
		p.MemoryPlot,
		&p.MemoryPlotData,
		memoryPlotDataLength,
		[]float64{memReserved, memInUse},
		fmt.Sprintf(" Memory: %s reserved, %s in use ", memReservedS, memInUseS))

	ui.Render(p.Grid)
}

func initPlot(
	plot *widgets.Plot,
	data *[]*list.List,
	dataLength int,
	labels []string,
	title string,
) {
	nbLines := len(labels)
	for i := 0; i < nbLines; i++ {
		(*data)[i] = list.New()
	}
	for i := 0; i < dataLength; i++ {
		for j := 0; j < nbLines; j++ {
			(*data)[j].PushBack(0.)
		}
	}
	plot.Title = title
	plot.PaddingTop = 1
	plot.DataLabels = labels
}

var avgDurationPlotDataLength int
var nbReadsWritesPlotDataLength int
var sizePlotDataLength int
var memoryPlotDataLength int

func (p *statsController) initUI() {
	p.resize()

	gridWidth := p.Grid.GetRect().Dx()
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

	p.SizePlotData = make([]*list.List, 2)
	initPlot(
		p.SizePlot,
		&p.SizePlotData,
		int(float64(gridWidth)*(sizePlotWidthPercent-.025)),
		[]string{"DB Size"},
		" DB Size ")

	p.NbReadsWritesPlotData = make([]*list.List, 2)
	if p.withDBHistograms {
		initPlot(
			p.NbReadsWritesPlot,
			&p.NbReadsWritesPlotData,
			int(float64(gridWidth)*(nbReadsWritesWidthPercent-.025)),
			[]string{"reads", "writes"},
			" Number of reads/writes ")

		p.AvgDurationPlotData = make([]*list.List, 2)
		initPlot(
			p.AvgDurationPlot,
			&p.AvgDurationPlotData,
			int(float64(gridWidth)*(avgDurationPlotWidthPercent-.025)),
			[]string{"read", "write"},
			" Avg. duration read/write ")
	}

	p.MemoryPlotData = make([]*list.List, 2)
	initPlot(
		p.MemoryPlot,
		&p.MemoryPlotData,
		int(float64(gridWidth)*(memoryPlotWidthPercent-.025)),
		[]string{"reserved", "in use"},
		" Memory reserved/in use ")

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

func newStatsController(withDBHistograms bool, tui Tui) Controller {
	// xterm color reference https://jonasjacek.github.io/colors/
	ui.Theme.Block.Title.Fg = ui.ColorGreen
	ctl := &statsController{
		withDBHistograms: withDBHistograms,
		Grid:             ui.NewGrid(),
		SummaryTable:     widgets.NewTable(),
		SizePlot:         widgets.NewPlot(),
		MemoryPlot:       widgets.NewPlot(),
		tui:              tui,
	}
	if withDBHistograms {
		ctl.NbReadsWritesPlot = widgets.NewPlot()
		ctl.AvgDurationPlot = widgets.NewPlot()
	}
	ctl.initUI()
	return ctl
}
