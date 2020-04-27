package statisticscmd

import (
	"fmt"

	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	terminalWidth     = 120
	heapAllocBarCount = 6
)

type memStatsController struct {
	Grid *ui.Grid

	HeapObjectsSparkline     *widgets.Sparkline
	HeapObjectSparklineGroup *widgets.SparklineGroup
	HeapObjectsData          *StatRing

	SysText       *widgets.Paragraph
	GCCPUFraction *widgets.Gauge

	HeapAllocBarChart     *widgets.BarChart
	HeapAllocBarChartData *StatRing

	HeapPie *widgets.PieChart
}

func (p *memStatsController) Resize() {
	p.resize()
	ui.Render(p.Grid)
}

func (p *memStatsController) resize() {
	w, h := ui.TerminalDimensions()
	// p.Grid.SetRect(0, 0, terminalWidth, h)
	p.Grid.SetRect(0, 0, w, h)
}

func (p *memStatsController) Render(data *map[string]*dto.MetricFamily) {
	if heapObjectsMetric := (*data)["go_memstats_heap_objects"]; heapObjectsMetric != nil {
		nbHeapObjects := uint64(*heapObjectsMetric.GetMetric()[0].GetGauge().Value)
		p.HeapObjectsData.Push(nbHeapObjects)
		p.HeapObjectsSparkline.Data = p.HeapObjectsData.NormalizedData()
		p.HeapObjectSparklineGroup.Title = fmt.Sprintf("HeapObjects, live heap object count: %d", nbHeapObjects)
	}

	if sysBytesMetric := (*data)["go_memstats_sys_bytes"]; sysBytesMetric != nil {
		nbSysBytes := *sysBytesMetric.GetMetric()[0].GetGauge().Value
		p.SysText.Text = fmt.Sprint(byteCountBinary(uint64(nbSysBytes)))
	}

	if gcCPUFractionMetric := (*data)["go_memstats_gc_cpu_fraction"]; gcCPUFractionMetric != nil {
		gcCPUFraction := *gcCPUFractionMetric.GetMetric()[0].GetGauge().Value
		// fNormalize := func() int {
		// 	f := gcCPUFraction
		// 	if f < 0.01 {
		// 		for f < 1 {
		// 			f = f * 10.0
		// 		}
		// 	}
		// 	return int(f)
		// }
		// p.GCCPUFraction.Label = fmt.Sprintf("%.2f%%", gcCPUFraction*100)
		p.GCCPUFraction.Percent = int(gcCPUFraction * 100)
		p.GCCPUFraction.Label = fmt.Sprintf("%.2f%%", gcCPUFraction*100)
	}

	if heapAllocMetric := (*data)["go_memstats_heap_alloc_bytes"]; heapAllocMetric != nil {
		heapAlloc := *heapAllocMetric.GetMetric()[0].GetGauge().Value
		p.HeapAllocBarChartData.Push(uint64(heapAlloc))
		p.HeapAllocBarChart.Data = p.HeapAllocBarChartData.Data()
		p.HeapAllocBarChart.Labels = nil
		for _, v := range p.HeapAllocBarChart.Data {
			p.HeapAllocBarChart.Labels = append(p.HeapAllocBarChart.Labels, byteCountBinary(uint64(v)))
		}
	}

	if heapIdleMetric, heapInUseMetric := (*data)["go_memstats_heap_idle_bytes"], (*data)["go_memstats_heap_inuse_bytes"]; heapIdleMetric != nil && heapInUseMetric != nil {
		heapIdle := *heapIdleMetric.GetMetric()[0].GetGauge().Value
		heapInUse := *heapInUseMetric.GetMetric()[0].GetGauge().Value
		p.HeapPie.Data = []float64{heapIdle, heapInUse}
	}

	ui.Render(p.Grid)
}

func (p *memStatsController) initUI() {
	p.resize()

	p.HeapObjectsSparkline.LineColor = ui.Color(89) // xterm color DeepPink4
	p.HeapObjectSparklineGroup = widgets.NewSparklineGroup(p.HeapObjectsSparkline)

	p.SysText.Title = "Sys, the total bytes of memory obtained from the OS"
	// p.SysText.PaddingLeft = 25
	p.SysText.PaddingLeft = 1
	p.SysText.PaddingTop = 1

	p.HeapAllocBarChart.BarGap = 2
	p.HeapAllocBarChart.BarWidth = 8
	p.HeapAllocBarChart.Title = "HeapAlloc, bytes of allocated heap objects"
	p.HeapAllocBarChart.NumFormatter = func(f float64) string { return "" }

	p.GCCPUFraction.Title = "GCCPUFraction 0%~100%"
	p.GCCPUFraction.BarColor = ui.Color(50) // xterm color Cyan2

	p.HeapPie.Title = "Used vs idle Heap"
	p.HeapPie.LabelFormatter = func(idx int, _ float64) string { return []string{"idle", "used"}[idx] }

	p.Grid.Set(
		ui.NewRow(.2, p.HeapObjectSparklineGroup),
		ui.NewRow(.8,
			ui.NewCol(.5,
				ui.NewRow(.2, p.SysText),
				ui.NewRow(.2, p.GCCPUFraction),
				ui.NewRow(.6, p.HeapAllocBarChart),
			),
			ui.NewCol(.5, p.HeapPie),
		),
	)

}

func newMemStatsController() Controller {
	w, _, _ := terminal.GetSize(0)
	if w <= 0 {
		w = terminalWidth
	}
	ctl := &memStatsController{
		Grid: ui.NewGrid(),

		HeapObjectsSparkline: widgets.NewSparkline(),
		// HeapObjectsData:      NewChartRing(terminalWidth),
		HeapObjectsData: NewChartRing(w),

		SysText:       widgets.NewParagraph(),
		GCCPUFraction: widgets.NewGauge(),

		HeapAllocBarChart:     widgets.NewBarChart(),
		HeapAllocBarChartData: NewChartRing(heapAllocBarCount),

		HeapPie: widgets.NewPieChart(),
	}

	ctl.initUI()

	return ctl
}
