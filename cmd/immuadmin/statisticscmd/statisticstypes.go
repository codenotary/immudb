package statisticscmd

import (
	"container/ring"
	"fmt"

	dto "github.com/prometheus/client_model/go"
)

type MetricsLoader interface {
	Load() (*map[string]*dto.MetricFamily, error)
}

type Controller interface {
	Render(*map[string]*dto.MetricFamily)
	Resize()
}

// StatRing, encapsulate container/ring,  adapt to chart data
type StatRing struct {
	r *ring.Ring
}

func NewChartRing(n int) *StatRing {
	return &StatRing{r: ring.New(n)}
}

func (p *StatRing) Push(n uint64) {
	p.r.Value = n
	p.r = p.r.Next()
}

// Data, convert underline data to float64
func (p *StatRing) Data() []float64 {
	var l []float64
	p.r.Do(func(x interface{}) {
		if v, ok := x.(uint64); ok {
			l = append(l, float64(v))
		} else {
			l = append(l, 0.0)
		}
	})
	return l
}

// NormalizedData return normalized data between [0,1]
func (p *StatRing) NormalizedData() []float64 {
	max := p.max()
	if max == 0 {
		return make([]float64, p.r.Len(), p.r.Len())
	}

	var l []float64
	p.r.Do(func(x interface{}) {
		var pct float64
		if v, ok := x.(uint64); ok {
			pct = float64(v) / float64(max)
		}
		l = append(l, pct)
	})
	return l
}

func (p *StatRing) max() uint64 {
	var max uint64
	// find max
	p.r.Do(func(x interface{}) {
		if v, ok := x.(uint64); ok && v > max {
			max = v
		}
	})
	return max
}

func byteCountBinary(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
