package metrics

import "github.com/prometheus/client_golang/prometheus"

type ProgressTracker interface {
	Add(v float64)
}

type prometheusProgressTracker struct {
	progress  prometheus.Gauge
	currValue float64
	maxValue  float64
}

func NewPrometheusProgressTracker(maxValue float64, gauge prometheus.Gauge) ProgressTracker {
	return &prometheusProgressTracker{
		progress: gauge,
		maxValue: maxValue,
	}
}

func (p *prometheusProgressTracker) Add(v float64) {
	p.currValue += v
	p.progress.Set(p.currValue / p.maxValue)
}
