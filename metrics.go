package main

import (
	"math"
	"sort"
)

func applyMetrics(r *Result, durations []int64) {
	avg, p50, p90, p95, p99, max := computeMetrics(durations, r.Success)
	r.AvgMs = avg
	r.P50Ms = p50
	r.P90Ms = p90
	r.P95Ms = p95
	r.P99Ms = p99
	r.MaxMs = max
	if r.Duration > 0 {
		r.RPS = float64(r.Success) / r.Duration.Seconds()
	}
}

func computeMetrics(durations []int64, successCount int) (avg, p50, p90, p95, p99, max float64) {
	if successCount == 0 {
		return 0, 0, 0, 0, 0, 0
	}
	vals := make([]int64, 0, successCount)
	for _, d := range durations {
		if d < 0 {
			continue
		}
		vals = append(vals, d)
	}
	if len(vals) == 0 {
		return 0, 0, 0, 0, 0, 0
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	var sum int64
	for _, v := range vals {
		sum += v
	}
	avg = float64(sum) / float64(len(vals))
	max = float64(vals[len(vals)-1])
	p50 = percentile(vals, 50)
	p90 = percentile(vals, 90)
	p95 = percentile(vals, 95)
	p99 = percentile(vals, 99)
	return
}

func percentile(vals []int64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	idx := int(math.Ceil((p/100.0)*float64(len(vals)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(vals) {
		idx = len(vals) - 1
	}
	return float64(vals[idx])
}

func percentileSeries(perSec [][]int64) (p50, p90, p95, p99 []float64) {
	n := len(perSec)
	p50 = make([]float64, n)
	p90 = make([]float64, n)
	p95 = make([]float64, n)
	p99 = make([]float64, n)
	for i := 0; i < n; i++ {
		if len(perSec[i]) == 0 {
			continue
		}
		sort.Slice(perSec[i], func(a, b int) bool { return perSec[i][a] < perSec[i][b] })
		p50[i] = percentile(perSec[i], 50)
		p90[i] = percentile(perSec[i], 90)
		p95[i] = percentile(perSec[i], 95)
		p99[i] = percentile(perSec[i], 99)
	}
	return
}

func countsToFloat64(counts []int64) []float64 {
	if len(counts) == 0 {
		return nil
	}
	out := make([]float64, len(counts))
	for i, v := range counts {
		out[i] = float64(v)
	}
	return out
}
