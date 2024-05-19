package chqdecoratorprocessor

import (
	"math"
)

type OnlineWindowStat struct {
	windowSize int
	n          int
	mean       float64
	m2         float64
	window     []float64
	numSame    int
}

func NewOnlineWindowStat(windowSize int) *OnlineWindowStat {
	return &OnlineWindowStat{
		windowSize: windowSize,
		window:     make([]float64, 0, windowSize+1),
	}
}

func (o *OnlineWindowStat) Update(x float64) {
	if math.IsNaN(x) {
		return
	}
	delta := x - o.mean
	o.n++
	prevMean := o.getMean()
	o.mean += delta / float64(o.n)
	o.m2 += delta * (x - o.mean)

	o.window = append(o.window, x)

	if prevMean != 0.0 && prevMean != o.getMean() {
		o.numSame = int(math.Max(0, float64(o.numSame-1)))
	} else if o.getMean() == round(x) {
		o.numSame = int(math.Min(float64(o.numSame+1), float64(o.windowSize+1)))
	}

	if len(o.window) <= o.windowSize {
		return
	}
	old := o.window[0]
	if round(old) == prevMean {
		o.numSame = int(math.Max(0, float64(o.numSame-1)))
	}
	oldValue := old
	oldDelta := oldValue - o.mean
	o.n--
	o.mean -= oldDelta / float64(o.n)
	o.m2 -= oldDelta * (oldValue - o.mean)
	o.window = o.window[1:]
}

func (o *OnlineWindowStat) variance() float64 {
	if o.n < 2 {
		return 0.0
	} else {
		return o.m2 / float64(o.n-1)
	}
}

func (o *OnlineWindowStat) stdDev() float64 {
	return math.Sqrt(o.variance())
}

func (o *OnlineWindowStat) getMean() float64 {
	return round(o.mean)
}

func (o *OnlineWindowStat) GreaterThanThreeStdDev(t float64) bool {
	return o.getMean()+3*o.stdDev() <= t
}

func round(d float64) float64 {
	if d < 1 {
		return d
	} else {
		return float64(math.Round(d))
	}
}
