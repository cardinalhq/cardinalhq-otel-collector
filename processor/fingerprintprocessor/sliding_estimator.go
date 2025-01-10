// Copyright 2024-2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fingerprintprocessor

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

type SlidingEstimator struct {
	windowSum        float64
	windowCount      int
	windowInterval   int64
	windowLastUpdate int64
}

func NewSlidingEstimator(windowInterval int64) *SlidingEstimator {
	return &SlidingEstimator{
		windowInterval: windowInterval,
	}
}

func (s *SlidingEstimator) Update(t int64, x float64) (float64, bool) {
	if s.windowLastUpdate == 0 {
		s.windowLastUpdate = t - t%s.windowInterval
	}
	if t-s.windowLastUpdate >= s.windowInterval {
		avg := s.windowSum / float64(s.windowCount)
		s.windowSum = 0
		s.windowCount = 0
		s.windowLastUpdate = t - t%s.windowInterval
		return avg, true
	}
	s.windowSum += x
	s.windowCount++
	return 0, false
}

type SlidingEstimatorStat struct {
	*OnlineWindowStat
	*SlidingEstimator
}

func NewSlidingEstimatorStat(windowSize int, windowInterval int64) *SlidingEstimatorStat {
	return &SlidingEstimatorStat{
		OnlineWindowStat: NewOnlineWindowStat(windowSize),
		SlidingEstimator: NewSlidingEstimator(windowInterval),
	}
}

func (s *SlidingEstimatorStat) Update(t int64, x float64) {
	v, emit := s.SlidingEstimator.Update(t, x)
	if emit {
		s.OnlineWindowStat.Update(v)
	}
}

func (s *SlidingEstimatorStat) GreaterThanThreeStdDev(t float64) bool {
	return s.OnlineWindowStat.GreaterThanThreeStdDev(t)
}
