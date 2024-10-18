// Copyright 2024 CardinalHQ, Inc
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

package ottl

import (
	"math"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Sampler interface {
	// Start starts the sampler
	Start() error
	// Stop stops the sampler
	Stop() error
	// GetSampleRate returns the sample rate for a given key
	GetSampleRate(key string) int
	// GetSampleRateMulti returns the sample rate for a given key with a count
	GetSampleRateMulti(key string, count int) int
}

const (
	defaultClearFrequencyDuration = 30 * time.Second
	defaultMaxRPS                 = 50
)

type rpsSampler struct {
	// clearFrequencyDuration is how often the counters reset as a Duration.
	// The default is 30s.
	clearFrequencyDuration time.Duration

	// maxKeys is the maximum number of keys to track. If this is set to 0, there
	// is no limit. The default is 0.
	maxKeys int

	// MaxRPS is the minimum number of events per second that should be
	// sampled. If the total number of events in the last ClearFrequencyDuration
	// is less than this number, all events will be sampled at 1. The default is
	// 50.
	MaxRPS int

	savedSampleRates map[string]int
	currentCounts    map[string]float64

	haveData bool
	done     chan struct{}

	lock sync.Mutex

	requestCount int64
	eventCount   int64

	logger *zap.Logger
}

var _ Sampler = (*rpsSampler)(nil)

func NewRPSSampler(ops ...Option) *rpsSampler {
	s := &rpsSampler{
		clearFrequencyDuration: defaultClearFrequencyDuration,
		MaxRPS:                 defaultMaxRPS,
	}
	for _, op := range ops {
		op(s)
	}
	return s
}

type Option func(*rpsSampler)

func WithClearFrequencyDuration(d time.Duration) Option {
	return func(s *rpsSampler) {
		s.clearFrequencyDuration = d
	}
}

func WithMaxKeys(maxKeys int) Option {
	return func(s *rpsSampler) {
		s.maxKeys = maxKeys
	}
}

func WithMaxRPS(minEventsPerSec int) Option {
	return func(s *rpsSampler) {
		s.MaxRPS = minEventsPerSec
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(s *rpsSampler) {
		s.logger = logger
	}
}

func (a *rpsSampler) Start() error {
	// apply defaults
	if a.clearFrequencyDuration == 0 {
		a.clearFrequencyDuration = defaultClearFrequencyDuration
	}
	if a.MaxRPS == 0 {
		a.MaxRPS = defaultMaxRPS
	}

	a.savedSampleRates = make(map[string]int)
	a.currentCounts = make(map[string]float64)
	a.done = make(chan struct{})

	go func() {
		ticker := time.NewTicker(a.clearFrequencyDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				a.updateMaps()
			case <-a.done:
				return
			}
		}
	}()
	return nil
}

func (a *rpsSampler) Stop() error {
	close(a.done)
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (a *rpsSampler) updateMaps() {
	// make a local copy of the sample counters for calculation
	a.lock.Lock()
	tmpCounts := a.currentCounts
	a.currentCounts = make(map[string]float64)
	a.lock.Unlock()
	newSavedSampleRates := make(map[string]int)
	// short circuit if no traffic
	numKeys := len(tmpCounts)
	if numKeys == 0 {
		// no traffic the last 30s. clear the result map
		a.lock.Lock()
		defer a.lock.Unlock()
		a.savedSampleRates = newSavedSampleRates
		return
	}

	// Goal events to send this interval is the total count of received events
	// divided by the desired average sample rate
	var sumEvents float64
	for _, count := range tmpCounts {
		sumEvents += count
	}
	// check to see if we fall below the minimum
	targetMinimumCount := float64(a.MaxRPS) * a.clearFrequencyDuration.Seconds()
	if a.logger != nil {
		a.logger.Debug("updateMaps", zap.Float64("sumEvents", sumEvents), zap.Float64("targetMinimumCount", targetMinimumCount))
	}
	if sumEvents < targetMinimumCount {
		// we still need to go through each key to set sample rates individually
		for k := range tmpCounts {
			newSavedSampleRates[k] = 1
		}
		a.lock.Lock()
		defer a.lock.Unlock()
		a.savedSampleRates = newSavedSampleRates
		return
	}

	updatedGoalSampleRate := math.Ceil(float64(sumEvents) / float64(targetMinimumCount))
	if a.logger != nil {
		a.logger.Debug("updateMaps", zap.Float64("updatedGoalSampleRate", updatedGoalSampleRate))
	}
	goalCount := float64(sumEvents) / updatedGoalSampleRate

	// goalRatio is the goalCount divided by the sum of all the log values - it
	// determines what percentage of the total event space belongs to each key
	var logSum float64
	for _, count := range tmpCounts {
		logSum += math.Log10(float64(count))
	}
	// Note that this can produce Inf if logSum is 0
	goalRatio := goalCount / logSum

	newSavedSampleRates = calculateSampleRates(goalRatio, tmpCounts)
	if a.logger != nil {
		a.logger.Debug("updateMaps",
			zap.Float64("goalCount", goalCount),
			zap.Float64("logSum", logSum),
			zap.Any("tmpCounts", tmpCounts),
			zap.Any("newSavedSampleRates", newSavedSampleRates))
	}
	a.lock.Lock()
	defer a.lock.Unlock()
	a.savedSampleRates = newSavedSampleRates
	a.haveData = true
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key.
func (a *rpsSampler) GetSampleRate(key string) int {
	return a.GetSampleRateMulti(key, 1)
}

// GetSampleRateMulti takes a key representing count spans and returns the
// appropriate sample rate for that key.
func (a *rpsSampler) GetSampleRateMulti(key string, count int) int {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.requestCount++
	a.eventCount += int64(count)

	// Enforce MaxKeys limit on the size of the map
	if a.maxKeys > 0 {
		// If a key already exists, increment it. If not, but we're under the limit, store a new key
		if _, found := a.currentCounts[key]; found || len(a.currentCounts) < a.maxKeys {
			a.currentCounts[key] += float64(count)
		}
	} else {
		a.currentCounts[key] += float64(count)
	}
	if !a.haveData {
		return 1
	}
	if rate, found := a.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

// This is an extraction of common calculation logic for all the key-based samplers.
func calculateSampleRates(goalRatio float64, buckets map[string]float64) map[string]int {
	// must go through the keys in a fixed order to prevent rounding from changing
	// results
	keys := make([]string, len(buckets))
	var i int
	for k := range buckets {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	// goal number of events per key is goalRatio * key count, but never less than
	// one. If a key falls below its goal, it gets a sample rate of 1 and the
	// extra available events get passed on down the line.
	newSampleRates := make(map[string]int)
	keysRemaining := len(buckets)
	var extra float64
	for _, key := range keys {
		count := math.Max(1, buckets[key])
		// take the max of 1 or my log10 share of the total
		goalForKey := math.Max(1, math.Log10(count)*goalRatio)
		// take this key's share of the extra and pass the rest along
		extraForKey := extra / float64(keysRemaining)
		goalForKey += extraForKey
		extra -= extraForKey
		keysRemaining--
		if count <= goalForKey {
			// there are fewer samples than the allotted number for this key. set
			// sample rate to 1 and redistribute the unused slots for future keys
			newSampleRates[key] = 1
			extra += goalForKey - count
		} else {
			// there are more samples than the allotted number. SampleLogs this key enough
			// to knock it under the limit (aka round up)
			rate := math.Ceil(count / goalForKey)
			// if counts are <= 1 we can get values for goalForKey that are +Inf
			// and subsequent division ends up with NaN. If that's the case,
			// fall back to 1
			if math.IsNaN(rate) {
				newSampleRates[key] = 1
			} else {
				newSampleRates[key] = int(rate)
			}
			extra += goalForKey - (count / float64(newSampleRates[key]))
		}
	}
	return newSampleRates
}
