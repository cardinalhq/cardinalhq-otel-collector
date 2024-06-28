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

package badgerbox

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Box struct {
	kvs           KVS
	interval      time.Duration
	intervalCount int64
	grace         time.Duration
	timefunc      TimeFunc
	ttl           time.Duration
	openIntervals map[int64]struct{}
	keyprefix     string
}

type TimeFunc func() time.Time

const (
	NoTTL                = time.Duration(0)
	intervalMarkerPrefix = "interval"
	keySeparator         = "-"
)

func NewBox(options ...BoxOptions) (*Box, error) {
	box := &Box{
		interval:      time.Minute,
		intervalCount: 60,
		grace:         0,
		timefunc:      time.Now,
		ttl:           NoTTL,
		openIntervals: make(map[int64]struct{}),
	}
	for _, opt := range options {
		opt.apply(box)
	}

	if err := box.loadOpenIntervals(); err != nil {
		return nil, err
	}
	return box, nil
}

func (b *Box) loadOpenIntervals() error {
	return b.kvs.ForEachPrefix([]byte(intervalMarkerPrefix), func(key []byte, value []byte) bool {
		interval, err := parseIntervalMarker(string(key))
		if err != nil {
			b.kvs.Delete(key)
			return true
		}
		b.openIntervals[interval] = struct{}{}
		return true
	})
}

// Generate a random number between 100000000000 and 999999999999
func randomSuffix() int64 {
	return rand.Int63n(999_999_999_999-100_000_000_000) + 100_000_000_000
}

func (b *Box) intervalNumber(ts time.Time) int64 {
	return ts.UnixNano() / int64(b.interval)
}

func (b *Box) generateFullKey(scope string, ts time.Time) []byte {
	r := fmt.Sprintf("%d-%s-%d", b.intervalNumber(ts), scope, randomSuffix())
	return []byte(r)
}

func (b *Box) generatePrefix(scope string, ts time.Time) []byte {
	r := fmt.Sprintf("%d-%s-", b.intervalNumber(ts), scope)
	return []byte(r)
}

// replace any non-letter, non-number characters with an underscore
func sanitizeScope(scope string) string {
	for i := 0; i < len(scope); i++ {
		if !((scope[i] >= 'a' && scope[i] <= 'z') || (scope[i] >= 'A' && scope[i] <= 'Z') || (scope[i] >= '0' && scope[i] <= '9')) {
			scope = scope[:i] + "_" + scope[i+1:]
		}
	}
	return scope
}

func markerToInterval(interval int64) string {
	return fmt.Sprintf("%s-%d", intervalMarkerPrefix, interval)
}

func parseIntervalMarker(marker string) (int64, error) {
	parts := strings.Split(marker, "-")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid interval marker: %s", marker)
	}
	interval, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid interval marker: %s", marker)
	}
	return interval, nil
}

// Put puts a new item into the timebox.  If the item's time is too old,
// it will not be added, and a nil will be returned.
func (b *Box) Put(scope string, ts time.Time, item []byte) (key []byte, err error) {
	scope = sanitizeScope(scope)
	if b.tooOld(ts) {
		return nil, nil
	}
	fullkey := b.generateFullKey(scope, ts)
	if err := b.kvs.Set(fullkey, item, b.ttl); err != nil {
		return []byte{}, err
	}
	intervalNumber := b.intervalNumber(ts)
	marker := fmt.Sprintf("%s-%d", intervalMarkerPrefix, intervalNumber)
	if err := b.kvs.Set([]byte(marker), []byte{}, b.ttl); err != nil {
		return []byte{}, err
	}
	if _, ok := b.openIntervals[intervalNumber]; !ok {
		if err := b.kvs.Set([]byte(markerToInterval(intervalNumber)), []byte{}, b.ttl); err != nil {
			return []byte{}, err
		}
		b.openIntervals[intervalNumber] = struct{}{}
	}
	return fullkey, nil
}

func (b *Box) tooOld(ts time.Time) bool {
	return (b.timefunc().Sub(ts) > b.interval*time.Duration(b.intervalCount)+b.grace)
}

// ForEach calls the given function for each item in the timebox.
// If the function returns false, the iteration stops.
func (b *Box) ForEach(scope string, ts time.Time, f func(key []byte, value []byte) bool) error {
	scope = sanitizeScope(scope)
	prefix := b.generatePrefix(scope, ts)
	return b.kvs.ForEachPrefix(prefix, f)
}

func (b *Box) GetClosedIntervals(ts time.Time) ([]int64, error) {
	intervals := []int64{}
	for interval := range b.openIntervals {
		if b.tooOld(ts) {
			intervals = append(intervals, interval)
		}
	}
	return intervals, nil
}

func (b *Box) CloseInterval(interval int64) error {
	delete(b.openIntervals, interval)
	return b.kvs.Delete([]byte(markerToInterval(interval)))
}
