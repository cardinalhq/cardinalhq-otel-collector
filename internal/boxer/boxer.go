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

package boxer

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Boxer struct {
	kvs           KVS
	interval      time.Duration
	intervalCount int64
	grace         time.Duration
	timefunc      TimeFunc
	ttl           time.Duration
	openIntervals map[int64]struct{}
	keySuffix     string
}

type TimeFunc func() time.Time

const (
	NoTTL                = time.Duration(0)
	intervalMarkerPrefix = "interval"
	keySeparator         = "-"
)

var (
	MaintainNotNeeded = errors.New("maintain not needed")
)

func NewBoxer(options ...BoxerOptions) (*Boxer, error) {
	box := &Boxer{
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

func (b *Boxer) loadOpenIntervals() error {
	return b.kvs.ForEachPrefix([]byte(intervalMarkerPrefix), func(key []byte, value []byte) bool {
		interval, err := parseIntervalMarker(string(key))
		if err != nil {
			_ = b.kvs.Delete(key)
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

func (b *Boxer) IntervalForTime(ts time.Time) int64 {
	return ts.UnixNano() / int64(b.interval)
}

func (b *Boxer) generateFullKey(scope string, ts time.Time) []byte {
	r := fmt.Sprintf("%d-%s-%d-%s", b.IntervalForTime(ts), scope, randomSuffix(), b.keySuffix)
	return []byte(r)
}

func (b *Boxer) generatePrefix(scope string, ts time.Time) []byte {
	r := fmt.Sprintf("%d-%s-", b.IntervalForTime(ts), scope)
	return []byte(r)
}

func (b *Boxer) generateIntervalPrefix(interval int64) []byte {
	return []byte(strconv.FormatInt(interval, 10) + keySeparator)
}

// replace any non-letter, non-number, non-period characters with an underscore
func sanitizeScope(scope string) string {
	for i := 0; i < len(scope); i++ {
		if !((scope[i] >= 'a' && scope[i] <= 'z') || (scope[i] >= 'A' && scope[i] <= 'Z') || (scope[i] >= '0' && scope[i] <= '9') || scope[i] == '.') {
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

func (b *Boxer) TimeForInterval(interval int64) time.Time {
	return time.Unix(0, 0).Add(b.interval * time.Duration(interval))
}

func (b *Boxer) SplitKey(key []byte) (scope string, ts time.Time, err error) {
	parts := strings.Split(string(key), "-")
	if len(parts) < 3 {
		return "", time.Time{}, fmt.Errorf("invalid key: %s", key)
	}
	tsint, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("invalid key: %s", key)
	}
	ts = b.TimeForInterval(tsint)
	scope = parts[1]
	return
}

// Put puts a new item into the timebox.  If the item's time is too old,
// it will not be added, and a nil will be returned.
func (b *Boxer) Put(scope string, ts time.Time, item []byte) (key []byte, err error) {
	scope = sanitizeScope(scope)
	if b.tooOld(ts) {
		return nil, nil
	}
	fullkey := b.generateFullKey(scope, ts)
	if err := b.kvs.Set(fullkey, item, b.ttl); err != nil {
		return []byte{}, err
	}
	intervalNumber := b.IntervalForTime(ts)
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

func (b *Boxer) tooOld(ts time.Time) bool {
	return (b.timefunc().Sub(ts) > b.interval*time.Duration(b.intervalCount)+b.grace)
}

func (b *Boxer) intervalTooOld(interval int64) bool {
	currentInterval := b.IntervalForTime(b.timefunc().Add(-b.grace))
	return interval < currentInterval-int64(b.intervalCount)
}

// ForEachScope calls the given function for each item in the timebox.
// If the function returns false, the iteration stops.
func (b *Boxer) ForEachScope(scope string, ts time.Time, f func(scope string, ts time.Time, key []byte, value []byte) bool) error {
	scope = sanitizeScope(scope)
	prefix := b.generatePrefix(scope, ts)
	return b.foreach(prefix, f)
}

func (b *Boxer) ForEach(tbox int64, f func(scope string, ts time.Time, key []byte, value []byte) bool) error {
	prefix := b.generateIntervalPrefix(tbox)
	return b.foreach(prefix, f)
}

func (b *Boxer) foreach(prefix []byte, f func(scope string, ts time.Time, key []byte, value []byte) bool) error {
	return b.kvs.ForEachPrefix(prefix, func(key []byte, value []byte) bool {
		scope, ts, err := b.SplitKey(key)
		if err != nil {
			return false
		}
		return f(scope, ts, key, value)
	})
}

func (b *Boxer) GetClosedIntervals(ts time.Time) ([]int64, error) {
	intervals := []int64{}
	for interval := range b.openIntervals {
		if b.intervalTooOld(interval) {
			intervals = append(intervals, interval)
		}
	}
	return intervals, nil
}

func (b *Boxer) GetAllIntervals() ([]int64, error) {
	intervals := []int64{}
	for interval := range b.openIntervals {
		intervals = append(intervals, interval)
	}
	return intervals, nil
}

func (b *Boxer) CloseInterval(interval int64) error {
	delete(b.openIntervals, interval)
	keys := [][]byte{[]byte(markerToInterval(interval))}
	b.kvs.ForEachPrefix(b.generateIntervalPrefix(interval), func(key []byte, value []byte) bool {
		keys = append(keys, key)
		return true
	})
	skeys := []string{}
	for _, k := range keys {
		skeys = append(skeys, string(k))
	}
	slog.Info("closing interval", "interval", interval, "keys", skeys)
	return b.kvs.Delete(keys...)
}

func (b *Boxer) Maintain() error {
	return b.kvs.Maintain()
}

func (b *Boxer) Wipe() error {
	if wiper, ok := b.kvs.(Wiper); ok {
		return wiper.Wipe()
	}
	return nil
}

func (b *Boxer) Close() error {
	return b.kvs.Close()
}
