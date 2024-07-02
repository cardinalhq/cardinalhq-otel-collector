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
	"time"

	"github.com/hashicorp/go-multierror"
)

type Boxer struct {
	buffer        Buffer
	interval      time.Duration
	intervalCount int64
	grace         time.Duration
	timefunc      TimeFunc
}

type TimeFunc func() time.Time

type BoxerForEachFunc func(value []byte) (bool, error)

var (
	MaintainNotNeeded = errors.New("maintain not needed")
)

func NewBoxer(options ...BoxerOptions) (*Boxer, error) {
	box := &Boxer{
		interval:      time.Minute,
		intervalCount: 60,
		grace:         0,
		timefunc:      time.Now,
	}
	for _, opt := range options {
		opt.apply(box)
	}

	return box, nil
}

func (b *Boxer) IntervalForTime(ts time.Time) int64 {
	return ts.UnixNano() / int64(b.interval)
}

func (b *Boxer) TimeForInterval(interval int64) time.Time {
	return time.Unix(0, 0).Add(b.interval * time.Duration(interval))
}

// Put puts a new item into the timebox.  If the item's time is too old,
// it will not be added, and a nil will be returned.
func (b *Boxer) Put(scope string, ts time.Time, item []byte) (tooold bool, err error) {
	if b.tooOld(ts) {
		return true, nil
	}
	interval := b.IntervalForTime(ts)
	err = b.buffer.Write(&BufferRecord{
		Interval: interval,
		Scope:    scope,
		Contents: item,
	})
	return false, err
}

func (b *Boxer) tooOld(ts time.Time) bool {
	return (b.timefunc().Sub(ts) > b.interval*time.Duration(b.intervalCount)+b.grace)
}

func (b *Boxer) intervalTooOld(now time.Time, interval int64) bool {
	currentInterval := b.IntervalForTime(now.Add(-b.grace))
	return interval < currentInterval-int64(b.intervalCount)
}

func (b *Boxer) ForEach(interval int64, scope string, fn BoxerForEachFunc) error {
	return b.buffer.ForEach(interval, scope, func(record *BufferRecord) (bool, error) {
		ok, err := fn(record.Contents)
		return ok, err
	})
}

func (b *Boxer) GetClosedIntervals(ts time.Time) ([]int64, error) {
	intervals, err := b.buffer.GetIntervals()
	if err != nil {
		return nil, err
	}
	var closed []int64
	for _, interval := range intervals {
		if b.intervalTooOld(ts, interval) {
			closed = append(closed, interval)
		}
	}
	return closed, nil
}

func (b *Boxer) GetAllIntervals() ([]int64, error) {
	return b.buffer.GetIntervals()
}

func (b *Boxer) CloseInterval(interval int64) error {
	intervals, err := b.GetClosedIntervals(b.TimeForInterval(interval))
	if err != nil {
		return err
	}
	var errs *multierror.Error
	for _, interval := range intervals {
		errs = multierror.Append(errs, b.CloseInterval(interval))
	}
	return errs.ErrorOrNil()
}

func (b *Boxer) Maintain() error {
	return nil
}

func (b *Boxer) Wipe() error {
	if wiper, ok := b.buffer.(Wiper); ok {
		return wiper.Wipe()
	}
	return nil
}

func (b *Boxer) Close() error {
	return b.buffer.Shutdown()
}
