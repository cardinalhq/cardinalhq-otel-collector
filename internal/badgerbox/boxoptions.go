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

import "time"

type BoxOptions interface {
	apply(*Box)
}

type boxOptionFunc func(*Box)

func (f boxOptionFunc) apply(box *Box) {
	f(box)
}

func WithKVS(kvs KVS) BoxOptions {
	return boxOptionFunc(func(b *Box) {
		b.kvs = kvs
	})
}

func WithInterval(interval time.Duration) BoxOptions {
	return boxOptionFunc(func(b *Box) {
		b.interval = interval
	})
}

func WithIntervalCount(intervalCount int64) BoxOptions {
	return boxOptionFunc(func(b *Box) {
		b.intervalCount = intervalCount
	})
}

func WithGrace(grace time.Duration) BoxOptions {
	return boxOptionFunc(func(b *Box) {
		b.grace = grace
	})
}

func WithTimeFunc(timefunc TimeFunc) BoxOptions {
	return boxOptionFunc(func(b *Box) {
		b.timefunc = timefunc
	})
}

func WithTTL(ttl time.Duration) BoxOptions {
	return boxOptionFunc(func(b *Box) {
		b.ttl = ttl
	})
}

func WithOpenIntervals(openIntervals map[int64]struct{}) BoxOptions {
	return boxOptionFunc(func(b *Box) {
		b.openIntervals = openIntervals
	})
}

func WithKeyPrefix(keyprefix string) BoxOptions {
	return boxOptionFunc(func(b *Box) {
		b.keyprefix = keyprefix
	})
}
