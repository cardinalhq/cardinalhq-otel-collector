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

package boxer

import "time"

type BoxerOptions interface {
	apply(*Boxer)
}

type boxerOptionFunc func(*Boxer)

func (f boxerOptionFunc) apply(box *Boxer) {
	f(box)
}

func WithBufferStorage(buffer Buffer) BoxerOptions {
	return boxerOptionFunc(func(b *Boxer) {
		b.buffer = buffer
	})
}

func WithInterval(interval time.Duration) BoxerOptions {
	return boxerOptionFunc(func(b *Boxer) {
		b.interval = interval
	})
}

func WithIntervalCount(intervalCount int64) BoxerOptions {
	return boxerOptionFunc(func(b *Boxer) {
		b.intervalCount = intervalCount
	})
}

func WithGrace(grace time.Duration) BoxerOptions {
	return boxerOptionFunc(func(b *Boxer) {
		b.grace = grace
	})
}

func WithTimeFunc(timefunc TimeFunc) BoxerOptions {
	return boxerOptionFunc(func(b *Boxer) {
		b.timefunc = timefunc
	})
}
