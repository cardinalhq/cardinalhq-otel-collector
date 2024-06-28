package badgerbox

import "time"

type BoxOptions interface {
	apply(*Box)
}

type boxOptionFunc func(*Box)

func (f boxOptionFunc) apply(box *Box) {
	f(box)
}

func newBoxOptions(opts []BoxOptions) *Box {
	box := &Box{}
	for _, opt := range opts {
		opt.apply(box)
	}
	return box
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
