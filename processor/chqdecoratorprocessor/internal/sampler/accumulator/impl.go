package accumulator

import "fmt"

type AccumulatorImpl[T int64 | float64] struct {
	buckets []T
	count   uint64
	sum     []T
}

var (
	_ Accumulator[float64] = &AccumulatorImpl[float64]{}
	_ Accumulator[int64]   = &AccumulatorImpl[int64]{}
)

func NewAccumulatorImlp[T int64 | float64](buckets []T) *AccumulatorImpl[T] {
	return &AccumulatorImpl[T]{
		buckets: buckets,
		sum:     make([]T, len(buckets)),
	}
}

func (a *AccumulatorImpl[T]) Add(value []T) error {
	if len(value) != len(a.buckets) {
		return fmt.Errorf("value length does not match buckets length")
	}
	for i, v := range value {
		a.sum[i] += v
	}
	a.count++
	return nil
}

func (a *AccumulatorImpl[T]) Buckets() []T {
	return a.buckets
}

func (a *AccumulatorImpl[T]) Count() uint64 {
	return a.count
}

func (a *AccumulatorImpl[T]) Sum() []T {
	return a.sum
}

func (a *AccumulatorImpl[T]) Avg() []T {
	ret := make([]T, len(a.buckets))
	if a.count == 0 {
		return ret
	}
	for i := range a.buckets {
		ret[i] = a.sum[i] / T(a.count)
	}
	return ret
}
