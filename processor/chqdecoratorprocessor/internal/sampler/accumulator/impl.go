package accumulator

import "fmt"

type AggregatorImpl[T int64 | float64] struct {
	buckets []T
	count   uint64
	sum     []T
}

var (
	_ Accumulator[float64] = &AggregatorImpl[float64]{}
	_ Accumulator[int64]   = &AggregatorImpl[int64]{}
)

func NewAggregatorImpl[T int64 | float64](buckets []T) *AggregatorImpl[T] {
	return &AggregatorImpl[T]{
		buckets: buckets,
		sum:     make([]T, len(buckets)),
	}
}

func (a *AggregatorImpl[T]) Add(value []T) error {
	if len(value) != len(a.buckets) {
		return fmt.Errorf("value length does not match buckets length")
	}
	for i, v := range value {
		a.sum[i] += v
	}
	a.count++
	return nil
}

func (a *AggregatorImpl[T]) Buckets() []T {
	return a.buckets
}

func (a *AggregatorImpl[T]) Count() uint64 {
	return a.count
}

func (a *AggregatorImpl[T]) Sum() []T {
	return a.sum
}

func (a *AggregatorImpl[T]) Avg() []T {
	ret := make([]T, len(a.buckets))
	if a.count == 0 {
		return ret
	}
	for i := range a.buckets {
		ret[i] = a.sum[i] / T(a.count)
	}
	return ret
}
