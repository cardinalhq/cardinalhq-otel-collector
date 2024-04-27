package accumulator

// All accumulators are treated as a histogram,  This might not be
// very memory efficient, but it lets us treat them almost identically
// in the rest of the code.
type Accumulator[T int64 | float64] interface {
	Add(value []T) error
	Buckets() []T
	Count() uint64
	Sum() []T
	Avg() []T
}
