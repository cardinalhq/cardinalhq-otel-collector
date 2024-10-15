package chqdecoratorprocessor

import (
	"context"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
	"go.opentelemetry.io/otel/metric"
)

type transformCounter struct {
	counter    metric.Int64Counter
	addOptions []metric.AddOption
}

var _ ottl.DeferrableCounter = (*transformCounter)(nil)

func (tc *transformCounter) Add(delta int64, options ...metric.AddOption) {
	tc.counter.Add(context.Background(), delta, append(tc.addOptions, options...)...)
}

func newTransformCounter(meter metric.Meter, name string, counterOptions []metric.Int64CounterOption, addOptions []metric.AddOption) (*transformCounter, error) {
	counter, err := meter.Int64Counter(name, counterOptions...)
	if err != nil {
		return nil, err
	}
	return &transformCounter{
		counter:    counter,
		addOptions: addOptions,
	}, nil
}
