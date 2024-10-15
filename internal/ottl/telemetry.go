package ottl

import "go.opentelemetry.io/otel/metric"

type DeferrableCounter interface {
	Add(delta int64, options ...metric.AddOption)
}

func CounterAdd(counter DeferrableCounter, delta int64, options ...metric.AddOption) {
	if counter == nil {
		return
	}
	counter.Add(delta, options...)
}
