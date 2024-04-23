package chqdecoratorprocessor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type decoratorSpansProcessor struct {
	telemetry *decoratorProcessorTelemetry
	logger    *zap.Logger
}

func newDecoratorSpansProcessor(set processor.CreateSettings, _ *Config) (*decoratorSpansProcessor, error) {
	var err error
	dsp := &decoratorSpansProcessor{
		logger: set.Logger,
	}

	dpt, err := newDecoratorProcessorTelemetry(set)
	if err != nil {
		return nil, fmt.Errorf("error creating chqdecorator processor telemetry: %w", err)
	}
	dsp.telemetry = dpt

	set.Logger.Info(
		"Decorator processor configured",
	)

	return dsp, nil
}

func (dmp *decoratorSpansProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	dmp.logger.Info("received and passed traces", zap.Int("num_spans", td.SpanCount()))
	return td, nil
}
