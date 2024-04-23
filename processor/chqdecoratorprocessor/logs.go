package chqdecoratorprocessor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type filterLogProcessor struct {
	telemetry *decoratorProcessorTelemetry
	logger    *zap.Logger
}

func newDecoratorLogsProcessor(set processor.CreateSettings, _ *Config) (*filterLogProcessor, error) {
	var err error
	dsp := &filterLogProcessor{
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

func (dmp *filterLogProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	dmp.logger.Info("received and passed logs", zap.Int("num_logs", ld.ResourceLogs().Len()))
	return ld, nil
}
