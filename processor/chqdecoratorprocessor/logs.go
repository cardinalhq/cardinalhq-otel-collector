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

package chqdecoratorprocessor

import (
	"context"
	"os"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/fingerprinter"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type logProcessor struct {
	sampler    sampler.LogSampler
	logger     *zap.Logger
	finger     fingerprinter.Fingerprinter
	podName    string
	customerID string
	clusterID  string
}

func newLogsProcessor(set processor.Settings) (*logProcessor, error) {
	samp := sampler.NewLogSamplerImpl(context.Background(), set.Logger)

	lp := &logProcessor{
		logger:  set.Logger,
		sampler: samp,
		podName: os.Getenv("POD_NAME"),
	}

	set.Logger.Info("Decorator processor configured")

	lp.finger = fingerprinter.NewFingerprinter()

	return lp, nil
}

func (lp *logProcessor) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				log := sl.LogRecords().At(k)
				fingerprint, level, err := lp.finger.Fingerprint(log.Body().AsString())
				if err != nil {
					lp.logger.Debug("Error fingerprinting log", zap.Error(err))
					continue
				}
				log.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				log.Attributes().PutStr(translate.CardinalFieldLevel, level)
				log.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, lp.podName)
			}
		}
	}

	return ld, nil
}

func (lp *logProcessor) Shutdown(_ context.Context) error {
	return nil
}
