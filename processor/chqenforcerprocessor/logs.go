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

package chqenforcerprocessor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processorhelper"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/sampler"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/translate"
)

func getServiceName(r pcommon.Map) string {
	snk := string(semconv.ServiceNameKey)
	if serviceNameField, found := r.Get(snk); found {
		return serviceNameField.AsString()
	}
	return "unknown"
}

func getFingerprint(l pcommon.Map) int64 {
	fnk := translate.CardinalFieldFingerprint
	if fingerprintField, found := l.Get(fnk); found {
		return fingerprintField.Int()
	}
	return 0
}

func (e *chqEnforcer) ConsumeLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	if ld.ResourceLogs().Len() == 0 {
		return ld, nil
	}

	now := time.Now()

	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		serviceName := getServiceName(rl.Resource().Attributes())
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				fingerprint := getFingerprint(lr.Attributes())
				if e.pbPhase == chqpb.Phase_POST {
					shouldDrop := e.shouldDropLog(serviceName, fingerprint, rl, sl, lr)
					if shouldDrop {
						return true
					}
				}
				if e.config.DropDecorationAttributes {
					removeAllCardinalFields(lr.Attributes())
				}
				if err := e.recordLog(now, serviceName, fingerprint, rl, sl, lr); err != nil {
					e.logger.Error("Failed to record log", zap.Error(err))
				}
				return false
			})
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})

	if ld.ResourceLogs().Len() == 0 {
		return ld, processorhelper.ErrSkipProcessingData
	}
	return ld, nil
}

func (e *chqEnforcer) shouldDropLog(serviceName string, fingerprint int64, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) bool {
	fingerprintString := fmt.Sprintf("%d", fingerprint)
	rule_match := e.logSampler.SampleLogs(serviceName, fingerprintString, rl, sl, lr)
	return rule_match != ""
}

func removeAllCardinalFields(attr pcommon.Map) {
	attr.RemoveIf(func(k string, _ pcommon.Value) bool {
		return strings.HasPrefix(k, translate.CardinalFieldPrefix)
	})
}

func (e *chqEnforcer) recordLog(now time.Time, serviceName string, fingerprint int64, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) error {
	message := lr.Body().AsString()
	logSize := int64(len(message))

	// Derive tags from e.config.LogsConfig.StatsEnrichments based on the contextId, and then add tags to the LogStats.Tags Map
	tags := make(map[string]string)
	for _, enrichment := range e.config.LogsConfig.StatsEnrichments {
		if enrichment.Context == "log" {
			for _, tag := range enrichment.Tags {
				if tagValue, found := lr.Attributes().Get(tag); found {
					key := fmt.Sprintf("log.%s", tag)
					tags[key] = tagValue.AsString()
				}
			}
		} else if enrichment.Context == "resource" {
			for _, tag := range enrichment.Tags {
				if tagValue, found := rl.Resource().Attributes().Get(tag); found {
					key := fmt.Sprintf("resource.%s", tag)
					tags[key] = tagValue.AsString()
				}
			}
		} else if enrichment.Context == "scope" {
			for _, tag := range enrichment.Tags {
				if tagValue, found := sl.Scope().Attributes().Get(tag); found {
					key := fmt.Sprintf("scope.%s", tag)
					tags[key] = tagValue.AsString()
				}
			}
		}
	}

	rec := &chqpb.LogStats{
		ServiceName: serviceName,
		Fingerprint: fingerprint,
		Phase:       e.pbPhase,
		VendorId:    e.config.Statistics.Vendor,
		Count:       1,
		LogSize:     logSize,
		Exemplar:    message,
		Tags:        tags,
	}
	bucketpile, err := e.logstats.Record(now, rec, "", 1, logSize)
	if err != nil {
		return err
	}
	if bucketpile != nil {
		// TODO should send this to a channel and have a separate goroutine send it
		go e.sendLogStats(context.Background(), now, bucketpile)
	}
	return nil
}

func (e *chqEnforcer) sendLogStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*chqpb.LogStats) {
	wrapper := &chqpb.LogStatsReport{
		SubmittedAt: now.UnixMilli(),
		Stats:       []*chqpb.LogStats{},
	}
	for _, items := range *bucketpile {
		wrapper.Stats = append(wrapper.Stats, items...)
	}

	if err := e.postLogStats(ctx, wrapper); err != nil {
		e.logger.Error("Failed to send log stats", zap.Error(err))
	}
	e.logger.Info("Sent log stats", zap.Int("count", len(wrapper.Stats)))
}

func (e *chqEnforcer) postLogStats(ctx context.Context, wrapper *chqpb.LogStatsReport) error {
	b, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}
	e.logger.Info("Sending log stats", zap.Int("count", len(wrapper.Stats)), zap.Int("length", len(b)))
	endpoint := e.config.Statistics.Endpoint + "/api/v1/logstats"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func (e *chqEnforcer) updateLogsSampling(sc sampler.SamplerConfig) {
	e.logger.Info("Updating log sampling config", zap.String("vendor", e.vendor))
	e.logSampler.UpdateConfig(sc.Logs.Sampling, e.vendor, e.telemetrySettings)
}
