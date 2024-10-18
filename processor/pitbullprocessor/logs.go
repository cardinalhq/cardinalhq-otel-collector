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

package pitbullprocessor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlresource"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlscope"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processorhelper"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/cardinalhq/cardinalhq-otel-collector/internal/chqpb"
	"github.com/cardinalhq/cardinalhq-otel-collector/internal/ottl"
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

func (e *pitbull) ConsumeLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	e.Lock()
	defer e.Unlock()

	if ld.ResourceLogs().Len() == 0 {
		return ld, nil
	}

	environment := translate.EnvironmentFromEnv()
	now := time.Now()
	emptySlice := pcommon.NewSlice()

	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		transformCtx := ottlresource.NewTransformContext(rl.Resource(), rl)
		e.logTransformations.ExecuteResourceTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
		if _, found := rl.Resource().Attributes().Get(translate.CardinalFieldDropMarker); found {
			return true
		}

		serviceName := getServiceName(rl.Resource().Attributes())
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			transformCtx := ottlscope.NewTransformContext(sl.Scope(), rl.Resource(), rl)
			e.logTransformations.ExecuteScopeTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
			if _, found := sl.Scope().Attributes().Get(translate.CardinalFieldDropMarker); found {
				return true
			}

			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				fingerprint, level, err := e.logFingerprinter.Fingerprint(lr.Body().AsString())
				if err != nil {
					e.logger.Debug("Error fingerprinting log", zap.Error(err))
					return true
				}

				lr.Attributes().PutInt(translate.CardinalFieldFingerprint, fingerprint)
				lr.Attributes().PutStr(translate.CardinalFieldLevel, level)
				lr.Attributes().PutStr(translate.CardinalFieldDecoratorPodName, e.podName)
				lr.Attributes().PutStr(translate.CardinalFieldCustomerID, environment.CustomerID())
				lr.Attributes().PutStr(translate.CardinalFieldCollectorID, environment.CollectorID())

				transformCtx := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)
				e.logTransformations.ExecuteLogTransforms(e.ottlProcessed, transformCtx, ottl.VendorID(e.vendor), emptySlice)
				if _, found := lr.Attributes().Get(translate.CardinalFieldDropMarker); found {
					return true
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

func removeAllCardinalFields(attr pcommon.Map) {
	attr.RemoveIf(func(k string, _ pcommon.Value) bool {
		return strings.HasPrefix(k, translate.CardinalFieldPrefix)
	})
}

func (e *pitbull) recordLog(now time.Time, serviceName string, fingerprint int64, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) error {
	message := lr.Body().AsString()
	logSize := int64(len(message))

	// Derive tags from e.config.LogsConfig.StatsEnrichments based on the contextId, and then add tags to the LogStats.Tags Map

	tags := e.processEnrichments(e.config.LogsConfig.StatsEnrichments, map[string]pcommon.Map{
		"resource": rl.Resource().Attributes(),
		"scope":    sl.Scope().Attributes(),
		"log":      lr.Attributes(),
	})

	rec := &chqpb.LogStats{
		ServiceName: serviceName,
		Fingerprint: fingerprint,
		Phase:       e.pbPhase,
		VendorId:    e.config.Statistics.Vendor,
		Count:       1,
		LogSize:     logSize,
		Exemplar: &chqpb.LogExemplar{
			ResourceTags: ToMap(rl.Resource().Attributes()),
			ScopeTags:    ToMap(sl.Scope().Attributes()),
			LogTags:      ToMap(lr.Attributes()),
			Exemplar:     message,
		},
		Tags: tags,
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

func (e *pitbull) sendLogStats(ctx context.Context, now time.Time, bucketpile *map[uint64][]*chqpb.LogStats) {
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

func (e *pitbull) postLogStats(ctx context.Context, wrapper *chqpb.LogStatsReport) error {
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
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		e.logger.Error("Failed to send log stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func (c *pitbull) updateLogTransformations(sc ottl.SamplerConfig) {
	c.Lock()
	defer c.Unlock()
	c.logger.Info("Updating log transformations", zap.Int("num_decorators", len(sc.Logs.Decorators)))
	newTransformations := ottl.NewTransformations(c.logger)

	c.logger.Info("json log config", zap.Any("logDecorators", sc.Logs))

	for _, decorator := range sc.Logs.Decorators {
		transformations, err := ottl.ParseTransformations(decorator, c.logger)
		if err != nil {
			c.logger.Error("Error parsing log transformation", zap.Error(err))
		} else {
			newTransformations = ottl.MergeWith(newTransformations, transformations)
		}
	}

	oldTransformation := c.logTransformations
	c.logTransformations = newTransformations
	oldTransformation.Stop()
}
