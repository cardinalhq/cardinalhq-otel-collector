// Copyright 2024-2025 CardinalHQ, Inc
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

package chqexemplarprocessor

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cardinalhq/oteltools/pkg/chqpb"
	"github.com/cardinalhq/oteltools/pkg/translate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"strconv"
	"time"
)

func getFingerprint(l pcommon.Map) int64 {
	fnk := translate.CardinalFieldFingerprint
	if fingerprintField, found := l.Get(fnk); found {
		return fingerprintField.Int()
	}
	return 0
}

func (p *exemplarProcessor) sendSketches(list *chqpb.ServiceLogCountList) error {
	if len(list.Sketches) > 0 {
		p.logger.Info("Sending log stats", zap.Int("sketches", len(list.Sketches)))
		b, err := proto.Marshal(list)
		if err != nil {
			return err
		}
		endpoint := p.config.Endpoint + "/api/v1/serviceLogCounts"
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(b))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/x-protobuf")

		resp, err := p.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
			p.logger.Error("Failed to send span stats", zap.Int("status", resp.StatusCode), zap.String("body", string(body)))
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}
	return nil
}

func (p *exemplarProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	if !p.config.Reporting.Logs.Enabled {
		return ld, nil
	}

	for i := range ld.ResourceLogs().Len() {
		rl := ld.ResourceLogs().At(i)
		resourceAttributes := rl.Resource().Attributes()
		cid := orgIdFromResource(resourceAttributes)
		tenant := p.getTenant(cid)
		serviceLogCountsCache, sok := p.serviceLogCounts.Load(cid)
		if !sok {
			p.logger.Info("Creating new log sketch cache", zap.String("cid", cid))
			serviceLogCountsCache = chqpb.NewServiceLogCountsCache(5*time.Minute, cid, p.sendSketches)
			p.serviceLogCounts.Store(cid, serviceLogCountsCache)
		}

		for j := range rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(j)
			for k := range sl.LogRecords().Len() {
				lr := sl.LogRecords().At(k)
				fingerprint := getFingerprint(lr.Attributes())
				p.addLogExemplar(tenant, rl, sl, lr, fingerprint)
				serviceLogCountsCache.Update(rl.Resource(), lr)
			}
		}
	}

	return ld, nil
}

func (p *exemplarProcessor) addLogExemplar(tenant *Tenant, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord, fingerprint int64) {
	extraKeys := []string{
		translate.CardinalFieldFingerprint, strconv.FormatInt(fingerprint, 10),
	}
	keys, exemplarKey := computeExemplarKey(rl.Resource(), extraKeys)
	if tenant.logCache.Contains(exemplarKey) {
		return
	}
	exemplarRecord := toLogExemplar(rl, sl, lr)
	tenant.logCache.Put(exemplarKey, keys, exemplarRecord)
}

func toLogExemplar(rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) plog.Logs {
	exemplarRecord := plog.NewLogs()
	copyRl := exemplarRecord.ResourceLogs().AppendEmpty()
	rl.Resource().CopyTo(copyRl.Resource())
	copySl := copyRl.ScopeLogs().AppendEmpty()
	sl.Scope().CopyTo(copySl.Scope())
	copyLr := copySl.LogRecords().AppendEmpty()
	lr.CopyTo(copyLr)

	return exemplarRecord
}
