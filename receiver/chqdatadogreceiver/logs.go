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

package datadogreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

type DDLog struct {
	DDSource string `json:"ddsource,omitempty"`
	DDTags   string `json:"ddtags,omitempty"`
	Message  string `json:"message,omitempty"`
	Hostname string `json:"hostname,omitempty"`
	Service  string `json:"service,omitempty"`
}

func handleLogsPayload(req *http.Request) (ret []*DDLog, err error) {
	ret = make([]*DDLog, 0)
	defer func() {
		_, errs := io.Copy(io.Discard, req.Body)
		err = errors.Join(err, errs, req.Body.Close())
	}()

	var ddLogs []*DDLog
	err = json.NewDecoder(req.Body).Decode(&ddLogs)
	if err != nil {
		return nil, err
	}
	return ddLogs, nil
}

func (ddr *datadogReceiver) processLogs(logs []*DDLog) error {
	for _, log := range logs {
		otelLog, err := ddr.convertLog(log)
		if err != nil {
			return err
		}
		if err := ddr.nextLogConsumer.ConsumeLogs(context.Background(), otelLog); err != nil {
			return err
		}
	}
	return nil
}

func (ddr *datadogReceiver) convertLog(log *DDLog) (plog.Logs, error) {
	lm := plog.NewLogs()
	rl := lm.ResourceLogs().AppendEmpty()
	rlAttr := rl.Resource().Attributes()
	rlAttr.PutStr(string(semconv.ServiceNameKey), log.Service)
	rlAttr.PutStr(string(semconv.HostNameKey), log.Hostname)
	ill := rl.ScopeLogs().AppendEmpty()
	logRecord := ill.LogRecords().AppendEmpty()
	logRecord.Body().SetStr(log.Message)
	logRecord.Attributes().PutStr("ddsource", log.DDSource)
	logRecord.Attributes().PutStr("ddtags", log.DDTags)
	logRecord.Attributes().PutStr("hostname", log.Hostname)
	logRecord.Attributes().PutStr("service", log.Service)
	return lm, nil
}
