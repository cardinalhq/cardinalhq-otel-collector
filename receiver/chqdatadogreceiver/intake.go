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
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqtagcacheextension"
	"github.com/mitchellh/mapstructure"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.uber.org/zap"
)

type datadogIntake struct {
	APIKey           string                      `json:"apiKey"`
	InternalHostname string                      `json:"internalHostname"`
	Meta             datadogIntakeMeta           `json:"meta"`
	HostTags         map[string][]string         `json:"host-tags"`
	IntakeEvents     map[string][]map[string]any `json:"events"`
}

type datadogIntakeMeta struct {
	Hostname string `json:"hostname"`
}

// Event holds an intakeEvent (w/ serialization to DD agent 5 intake format)
type intakeEvent struct {
	Title          string   `mapstructure:"msg_title"`
	Text           string   `mapstructure:"msg_text"`
	Ts             int64    `mapstructure:"timestamp"`
	Priority       string   `mapstructure:"priority,omitempty"`
	Host           string   `mapstructure:"host"`
	Tags           []string `mapstructure:"tags,omitempty"`
	AlertType      string   `mapstructure:"alert_type,omitempty"`
	AggregationKey string   `mapstructure:"aggregation_key,omitempty"`
	SourceTypeName string   `mapstructure:"source_type_name,omitempty"`
	EventType      string   `mapstructure:"event_type,omitempty"`
}

func handleIntakePayload(req *http.Request) (ddIntake datadogIntake, err error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		err = fmt.Errorf("failed to read request body: %w", err)
		return datadogIntake{}, err
	}

	err = json.Unmarshal(body, &ddIntake)
	if err != nil {
		return datadogIntake{}, fmt.Errorf("failed to unmarshal intake body %v", err)
	}

	return
}

func (ddr *datadogReceiver) processIntake(ctx context.Context, apikey string, intake datadogIntake) error {
	logs, err := ddr.convertIntakeToLogs(apikey, intake)
	if err != nil {
		return err
	}
	if err := ddr.nextLogConsumer.ConsumeLogs(ctx, logs); err != nil {
		return err
	}

	return nil
}

func (ddr *datadogReceiver) processHostTags(intake datadogIntake, apikey string) {
	if len(intake.HostTags) == 0 {
		return // no host tags to update
	}

	if apikey == "" {
		ddr.gpLogger.Info("No API key in intake, cannot cache tags")
		return // no api key, nothing to do
	}

	hostname := intake.InternalHostname
	if hostname == "" {
		hostname = intake.Meta.Hostname
	}
	if hostname == "" {
		ddr.gpLogger.Info("No hostname in intake, cannot cache tags")
		return // probably not something we want
	}

	key := apikey + "/" + hostname

	tags := make([]chqtagcacheextension.Tag, 0, len(intake.HostTags))
	for _, v := range intake.HostTags {
		for _, tag := range v {
			items := strings.SplitN(tag, ":", 2)
			if len(items) != 2 {
				continue
			}
			tags = append(tags, chqtagcacheextension.Tag{
				Name:  items[0],
				Value: items[1],
			})
		}
	}

	if len(tags) == 0 {
		return // no host tags to update
	}

	if err := ddr.tagcacheExtension.PutCache(key, tags); err != nil {
		ddr.gpLogger.Error("Failed to put tags in cache", zap.Error(err))
	}
}

func (ddr *datadogReceiver) makeTags(apikey string, hostname string) (tags map[string]string) {
	tags = map[string]string{}

	if hostname == "" {
		return tags
	}

	cachedTags := newLocalTagCache()

	for _, tag := range cachedTags.FetchCache(ddr.tagcacheExtension, apikey, hostname) {
		tags[tag.Name] = tag.Value
	}

	return tags
}

func getHostname(tags map[string]string, intake datadogIntake) (hostname string) {
	hostname = intake.InternalHostname
	if hostname == "" {
		hostname = intake.Meta.Hostname
	}

	if hostname == "" && tags != nil {
		hostname = tags["host"]
	}
	return
}

func (ddr *datadogReceiver) convertIntakeToLogs(apikey string, intake datadogIntake) (plog.Logs, error) {
	t := pcommon.NewTimestampFromTime(time.Now())

	lm := plog.NewLogs()

	for eventType, events := range intake.IntakeEvents {
		for _, ej := range events {
			var event intakeEvent
			err := mapstructure.Decode(ej, &event)
			if err != nil {
				continue
			}

			kvm := splitTagSlice(event.Tags)

			hostname := event.Host
			if hostname == "" {
				hostname = getHostname(kvm, intake)
			}

			// create a new resource for every single item for now.
			// this can later be merged based on the keys, if we wish.
			rl := lm.ResourceLogs().AppendEmpty()
			rAttr := rl.Resource().Attributes()
			rl.SetSchemaUrl(semconv.SchemaURL)

			rAttr.PutStr(string(semconv.HostNameKey), hostname)

			scope := rl.ScopeLogs().AppendEmpty()
			sAttr := scope.Scope().Attributes()
			sAttr.PutStr(string(semconv.TelemetrySDKNameKey), "Datadog")

			logRecord := scope.LogRecords().AppendEmpty()
			lAttr := logRecord.Attributes()

			for k, v := range kvm {
				decorateItem(k, v, rAttr, sAttr, lAttr)
			}
			hosttags := ddr.makeTags(apikey, hostname)
			for k, v := range hosttags {
				decorateItem(k, v, rAttr, sAttr, lAttr)
			}

			jb, err := json.Marshal(ej)
			if err != nil {
				continue
			}
			logRecord.Body().SetStr(string(jb))

			//TODO: get this from the host payload
			logRecord.SetObservedTimestamp(t)
			logRecord.Attributes().PutStr("priority", event.Priority)
			logRecord.Attributes().PutStr("alert.type", event.AlertType)
			logRecord.Attributes().PutStr("aggregation.key", event.AggregationKey)
			logRecord.Attributes().PutStr("source.type.name", event.SourceTypeName)
			logRecord.Attributes().PutStr("event.type", event.EventType)
			logRecord.Attributes().PutStr(string(semconv.EventNameKey), "datadog."+eventType+"."+event.EventType)
		}
	}

	return lm, nil
}
