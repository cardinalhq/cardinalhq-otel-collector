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

package chqkubeeventsexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"io"
	"net/http"
	"regexp"
	"time"
)

type InvolvedObject struct {
	Kind      string            `json:"kind"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Metadata  map[string]string `json:"metadata"`
}

type KubernetesEvent struct {
	InvolvedObject InvolvedObject `json:"involvedObject"`
	Timestamp      string         `json:"timestamp"`
	Message        string         `json:"message"`
	Reason         string         `json:"reason"`
	Type           string         `json:"type"`
}

const (
	Kind           = "kind"
	Event          = "event"
	Name           = "name"
	Namespace      = "namespace"
	FirstTimestamp = "firstTimestamp"
	Message        = "message"
	Reason         = "reason"
	Type           = "type"
	Normal         = "Normal"
	Warning        = "Warning"
	Image          = "image"
)

func (e *kubeEventsExporter) ConsumeLogs(ctx context.Context, pl plog.Logs) error {
	resourceLogs := pl.ResourceLogs()
	events := make([]KubernetesEvent, 0)
	for i := 0; i < resourceLogs.Len(); i++ {
		resourceLog := resourceLogs.At(i)
		scopeLogs := resourceLog.ScopeLogs()

		for j := 0; j < scopeLogs.Len(); j++ {
			scopeLog := scopeLogs.At(j)
			logs := scopeLog.LogRecords()
			for k := 0; k < logs.Len(); k++ {
				lr := logs.At(k)
				rawValue := lr.Body().AsRaw()
				e.logger.Info("Received kube event", zap.Any("body", rawValue))

				if _, ok := rawValue.(map[string]interface{}); ok {
					bodyMap := lr.Body().Map().AsRaw()
					e.logger.Info("Received kube event", zap.Any("body", bodyMap))
					if object, objectPresent := bodyMap["object"]; objectPresent {
						if objectMap, objectIsAMap := object.(map[string]interface{}); objectIsAMap {
							topLevelKind, topLevelKindPresent := objectMap["kind"]
							if topLevelKindPresent && topLevelKind == "Event" {
								if involvedObject, involvedObjectPresent := objectMap["involvedObject"]; involvedObjectPresent {
									if involvedObjectMap, involvedObjectMapPresent := involvedObject.(map[string]interface{}); involvedObjectMapPresent {
										kind, kindOk := involvedObjectMap[Kind].(string)
										name, nameOk := involvedObjectMap[Name].(string)
										namespace, namespaceOk := involvedObjectMap[Namespace].(string)

										if kindOk && nameOk && namespaceOk {
											timestamp, timestampOk := objectMap[FirstTimestamp].(string)
											message, messageOk := objectMap[Message].(string)
											reason, reasonOk := objectMap[Reason].(string)
											eventType, typeOk := objectMap[Type].(string)

											if timestampOk && messageOk && reasonOk && typeOk {
												involved := InvolvedObject{
													Kind:      kind,
													Name:      name,
													Namespace: namespace,
												}

												kubeEvent := KubernetesEvent{
													InvolvedObject: involved,
													Timestamp:      timestamp,
													Message:        message,
													Reason:         reason,
													Type:           eventType,
												}
												switch kubeEvent.Type {
												case Normal:
													image := extractImageURL(kubeEvent.Message)
													if image != "" {
														if kubeEvent.InvolvedObject.Metadata == nil {
															kubeEvent.InvolvedObject.Metadata = make(map[string]string)
														}
														kubeEvent.InvolvedObject.Metadata[Image] = image
														events = append(events, kubeEvent)
													}

												case Warning:
													events = append(events, kubeEvent)

												default:
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	if len(events) > 0 {
		e.exportEvents(events)
	}
	return nil
}

func (e *kubeEventsExporter) sendAsync(kubeEvents []KubernetesEvent) {
	go func() {
		ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err := e.postKubeEvent(ctxWithTimeout, kubeEvents)
		if err != nil {
			e.logger.Error("Failed to send kube event", zap.Error(err))
		}
	}()
}

func extractImageURL(message string) string {
	regex := regexp.MustCompile(`Successfully pulled image "(.*?)"`)
	matches := regex.FindStringSubmatch(message)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func (e *kubeEventsExporter) postKubeEvent(ctx context.Context, events []KubernetesEvent) error {
	endpoint := e.config.Endpoint + "/api/v1/kubeEvents"
	b, err := json.Marshal(events)
	e.logger.Info("Sending kube events", zap.Int("count", len(events)), zap.String("endpoint", endpoint),
		zap.String("events", string(b)))
	if err != nil {
		return fmt.Errorf("failed to marshal kube events: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			e.logger.Warn("Request canceled", zap.Error(ctx.Err()))
			return nil
		}
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		e.logger.Error("Failed to send kube events",
			zap.Int("status", resp.StatusCode),
			zap.String("body", string(body)))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
