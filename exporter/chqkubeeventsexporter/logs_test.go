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
	"context"
	"encoding/json"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKubeEventsExporter_ConsumeLogs_WarningEvent(t *testing.T) {
	payload := `{
		"object": {
			"apiVersion": "v1",
			"count": 4,
			"eventTime": null,
			"firstTimestamp": "2024-11-19T19:27:22Z",
			"involvedObject": {
				"apiVersion": "v1",
				"fieldPath": "spec.containers{memory-hog}",
				"kind": "Pod",
				"name": "memory-hog-77bc84f698-8ddlj",
				"namespace": "chq-demo-apps",
				"resourceVersion": "36606655",
				"uid": "e69d4c5d-3334-454b-8886-f31bc5139dbe"
			},
			"kind": "Event",
			"lastTimestamp": "2024-11-19T19:28:06Z",
			"message": "Back-off restarting failed container memory-hog in pod memory-hog-77bc84f698-8ddlj_chq-demo-apps(e69d4c5d-3334-454b-8886-f31bc5139dbe)",
			"metadata": {
				"creationTimestamp": "2024-11-19T19:27:22Z",
				"managedFields": [
					{
						"apiVersion": "v1",
						"fieldsType": "FieldsV1",
						"fieldsV1": {
							"f:count": {},
							"f:firstTimestamp": {},
							"f:involvedObject": {},
							"f:lastTimestamp": {},
							"f:message": {},
							"f:reason": {},
							"f:reportingComponent": {},
							"f:reportingInstance": {},
							"f:source": { "f:component": {}, "f:host": {} },
							"f:type": {}
						},
						"manager": "kubelet",
						"operation": "Update",
						"time": "2024-11-19T19:28:06Z"
					}
				],
				"name": "memory-hog-77bc84f698-8ddlj.1809753e7d7f8595",
				"namespace": "chq-demo-apps",
				"resourceVersion": "36607031",
				"uid": "fcb5016d-ada6-464c-96c7-d143a305b38f"
			},
			"reason": "BackOff",
			"reportingComponent": "kubelet",
			"reportingInstance": "ip-10-181-10-113.us-east-2.compute.internal",
			"source": {
				"component": "kubelet",
				"host": "ip-10-181-10-113.us-east-2.compute.internal"
			},
			"type": "Warning"
		},
		"type": "MODIFIED"
	}`

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	logRecord := scopeLogs.LogRecords().AppendEmpty()

	var body map[string]interface{}
	err := json.Unmarshal([]byte(payload), &body)
	assert.NoError(t, err)

	err = logRecord.Body().SetEmptyMap().FromRaw(body)
	assert.NoError(t, err)

	exporter := &kubeEventsExporter{
		logger: zap.NewNop(),
	}

	events := make([]KubernetesEvent, 0)
	events = append(events, KubernetesEvent{
		InvolvedObject: InvolvedObject{
			Kind:      "Pod",
			Name:      "memory-hog-77bc84f698-8ddlj",
			Namespace: "chq-demo-apps",
		},
		Timestamp: "2024-11-19T19:27:22Z",
		Message:   "Back-off restarting failed container memory-hog in pod memory-hog-77bc84f698-8ddlj_chq-demo-apps(e69d4c5d-3334-454b-8886-f31bc5139dbe)",
		Reason:    "BackOff",
		Type:      "Warning",
	})

	var funcCalled bool
	mockSend := func(event []KubernetesEvent) {
		funcCalled = true
		assert.Equal(t, events, event)
	}
	exporter.exportEvents = mockSend

	err = exporter.ConsumeLogs(context.Background(), logs)
	assert.True(t, funcCalled)
	assert.NoError(t, err)
}
func TestKubeEventsExporter_ConsumeLogs_NormalEvent(t *testing.T) {
	payload := `{
		"object": {
			"apiVersion": "v1",
			"count": 1,
			"eventTime": null,
			"firstTimestamp": "2024-11-19T23:21:17Z",
			"involvedObject": {
				"apiVersion": "v1",
				"fieldPath": "spec.containers{android-api}",
				"kind": "Pod",
				"name": "android-api-699749d6b5-86rcj",
				"namespace": "chq-demo-apps",
				"resourceVersion": "36675819",
				"uid": "dcc6d1ae-bef5-411c-a843-337290a78447"
			},
			"kind": "Event",
			"lastTimestamp": "2024-11-19T23:21:17Z",
			"message": "Successfully pulled image \"033263751764.dkr.ecr.us-east-2.amazonaws.com/cardinalhq/demo/android-api:latest\" in 5.77s (5.77s including waiting). Image size: 260589990 bytes.",
			"metadata": {
				"creationTimestamp": "2024-11-19T23:21:17Z",
				"managedFields": [
					{
						"apiVersion": "v1",
						"fieldsType": "FieldsV1",
						"fieldsV1": {
							"f:count": {},
							"f:firstTimestamp": {},
							"f:involvedObject": {},
							"f:lastTimestamp": {},
							"f:message": {},
							"f:reason": {},
							"f:reportingComponent": {},
							"f:reportingInstance": {},
							"f:source": { "f:component": {}, "f:host": {} },
							"f:type": {}
						},
						"manager": "kubelet",
						"operation": "Update",
						"time": "2024-11-19T23:21:17Z"
					}
				],
				"name": "android-api-699749d6b5-86rcj.18098202423c854e",
				"namespace": "chq-demo-apps",
				"resourceVersion": "36675881",
				"uid": "efbc1795-d10b-446e-ad10-0751a6ae49dd"
			},
			"reason": "Pulled",
			"reportingComponent": "kubelet",
			"reportingInstance": "ip-10-181-10-113.us-east-2.compute.internal",
			"source": {
				"component": "kubelet",
				"host": "ip-10-181-10-113.us-east-2.compute.internal"
			},
			"type": "Normal"
		},
		"type": "ADDED"
	}`

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	logRecord := scopeLogs.LogRecords().AppendEmpty()

	var body map[string]interface{}
	err := json.Unmarshal([]byte(payload), &body)
	assert.NoError(t, err)

	err = logRecord.Body().SetEmptyMap().FromRaw(body)
	assert.NoError(t, err)

	exporter := &kubeEventsExporter{
		logger: zap.NewNop(),
	}

	events := make([]KubernetesEvent, 0)
	events = append(events, KubernetesEvent{
		InvolvedObject: InvolvedObject{
			Kind:      "Pod",
			Name:      "android-api-699749d6b5-86rcj",
			Namespace: "chq-demo-apps",
			Metadata: map[string]string{
				"image": "033263751764.dkr.ecr.us-east-2.amazonaws.com/cardinalhq/demo/android-api:latest",
			},
		},
		Timestamp: "2024-11-19T23:21:17Z",
		Message:   "Successfully pulled image \"033263751764.dkr.ecr.us-east-2.amazonaws.com/cardinalhq/demo/android-api:latest\" in 5.77s (5.77s including waiting). Image size: 260589990 bytes.",
		Reason:    "Pulled",
		Type:      "Normal",
	})

	var funcCalled bool
	mockSend := func(event []KubernetesEvent) {
		funcCalled = true
		assert.Equal(t, events, event)
	}
	exporter.exportEvents = mockSend

	err = exporter.ConsumeLogs(context.Background(), logs)
	assert.True(t, funcCalled)
	assert.NoError(t, err)
}

func TestKubeEventsExporter_ConsumeLogs_NoActionOnRegularEvent(t *testing.T) {
	payload := `{
		"object": {
			"apiVersion": "v1",
			"count": 1,
			"eventTime": null,
			"firstTimestamp": "2024-11-19T23:21:17Z",
			"involvedObject": {
				"apiVersion": "v1",
				"fieldPath": "spec.containers{android-api}",
				"kind": "Pod",
				"name": "android-api-699749d6b5-86rcj",
				"namespace": "chq-demo-apps",
				"resourceVersion": "36675819",
				"uid": "dcc6d1ae-bef5-411c-a843-337290a78447"
			},
			"kind": "Event",
			"lastTimestamp": "2024-11-19T23:21:17Z",
			"message": "Just a regular event",
			"metadata": {
				"creationTimestamp": "2024-11-19T23:21:17Z",
				"managedFields": [
					{
						"apiVersion": "v1",
						"fieldsType": "FieldsV1",
						"fieldsV1": {
							"f:count": {},
							"f:firstTimestamp": {},
							"f:involvedObject": {},
							"f:lastTimestamp": {},
							"f:message": {},
							"f:reason": {},
							"f:reportingComponent": {},
							"f:reportingInstance": {},
							"f:source": { "f:component": {}, "f:host": {} },
							"f:type": {}
						},
						"manager": "kubelet",
						"operation": "Update",
						"time": "2024-11-19T23:21:17Z"
					}
				],
				"name": "android-api-699749d6b5-86rcj.18098202423c854e",
				"namespace": "chq-demo-apps",
				"resourceVersion": "36675881",
				"uid": "efbc1795-d10b-446e-ad10-0751a6ae49dd"
			},
			"reason": "Pulled",
			"reportingComponent": "kubelet",
			"reportingInstance": "ip-10-181-10-113.us-east-2.compute.internal",
			"source": {
				"component": "kubelet",
				"host": "ip-10-181-10-113.us-east-2.compute.internal"
			},
			"type": "Normal"
		},
		"type": "ADDED"
	}`

	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	logRecord := scopeLogs.LogRecords().AppendEmpty()

	var body map[string]interface{}
	err := json.Unmarshal([]byte(payload), &body)
	assert.NoError(t, err)

	err = logRecord.Body().SetEmptyMap().FromRaw(body)
	assert.NoError(t, err)

	exporter := &kubeEventsExporter{
		logger: zap.NewNop(),
	}

	var funcCalled bool
	mockSend := func(event []KubernetesEvent) {
		funcCalled = true
	}
	exporter.exportEvents = mockSend

	err = exporter.ConsumeLogs(context.Background(), logs)
	assert.False(t, funcCalled, "sendAsync should not be called for a regular event message")
	assert.NoError(t, err)
}
