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

package v1

import (
	"errors"

	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/baseobj"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
	v1pod "github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/v1"
)

func ConvertDaemonSet(config *converterconfig.Config, us unstructured.Unstructured) (baseobj.K8SObject, error) {
	if us.GetKind() != "DaemonSet" || us.GetAPIVersion() != "apps/v1" {
		return nil, errors.New("not a apps/v1 DaemonSet")
	}
	var k8sobj appsv1.DaemonSet
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(us.Object, &k8sobj)
	if err != nil {
		return nil, err
	}

	podSummary := &graphpb.AppsDaemonSetSummary{
		BaseObject: baseobj.Make(config, &us, us.GetAPIVersion(), us.GetKind()),
		Spec: &graphpb.AppsDaemonSetSpec{
			Template: &graphpb.AppsDaemonSetTemplate{
				Metadata: baseobj.Make(config, &k8sobj.Spec.Template.ObjectMeta, "v1", "Pod"),
				PodSpec:  v1pod.GetPodSpec(config, k8sobj.Spec.Template.Spec),
			},
		},
		Status: &graphpb.AppDaemonSetStatus{
			CurrentNumberScheduled: k8sobj.Status.CurrentNumberScheduled,
			DesiredNumberScheduled: k8sobj.Status.DesiredNumberScheduled,
			NumberAvailable:        k8sobj.Status.NumberAvailable,
			NumberMisscheduled:     k8sobj.Status.NumberMisscheduled,
			NumberReady:            k8sobj.Status.NumberReady,
			NumberUnavailable:      k8sobj.Status.NumberUnavailable,
			UpdatedNumberScheduled: k8sobj.Status.UpdatedNumberScheduled,
		},
	}
	return podSummary, nil
}
