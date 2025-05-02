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

package v2

import (
	"errors"

	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/baseobj"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

func ConvertHPA(config *converterconfig.Config, us unstructured.Unstructured) (baseobj.K8SObject, error) {
	if us.GetKind() != "HorizontalPodAutoscaler" || us.GetAPIVersion() != "autoscaling/v2" {
		return nil, errors.New("unstructured object is not a autoscaling/v2.HorizontalPodAutoscaler")
	}

	var k8sobj autoscalingv2.HorizontalPodAutoscaler
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(us.Object, &k8sobj)
	if err != nil {
		return nil, err
	}

	minReplicas := int32(1)
	if k8sobj.Spec.MinReplicas != nil {
		minReplicas = int32(*k8sobj.Spec.MinReplicas)
	}
	hpa := &graphpb.AutoscalingHpaSummary{
		BaseObject: baseobj.Make(config, &us, us.GetAPIVersion(), us.GetKind()),
		Spec: &graphpb.AutoscalingHpaSpec{
			MinReplicas: minReplicas,
			MaxReplicas: k8sobj.Spec.MaxReplicas,
			Target: &graphpb.AutoscalingHpaTarget{
				ApiVersion: k8sobj.Spec.ScaleTargetRef.APIVersion,
				Kind:       k8sobj.Spec.ScaleTargetRef.Kind,
				Name:       k8sobj.Spec.ScaleTargetRef.Name,
			},
		},
		Status: &graphpb.AutoscalingHpaStatus{
			CurrentReplicas: k8sobj.Status.CurrentReplicas,
			DesiredReplicas: k8sobj.Status.DesiredReplicas,
		},
	}

	for _, cond := range k8sobj.Status.Conditions {
		hpa.Status.Conditions = append(hpa.Status.Conditions, &graphpb.AutoscalingHpaCondition{
			Type:               string(cond.Type),
			Status:             string(cond.Status),
			LastTransitionTime: timestamppb.New(cond.LastTransitionTime.Time),
			Reason:             cond.Reason,
			Message:            cond.Message,
		})
	}

	return hpa, nil
}
