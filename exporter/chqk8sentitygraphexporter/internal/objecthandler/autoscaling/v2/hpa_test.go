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
	"testing"

	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

func TestConvertHPA(t *testing.T) {
	config := &converterconfig.Config{}

	t.Run("valid HorizontalPodAutoscaler/v2", func(t *testing.T) {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "autoscaling/v2",
				Kind:       "HorizontalPodAutoscaler",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-hpa",
				Namespace: "default",
			},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				MinReplicas: int32Ptr(2),
				MaxReplicas: 5,
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-deployment",
				},
			},
			Status: autoscalingv2.HorizontalPodAutoscalerStatus{
				CurrentReplicas: 3,
				DesiredReplicas: 4,
				Conditions: []autoscalingv2.HorizontalPodAutoscalerCondition{
					{
						Type:               autoscalingv2.ScalingActive,
						Status:             corev1.ConditionTrue,
						LastTransitionTime: metav1.Now(),
						Reason:             "Ready",
						Message:            "Scaling is active",
					},
				},
			},
		}

		unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(hpa)
		require.NoError(t, err)

		us := unstructured.Unstructured{Object: unstructuredObj}
		result, err := ConvertHPA(config, us)
		require.NoError(t, err)

		hpaSummary, ok := result.(*graphpb.AutoscalingHpaSummary)
		require.True(t, ok)

		assert.Equal(t, "test-hpa", hpaSummary.BaseObject.Name)
		assert.Equal(t, "default", hpaSummary.BaseObject.Namespace)
		assert.Equal(t, int32(2), hpaSummary.Spec.MinReplicas)
		assert.Equal(t, int32(5), hpaSummary.Spec.MaxReplicas)
		assert.Equal(t, "apps/v1", hpaSummary.Spec.Target.ApiVersion)
		assert.Equal(t, "Deployment", hpaSummary.Spec.Target.Kind)
		assert.Equal(t, "test-deployment", hpaSummary.Spec.Target.Name)
		assert.Equal(t, int32(3), hpaSummary.Status.CurrentReplicas)
		assert.Equal(t, int32(4), hpaSummary.Status.DesiredReplicas)
		require.Len(t, hpaSummary.Status.Conditions, 1)
		assert.Equal(t, "ScalingActive", hpaSummary.Status.Conditions[0].Type)
		assert.Equal(t, "True", hpaSummary.Status.Conditions[0].Status)
		assert.Equal(t, "Ready", hpaSummary.Status.Conditions[0].Reason)
		assert.Equal(t, "Scaling is active", hpaSummary.Status.Conditions[0].Message)
	})

	t.Run("invalid kind or version", func(t *testing.T) {
		us := unstructured.Unstructured{}
		us.SetKind("Pod")
		us.SetAPIVersion("v1")

		_, err := ConvertHPA(config, us)
		assert.Error(t, err)
		assert.Equal(t, "unstructured object is not a autoscaling/v2.HorizontalPodAutoscaler", err.Error())
	})

	t.Run("conversion failure", func(t *testing.T) {
		us := unstructured.Unstructured{}
		us.SetKind("HorizontalPodAutoscaler")
		us.SetAPIVersion("autoscaling/v2")
		us.Object = map[string]any{
			"invalid": "data",
		}

		_, err := ConvertHPA(config, us)
		assert.Error(t, err)
	})
}

func int32Ptr(i int32) *int32 {
	return &i
}
