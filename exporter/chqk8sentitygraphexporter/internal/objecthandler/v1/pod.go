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
	"slices"

	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/baseobj"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

func ConvertPod(config *converterconfig.Config, us unstructured.Unstructured) (baseobj.K8SObject, error) {
	if us.GetKind() != "Pod" || us.GetAPIVersion() != "v1" {
		return nil, errors.New("Not a v1 Pod")
	}
	var k8sobj corev1.Pod
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(us.Object, &k8sobj)
	if err != nil {
		return nil, err
	}

	podSummary := &graphpb.PodSummary{
		BaseObject: baseobj.Make(config, &us, us.GetAPIVersion(), us.GetKind()),
		Spec:       GetPodSpec(config, k8sobj.Spec),
		Status: &graphpb.PodStatus{
			Phase:           string(k8sobj.Status.Phase),
			PhaseMessage:    k8sobj.Status.Message,
			ContainerStatus: containerStatuses(k8sobj.Status),
			HostIps:         hostIPs(k8sobj),
			PodIps:          podIPs(k8sobj),
		},
	}

	if k8sobj.Status.StartTime != nil {
		t := k8sobj.Status.StartTime.Time.UTC()
		podSummary.Status.StartedAt = timestamppb.New(t)
	}

	return podSummary, nil
}

func containerStatuses(podStatus corev1.PodStatus) []*graphpb.PodContainerStatus {
	var statuses []*graphpb.PodContainerStatus
	for _, status := range podStatus.ContainerStatuses {
		statuses = append(statuses, containerStatus(status))
	}
	return statuses
}

func containerStatus(status corev1.ContainerStatus) *graphpb.PodContainerStatus {
	s := &graphpb.PodContainerStatus{
		Name: status.Name,
		Image: &graphpb.ImageSummary{
			Image:   status.Image,
			ImageId: status.ImageID,
		},
		Ready: status.Ready,
	}

	if status.State.Waiting != nil {
		switch status.State.Waiting.Reason {
		case "ImagePullBackOff":
			s.IsImagePullBackOff = true
		case "CrashLoopBackOff":
			s.IsCrashLoopBackOff = true
		}
	}
	if status.LastTerminationState.Terminated != nil {
		if status.LastTerminationState.Terminated.Reason == "OOMKilled" {
			s.WasOomKilled = true
		}
	}
	return s
}

func podIPs(pod corev1.Pod) []string {
	var ips []string
	for _, podIP := range pod.Status.PodIPs {
		ips = append(ips, podIP.IP)
	}
	slices.Sort(ips)
	return ips
}

func hostIPs(pod corev1.Pod) []string {
	var ips []string
	for _, hostIP := range pod.Status.HostIPs {
		ips = append(ips, hostIP.IP)
	}
	slices.Sort(ips)
	return ips
}
