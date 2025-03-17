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
	"strings"

	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
	mapset "github.com/deckarep/golang-set/v2"
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
	var pod corev1.Pod
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(us.Object, &pod)
	if err != nil {
		return nil, err
	}

	podSummary := &graphpb.PodSummary{
		BaseObject:         baseobj.BaseFromUnstructured(config, us),
		Phase:              string(pod.Status.Phase),
		PhaseMessage:       pod.Status.Message,
		ServiceAccountName: pod.Spec.ServiceAccountName,
	}

	if pod.Status.StartTime != nil {
		t := pod.Status.StartTime.Time.UTC()
		podSummary.StartedAt = timestamppb.New(t)
	}

	setupContainers(config, pod, podSummary)
	updateContainersFromStatuses(pod, podSummary)

	podSummary.HostIps = hostIPs(pod)
	podSummary.PodIps = podIPs(pod)

	return podSummary, nil
}

func setupContainers(conf *converterconfig.Config, pod corev1.Pod, podSummary *graphpb.PodSummary) {
	for _, container := range pod.Spec.Containers {
		podSummary.Containers = append(podSummary.Containers, &graphpb.PodContainerSummary{
			Name: container.Name,
			Image: &graphpb.ImageSummary{
				Image: container.Image,
			},
			Resources:      convertPodResources(container.Resources),
			ConfigMapNames: containerConfigMapNames(conf, pod, container),
			SecretNames:    containerSecretNames(conf, pod, container),
		})
	}
	slices.SortFunc(podSummary.Containers, func(a, b *graphpb.PodContainerSummary) int {
		return strings.Compare(a.Name, b.Name)
	})
}

// containerConfigMapNames returns the list of ConfigMap names used by the container,
// either in an env var or in a volume mount.
func containerConfigMapNames(conf *converterconfig.Config, pod corev1.Pod, container corev1.Container) []string {
	seen := mapset.NewSet[string]()
	containerConfigMapNamesFromEnv(seen, container)
	containerConfigMapNamesFromVolumes(conf, seen, pod.Spec.Volumes, container)
	if seen.Cardinality() == 0 {
		return nil
	}
	ret := seen.ToSlice()
	slices.Sort(ret)
	return ret
}

// containerConfigMapNamesFromEnv returns the list of ConfigMap envar references.
func containerConfigMapNamesFromEnv(seen mapset.Set[string], container corev1.Container) {
	for _, envVar := range container.Env {
		if envVar.ValueFrom != nil && envVar.ValueFrom.ConfigMapKeyRef != nil {
			seen.Add(envVar.ValueFrom.ConfigMapKeyRef.Name)
		}
	}
	for _, ef := range container.EnvFrom {
		if ef.ConfigMapRef != nil {
			seen.Add(ef.ConfigMapRef.Name)
		}
	}
}

// containerConfigMapNamesFromVolumes returns the list of ConfigMap names used by the container,
// filtering out common system configmaps like "kube-root-ca.crt".
func containerConfigMapNamesFromVolumes(conf *converterconfig.Config, seen mapset.Set[string], podVolumes []corev1.Volume, container corev1.Container) {
	for _, vm := range container.VolumeMounts {
		for _, vol := range podVolumes {
			if vol.Name == vm.Name && vol.ConfigMap != nil {
				if isFilteredConfigMapName(conf, vol.ConfigMap.Name) {
					continue
				}
				seen.Add(vol.ConfigMap.Name)
			}
		}
	}
}

// containerSecretNames returns the list of Secret names used by the container,
// either in an env var or in a volume mount.
func containerSecretNames(conf *converterconfig.Config, pod corev1.Pod, container corev1.Container) []string {
	seen := mapset.NewSet[string]()
	containerSecretNamesFromEnv(seen, container)
	containerSecretNamesFromVolumes(conf, seen, pod.Spec.Volumes, container)
	if seen.Cardinality() == 0 {
		return nil
	}
	ret := seen.ToSlice()
	slices.Sort(ret)
	return ret
}

// containerSecretNamesFromEnv returns the list of Secret envar references.
func containerSecretNamesFromEnv(seen mapset.Set[string], container corev1.Container) {
	for _, envVar := range container.Env {
		if envVar.ValueFrom != nil && envVar.ValueFrom.SecretKeyRef != nil {
			seen.Add(envVar.ValueFrom.SecretKeyRef.Name)
		}
	}
	for _, ef := range container.EnvFrom {
		if ef.SecretRef != nil {
			seen.Add(ef.SecretRef.Name)
		}
	}
}

// containerSecretNamesFromVolumes adds the list of Secret names used by the container,
// to the given set, filtering out common system secrets.
func containerSecretNamesFromVolumes(conf *converterconfig.Config, seen mapset.Set[string], podVolumes []corev1.Volume, container corev1.Container) {
	for _, vm := range container.VolumeMounts {
		for _, vol := range podVolumes {
			if vol.Name == vm.Name && vol.Secret != nil {
				if isFilteredSecretName(conf, vol.Secret.SecretName) {
					continue
				}
				seen.Add(vol.Secret.SecretName)
			}
		}
	}
}

func convertPodResources(resources corev1.ResourceRequirements) map[string]string {
	requests := make(map[string]string)
	for resourceName, quantity := range resources.Requests {
		if !quantity.IsZero() {
			requests["requests."+string(resourceName)] = quantity.String()
		}
	}
	for resourceName, quantity := range resources.Limits {
		if !quantity.IsZero() {
			requests["limits."+string(resourceName)] = quantity.String()
		}
	}
	if len(requests) == 0 {
		return nil
	}
	return requests
}

func updateContainersFromStatuses(pod corev1.Pod, podSummary *graphpb.PodSummary) {
	for _, status := range pod.Status.ContainerStatuses {
		for i, container := range podSummary.Containers {
			if container.Name == status.Name {
				updateContainerFromStatus(podSummary.Containers[i], status)
				break
			}
		}
	}
}

func updateContainerFromStatus(containerSummary *graphpb.PodContainerSummary, status corev1.ContainerStatus) {
	// The actual image and imageID may be different that in the container spec.
	containerSummary.Image.ImageId = status.ImageID
	if containerSummary.Image.Image != status.Image && status.Image != "" {
		containerSummary.Image.Image = status.Image
	}
	containerSummary.Ready = status.Ready
	if status.State.Waiting != nil {
		switch status.State.Waiting.Reason {
		case "ImagePullBackOff":
			containerSummary.IsImagePullBackOff = true
		case "CrashLoopBackOff":
			containerSummary.IsCrashLoopBackOff = true
		}
	}
	if status.LastTerminationState.Terminated != nil {
		if status.LastTerminationState.Terminated.Reason == "OOMKilled" {
			containerSummary.WasOomKilled = true
		}
	}
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
