package v1

import (
	"slices"
	"strings"

	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
	mapset "github.com/deckarep/golang-set/v2"
	corev1 "k8s.io/api/core/v1"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

func GetPodSpec(conf *converterconfig.Config, podSpec corev1.PodSpec) *graphpb.PodSpec {
	return &graphpb.PodSpec{
		ServiceAccountName: podSpec.ServiceAccountName,
		Containers:         GetContainerSpecs(conf, podSpec),
	}
}

func GetContainerSpecs(conf *converterconfig.Config, podSpec corev1.PodSpec) []*graphpb.PodContainerSpec {
	specs := make([]*graphpb.PodContainerSpec, 0, len(podSpec.Containers))
	for _, container := range podSpec.Containers {
		specs = append(specs, &graphpb.PodContainerSpec{
			Name:           container.Name,
			Image:          container.Image,
			Resources:      convertPodResources(container.Resources),
			ConfigMapNames: containerConfigMapNames(conf, podSpec, container),
			SecretNames:    containerSecretNames(conf, podSpec, container),
		})
	}
	slices.SortFunc(specs, func(a, b *graphpb.PodContainerSpec) int {
		return strings.Compare(a.Name, b.Name)
	})
	return specs
}

// containerConfigMapNames returns the list of ConfigMap names used by the container,
// either in an env var or in a volume mount.
func containerConfigMapNames(conf *converterconfig.Config, podSpec corev1.PodSpec, container corev1.Container) []string {
	seen := mapset.NewSet[string]()
	containerConfigMapNamesFromEnv(seen, container)
	containerConfigMapNamesFromVolumes(conf, seen, podSpec.Volumes, container)
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
func containerSecretNames(conf *converterconfig.Config, podSpec corev1.PodSpec, container corev1.Container) []string {
	seen := mapset.NewSet[string]()
	containerSecretNamesFromEnv(seen, container)
	containerSecretNamesFromVolumes(conf, seen, podSpec.Volumes, container)
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
