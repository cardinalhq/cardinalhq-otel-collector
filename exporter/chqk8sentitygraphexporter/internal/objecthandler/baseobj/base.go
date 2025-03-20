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

package baseobj

import (
	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

type K8SObject interface {
	GetBaseObject() *graphpb.BaseObject
}

func computeIdentifier(conf *converterconfig.Config, b *graphpb.BaseObject) string {
	if b.Namespace == "" {
		return "kubernetes/" + conf.IDPrefix + "/" + b.ApiVersion + "/" + b.Kind + "/" + b.Name
	}
	return "kubernetes/" + conf.IDPrefix + "/" + b.ApiVersion + "/" + b.Kind + "/" + b.Namespace + "/" + b.Name
}

// OwnerRef is a struct that contains the owner reference fields for a k8s object.
// This is used to determine the parent-child relationship between k8s objects.
// The Controller field is used to determine if the owner is a controller of the object.
type OwnerRef struct {
	APIVersion string `json:"api_version" yaml:"api_version"`
	Kind       string `json:"kind" yaml:"kind"`
	Name       string `json:"name" yaml:"name"`
	Controller bool   `json:"controller,omitempty" yaml:"controller,omitempty"`
}

type MetadataGetter interface {
	GetUID() types.UID
	GetResourceVersion() string
	GetNamespace() string
	GetName() string
	GetLabels() map[string]string
	GetAnnotations() map[string]string
	GetOwnerReferences() []metav1.OwnerReference
}

// Make is a function that converts an unstructured object to a BaseObject.
func Make(config *converterconfig.Config, us MetadataGetter, apiVersion string, kind string) *graphpb.BaseObject {
	pbb := &graphpb.BaseObject{
		ApiVersion:      apiVersion,
		Kind:            kind,
		Uid:             string(us.GetUID()),
		ResourceVersion: us.GetResourceVersion(),
		Namespace:       us.GetNamespace(),
		Name:            us.GetName(),
		Labels:          us.GetLabels(),
		Annotations:     filteredAnnotations(config, us.GetAnnotations()),
		OwnerRef:        ownerrefsFromUnstructured(us),
	}
	pbb.Id = computeIdentifier(config, pbb)

	return pbb
}

func ownerrefsFromUnstructured(us MetadataGetter) []*graphpb.OwnerRef {
	var ownerRefs []*graphpb.OwnerRef
	for _, ownerRef := range us.GetOwnerReferences() {
		o := &graphpb.OwnerRef{
			ApiVersion: ownerRef.APIVersion,
			Kind:       ownerRef.Kind,
			Name:       ownerRef.Name,
			Controller: ptr.Deref(ownerRef.Controller, false),
		}
		ownerRefs = append(ownerRefs, o)
	}
	return ownerRefs
}

func filteredAnnotations(config *converterconfig.Config, annotations map[string]string) map[string]string {
	filtered := make(map[string]string)
	for k, v := range annotations {
		if !isFilteredAnnotation(config, k) {
			filtered[k] = v
		}
	}
	return filtered
}

func isFilteredAnnotation(config *converterconfig.Config, annotation string) bool {
	for _, matcher := range config.IgnoredAnnotations {
		if matcher.Match(annotation) {
			return true
		}
	}
	return false
}
