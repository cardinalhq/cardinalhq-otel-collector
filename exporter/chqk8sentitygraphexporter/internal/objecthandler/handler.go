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

package objecthandler

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	convertv1 "github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/v1"
)

type ConverterFunc func(us unstructured.Unstructured) (any, error)

type objectSelector struct {
	APIVersion string
	Kind       string
}

type Converters map[objectSelector]ConverterFunc

type ObjectHandler interface {
	Feed(rlattr pcommon.Map, lattr pcommon.Map, bodyValue pcommon.Value) (*PackagedObject, error)
}

type converterImpl struct {
	converters Converters
}

var _ ObjectHandler = (*converterImpl)(nil)

func NewObjectHandler() ObjectHandler {
	ret := &converterImpl{
		converters: Converters{},
	}
	ret.installConverters()
	return ret
}

func (h *converterImpl) installConverters() {
	h.converters[objectSelector{"v1", "Pod"}] = convertv1.ConvertPod
	h.converters[objectSelector{"v1", "ConfigMap"}] = convertv1.ConvertConfigMap
	h.converters[objectSelector{"v1", "Secret"}] = convertv1.ConvertSecret
}

// Feed takes a set of attributes and an object and converts the object to a PackagedObject.
// If the object is not recognized, no error will be returned but the PackagedObject will be nil.
// Errors will only be returned if the object was of a type we know about, but for
// some reason the content is invalid.
func (h *converterImpl) Feed(rlattr pcommon.Map, lattr pcommon.Map, bodyValue pcommon.Value) (*PackagedObject, error) {
	// The k8sobjectsreceiver will always populate the log body as a map.
	if bodyValue.Type() != pcommon.ValueTypeMap {
		return nil, nil
	}

	// Pull out the APIVersion and Kind from the object and look for our converter.
	us := unstructured.Unstructured{Object: bodyValue.Map().AsRaw()}
	APIVersion := us.GetAPIVersion()
	Kind := us.GetKind()
	selector := objectSelector{APIVersion: APIVersion, Kind: Kind}
	converter, ok := h.converters[selector]
	if !ok {
		return nil, nil
	}

	// Convert the object and package it up.
	converted, err := converter(us)
	if err != nil {
		return nil, err
	}
	if converted == nil {
		return nil, nil
	}
	result := &PackagedObject{
		ResouceAttributes: rlattr.AsRaw(),
		RecordAttributes:  lattr.AsRaw(),
		Object:            converted,
	}

	return result, nil
}
