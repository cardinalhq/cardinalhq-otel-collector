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
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	convertappsv1 "github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/apps/v1"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/baseobj"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
	convertv1 "github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/v1"
	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
)

type ConverterFunc func(config *converterconfig.Config, us unstructured.Unstructured) (baseobj.K8SObject, error)

type objectSelector struct {
	APIVersion string
	Kind       string
}

type Converters map[objectSelector]ConverterFunc

type ObjectHandler interface {
	Feed(rlattr pcommon.Map, lattr pcommon.Map, bodyValue pcommon.Value) (*graphpb.PackagedObject, error)
}

type converterImpl struct {
	config     *converterconfig.Config
	converters Converters
}

var _ ObjectHandler = (*converterImpl)(nil)

func NewObjectHandler(config *converterconfig.Config) ObjectHandler {
	ret := &converterImpl{
		config:     config,
		converters: Converters{},
	}
	ret.installConverters()
	return ret
}

func (h *converterImpl) installConverters() {
	h.converters[objectSelector{"apps/v1", "DaemonSet"}] = convertappsv1.ConvertDaemonSet
	h.converters[objectSelector{"apps/v1", "Deployment"}] = convertappsv1.ConvertDeployment
	h.converters[objectSelector{"apps/v1", "ReplicaSet"}] = convertappsv1.ConvertReplicaSet
	h.converters[objectSelector{"apps/v1", "StatefulSet"}] = convertappsv1.ConvertStatefulSet
	h.converters[objectSelector{"v1", "ConfigMap"}] = convertv1.ConvertConfigMap
	h.converters[objectSelector{"v1", "Pod"}] = convertv1.ConvertPod
	h.converters[objectSelector{"v1", "Secret"}] = convertv1.ConvertSecret
}

// Feed takes a set of attributes and an object and converts the object to a PackagedObject.
// If the object is not recognized, no error will be returned but the PackagedObject will be nil.
// Errors will only be returned if the object was of a type we know about, but for
// some reason the content is invalid.
func (h *converterImpl) Feed(rlattr pcommon.Map, lattr pcommon.Map, bodyValue pcommon.Value) (*graphpb.PackagedObject, error) {
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
	converted, err := converter(h.config, us)
	if err != nil {
		return nil, err
	}
	if converted == nil {
		return nil, nil
	}

	rla := map[string]string{}
	rlattr.Range(func(k string, v pcommon.Value) bool {
		rla[k] = v.AsString()
		return true
	})

	la := map[string]string{}
	lattr.Range(func(k string, v pcommon.Value) bool {
		la[k] = v.AsString()
		return true
	})

	po := graphpb.NewPackagedObject(converted, rla, la)
	if po == nil {
		return nil, fmt.Errorf("unknown summary type: %T %s/%s", converted, APIVersion, Kind)
	}

	return po, nil
}
