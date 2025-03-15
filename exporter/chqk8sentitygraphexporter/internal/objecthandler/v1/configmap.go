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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/baseobj"
	"github.com/cespare/xxhash/v2"
)

type ConfigMapSummary struct {
	baseobj.BaseObject `json:",inline"`
	DataHashes         map[string]uint64 `json:"data_hashes"`
}

func ConvertConfigMap(us unstructured.Unstructured) (*ConfigMapSummary, error) {
	if us.GetKind() != "ConfigMap" || us.GetAPIVersion() != "v1" {
		return nil, errors.New("unstructured object is not a ConfigMap")
	}
	var cm corev1.ConfigMap
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(us.Object, &cm)
	if err != nil {
		return nil, err
	}

	cms := &ConfigMapSummary{
		BaseObject: baseobj.BaseFromUnstructured(us),
		DataHashes: calculateConfigMapDataHashes(cm),
	}

	return cms, nil
}

func calculateConfigMapDataHashes(cm corev1.ConfigMap) map[string]uint64 {
	dataHashes := make(map[string]uint64)
	for k, v := range cm.Data {
		dataHashes[k] = xxhash.Sum64String(v)
	}
	if len(dataHashes) == 0 {
		return nil
	}
	return dataHashes
}
