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
	"bytes"
	"errors"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/baseobj"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
	"github.com/cardinalhq/oteltools/pkg/graph/graphpb"
)

func ConvertConfigMap(config *converterconfig.Config, us unstructured.Unstructured) (baseobj.K8SObject, error) {
	if us.GetKind() != "ConfigMap" || us.GetAPIVersion() != "v1" {
		return nil, errors.New("unstructured object is not a ConfigMap")
	}

	if isFilteredConfigMapName(config, us.GetName()) {
		return nil, nil
	}

	var k8sobj corev1.ConfigMap
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(us.Object, &k8sobj)
	if err != nil {
		return nil, err
	}

	cms := &graphpb.ConfigMapSummary{
		BaseObject: baseobj.Make(config, &us, us.GetAPIVersion(), us.GetKind()),
		Hashes:     calculateConfigMapDataHashes(config.HashItems, k8sobj),
	}

	return cms, nil
}

func calculateConfigMapDataHashes(hashitems []string, cm corev1.ConfigMap) map[string]string {
	dataHashes := make(map[string]string)
	buf := bytes.Buffer{}
	for _, item := range hashitems {
		buf.WriteString(item)
	}
	buf.WriteString(cm.APIVersion + cm.Kind + cm.Name + cm.Namespace + string(cm.UID))
	header := buf.Bytes()
	for k, v := range cm.Data {
		dataHashes[k] = calculateHashValue(header, k, []byte(v))
	}
	for k, v := range cm.BinaryData {
		dataHashes[k] = calculateHashValue(header, k, v)
	}
	if len(dataHashes) == 0 {
		return nil
	}
	return dataHashes
}
