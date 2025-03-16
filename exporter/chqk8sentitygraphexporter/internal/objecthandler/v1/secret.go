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
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"
)

type SecretSummary struct {
	baseobj.BaseObject `json:",inline"`
	DataHashes         map[string]string `json:"data_hashes"`
}

func ConvertSecret(config *converterconfig.Config, us unstructured.Unstructured) (baseobj.K8SObject, error) {
	if us.GetKind() != "Secret" || us.GetAPIVersion() != "v1" {
		return nil, errors.New("unstructured object is not a Secret")
	}

	var secret corev1.Secret
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(us.Object, &secret)
	if err != nil {
		return nil, err
	}

	ss := &SecretSummary{
		BaseObject: baseobj.BaseFromUnstructured(config, us),
		DataHashes: calculateSecretDataHashes(secret),
	}

	return ss, nil
}

func calculateSecretDataHashes(secret corev1.Secret) map[string]string {
	dataHashes := make(map[string]string)
	header := []byte(secret.APIVersion + secret.Kind + secret.Name + secret.Namespace + string(secret.UID))
	for k, v := range secret.Data {
		dataHashes[k] = calculateHashValue(header, k, v)
	}
	if len(dataHashes) == 0 {
		return nil
	}
	return dataHashes
}
