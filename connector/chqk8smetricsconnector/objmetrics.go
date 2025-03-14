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

package chqk8smetricsconnector

import (
	"github.com/cardinalhq/oteltools/signalbuilder"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const (
	scopeBase      = "github.com/cardinalhq/cardinalhq-otel-collector/connector/chqk8smetricsconnector"
	scopePod       = scopeBase + "/pod"
	scopeConfigMap = scopeBase + "/configmap"
	scopeSecret    = scopeBase + "/secret"
)

type converterFunc func(builder *signalbuilder.MetricsBuilder, rlattr pcommon.Map, lattr pcommon.Map, body pcommon.Map) error

type objectSelector struct {
	APIVersion string
	Kind       string
}

func buildConverters() map[objectSelector]converterFunc {
	ret := map[objectSelector]converterFunc{}

	ret[objectSelector{"v1", "pod"}] = convertPod

	return ret
}

func (c *md) processObject(builder *signalbuilder.MetricsBuilder, rlattr pcommon.Map, lattr pcommon.Map, bodyValue pcommon.Value) {
	if bodyValue.Type() != pcommon.ValueTypeMap {
		return
	}
	body := bodyValue.Map()
	APIVersion, ok := body.Get("apiVersion")
	if !ok {
		return
	}
	Kind, ok := body.Get("kind")
	if !ok {
		return
	}

	selector := objectSelector{APIVersion: APIVersion.AsString(), Kind: Kind.AsString()}

	if converter, ok := c.converters[selector]; ok {
		if err := converter(builder, rlattr, lattr, body); err != nil {
			return
		}
	}
}

func convertPod(builder *signalbuilder.MetricsBuilder, rlattr pcommon.Map, lattr pcommon.Map, body pcommon.Map) error {
	// TODO: Implement this
	return nil
}
