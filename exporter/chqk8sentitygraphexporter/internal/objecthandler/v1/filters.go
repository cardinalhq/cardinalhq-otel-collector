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

import "github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter/internal/objecthandler/converterconfig"

func isFilteredSecretName(conf *converterconfig.Config, name string) bool {
	for _, matcher := range conf.IgnoredSecretNames {
		if matcher.Match(name) {
			return true
		}
	}
	return false
}

func isFilteredConfigMapName(conf *converterconfig.Config, name string) bool {
	for _, matcher := range conf.IgnoredConfigMapNames {
		if matcher.Match(name) {
			return true
		}
	}
	return false
}
