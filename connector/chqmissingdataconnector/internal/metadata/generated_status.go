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

// Package metadata contains the CardinalHQ Missing Data Connector metadata.
package metadata

import (
	"go.opentelemetry.io/collector/component"
)

var (
	// Type is the component type for the CardinalHQ Missing Data Connector.
	Type = component.MustNewType("chqmissingdata")

	// ScopeName is the scope name for the CardinalHQ Missing Data Connector.
	ScopeName = "github.com/cardinalhq/cardinalhq-otel-collector/connector/chqmissingdataconnector"
)

const (
	// MetricsToMetricsStability is the stability level for the CardinalHQ Missing Data Connector.
	MetricsToMetricsStability = component.StabilityLevelAlpha
)
