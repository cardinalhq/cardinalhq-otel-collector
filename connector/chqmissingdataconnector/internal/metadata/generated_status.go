package metadata

import (
	"go.opentelemetry.io/collector/component"
)

var (
	Type      = component.MustNewType("chqmissingdata")
	ScopeName = "github.com/cardinalhq/cardinalhq-otel-collector/connector/chqmissingdataconnector"
)

const (
	MetricsToMetricsStability = component.StabilityLevelAlpha
)
