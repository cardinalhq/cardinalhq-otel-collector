package extractmetricsprocessor

import "go.opentelemetry.io/collector/component"

const (
	// gaugeDoubleType is the gauge double metric type.
	gaugeDoubleType = "gauge_double"

	// gaugeIntType is the gauge int metric type.
	gaugeIntType = "gauge_int"

	// counterDoubleType is the counter float metric type.
	counterDoubleType = "counter_double"

	// counterIntType is the counter int metric type.
	counterIntType = "counter_int"
)

type Config struct {
	ConfigurationExtension *component.ID `mapstructure:"configuration_extension"`
}
