package chqmissingdataconnector

import (
	"fmt"
)

type Config struct {
	Metrics map[string]MetricInfo `mapstructure:"metrics"`
}

type MetricInfo struct {
	Description string            `mapstructure:"description"`
	Conditions  []string          `mapstructure:"conditions"`
	Attributes  []AttributeConfig `mapstructure:"attributes"`
}

type AttributeConfig struct {
	Key   string `mapstructure:"key"`
	Value string `mapstructure:"value"`
}

func (c *Config) Validate() error {
	return nil
}

func (i *MetricInfo) validateAttributes() error {
	for _, attr := range i.Attributes {
		if attr.Key == "" {
			return fmt.Errorf("attribute key missing")
		}
		if attr.Value == "" {
			return fmt.Errorf("attribute value missing")
		}
	}
	return nil
}
