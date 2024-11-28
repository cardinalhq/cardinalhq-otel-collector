package newrelicreceiver

import (
    "fmt"
    "time"

    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/config/confighttp"
)

type Config struct {
    confighttp.ServerConfig `mapstructure:",squash"`
    APIKey                  string        `mapstructure:"api_key"`
    ReadTimeout             time.Duration `mapstructure:"read_timeout"`
}

var _ component.Config = (*Config)(nil)

func (cfg *Config) Validate() error {
    if cfg.APIKey == "" {
        return fmt.Errorf("api_key is required")
    }
    return nil
}
