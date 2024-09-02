package githubeventsreceiver

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
)

type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"`
	Path                    string `mapstructure:"path"`        // path where the receiver instance will accept events. Default is /events
	Secret                  string `mapstructure:"secret"`      // secret to verify that a webhook delivery is from GitHub.
	HealthPath              string `mapstructure:"health_path"` // path for health check api. Default is /health_check
}

var _ component.Config = (*Config)(nil)

func (cfg *Config) Validate() error {
	return nil

}
