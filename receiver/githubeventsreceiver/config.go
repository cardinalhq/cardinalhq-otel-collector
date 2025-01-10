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
