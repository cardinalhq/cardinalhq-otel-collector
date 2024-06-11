// Copyright 2024 CardinalHQ, Inc
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

package chqauthextension

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
)

type ServerAuth struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	CacheTTLValid           time.Duration `mapstructure:"cache_ttl_valid"`
	CacheTTLInvalid         time.Duration `mapstructure:"cache_ttl_invalid"`
}

type Config struct {
	ServerAuth ServerAuth `mapstructure:"server_auth"`
}

func (cfg *Config) Validate() error {
	if cfg.ServerAuth.CacheTTLValid <= 0 {
		cfg.ServerAuth.CacheTTLValid = 10 * time.Minute
	}
	if cfg.ServerAuth.CacheTTLInvalid <= 0 {
		cfg.ServerAuth.CacheTTLInvalid = 1 * time.Minute
	}
	if cfg.ServerAuth.Endpoint == "" {
		return errors.New("server_auth.endpoint must be set")
	}
	return nil
}
