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

package chqconfigextension

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
)

type Config struct {
	confighttp.ClientConfig `mapstructure:",squash"`
	Endpoint                string        `mapstructure:"endpoint"`
	APIKey                  string        `mapstructure:"api_key"`
	CheckInterval           time.Duration `mapstructure:"check_interval"`
}

func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return errors.New("auth.endpoint must be set")
	}
	if cfg.APIKey == "" {
		return errors.New("auth.api_key must be set")
	}
	if cfg.CheckInterval <= 0 {
		return errors.New("auth.check_interval must be set to a positive value")
	}
	return nil
}
