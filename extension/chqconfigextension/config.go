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
	"net/url"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
)

type Config struct {
	Source        ConfigSourceConfig `mapstructure:"source"`
	CheckInterval time.Duration      `mapstructure:"check_interval"`
}

type ConfigSourceConfig struct {
	confighttp.ClientConfig `mapstructure:",squash"`

	scheme string
}

var (
	sourceSchemes = []string{"http", "https"}

	errMissingEndpoint       = errors.New("auth.endpoint must be set")
	errInvalidEndpoint       = errors.New("auth.endpoint must be a valid URL")
	errInvalidScheme         = errors.New("auth.endpoint: supported schemes: " + strings.Join(sourceSchemes, ", "))
	errCheckIntervalTooShort = errors.New("check_interval must be set to a positive value greater than 10s")
)

func (cfg *Config) Validate() error {
	if err := cfg.Source.Validate(); err != nil {
		return err
	}
	if cfg.CheckInterval < 10*time.Second {
		return errCheckIntervalTooShort
	}
	return nil
}

func (cfg *ConfigSourceConfig) Validate() error {
	if cfg.Endpoint == "" {
		return errMissingEndpoint
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return errInvalidEndpoint
	}
	if !slices.Contains(sourceSchemes, u.Scheme) {
		return errInvalidScheme
	}
	cfg.scheme = u.Scheme
	return nil
}
