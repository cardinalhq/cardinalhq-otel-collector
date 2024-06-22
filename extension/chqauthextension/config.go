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

type ClientAuth struct {
	APIKey   string `mapstructure:"api_key"`
	Insecure bool   `mapstructure:"insecure"`
}

type Config struct {
	ServerAuth *ServerAuth `mapstructure:"server_auth"`
	ClientAuth *ClientAuth `mapstructure:"client_auth"`
}

const (
	defaultCacheValidTTL   = 10 * time.Minute
	defaultCacheInvalidTTL = 1 * time.Minute
)

var (
	errServerAuthEndpoint  = errors.New("server_auth.endpoint must be set")
	errNoAuthConfig        = errors.New("one of client_auth.api_key or server_auth.endpoint must be set")
	errDuplicateAuthConfig = errors.New("only one of client_auth.api_key or server_auth.endpoint can be set")
	errNoClientAPIKey      = errors.New("client_auth.api_key must be set")
)

func (cfg *Config) Validate() error {
	cauth := cfg.ClientAuth != nil
	sauth := cfg.ServerAuth != nil

	if cauth && sauth {
		return errDuplicateAuthConfig
	}

	if !cauth && !sauth {
		return errNoAuthConfig
	}

	if sauth {
		return cfg.ServerAuth.Validate()
	}
	return cfg.ClientAuth.Validate()
}

func (cfg *ClientAuth) Validate() error {
	if cfg.APIKey == "" {
		return errNoClientAPIKey
	}

	return nil
}

func (cfg *ServerAuth) Validate() error {
	if cfg.Endpoint == "" {
		return errServerAuthEndpoint
	}
	if cfg.CacheTTLValid <= time.Minute {
		cfg.CacheTTLValid = defaultCacheValidTTL
	}
	if cfg.CacheTTLInvalid <= time.Minute {
		cfg.CacheTTLInvalid = defaultCacheInvalidTTL
	}
	return nil
}
