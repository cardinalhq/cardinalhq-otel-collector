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
	"context"
	"net/http"
	"strings"

	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/auth"
	creds "google.golang.org/grpc/credentials"
)

type chqClientAuth struct {
	config   *ClientAuth
	insecure bool
	env      map[string]string
}

func newClientAuthExtension(cfg *Config, _ extension.Settings) auth.Client {
	chq := chqClientAuth{
		config:   cfg.ClientAuth,
		insecure: cfg.ClientAuth.Insecure,
		env:      cfg.ClientAuth.Environment,
	}
	return auth.NewClient(
		auth.WithClientRoundTripper(chq.roundTripper),
		auth.WithClientPerRPCCredentials(chq.perRPCCredentials),
	)
}

type chqRoundTripper struct {
	base        http.RoundTripper
	apiKey      string
	collectorID string
	env         map[string]string
}

func (chq *chqClientAuth) roundTripper(base http.RoundTripper) (http.RoundTripper, error) {
	if chq.config.APIKey == "" {
		return base, nil
	}
	return &chqRoundTripper{
		base:        base,
		apiKey:      chq.config.APIKey,
		collectorID: chq.config.CollectorID,
		env:         chq.env,
	}, nil
}

func (c *chqRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	newReq := req.Clone(req.Context())
	newReq.Header.Set(apiKeyHeader, c.apiKey)
	if c.collectorID != "" {
		newReq.Header.Set(collectorIDHeader, c.collectorID)
	}
	newReq.Header.Set(envKeyHeader, encodeEnv(c.env))
	return c.base.RoundTrip(newReq)
}

type chqPerRPCAuth struct {
	metadata map[string]string
	insecure bool
}

func (chq *chqClientAuth) perRPCCredentials() (creds.PerRPCCredentials, error) {
	metadata := map[string]string{
		apiKeyHeader: chq.config.APIKey,
	}
	if chq.config.CollectorID != "" {
		metadata[collectorIDHeader] = chq.config.CollectorID
	}
	if len(chq.env) > 0 {
		metadata[envKeyHeader] = encodeEnv(chq.env)
	}
	return &chqPerRPCAuth{
		metadata: metadata,
		insecure: chq.config.Insecure,
	}, nil
}

func encodeEnv(env map[string]string) string {
	var items []string
	for k, v := range env {
		items = append(items, k+"="+v)
	}
	return strings.Join(items, ";")
}

func (c *chqPerRPCAuth) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return c.metadata, nil
}

func (c *chqPerRPCAuth) RequireTransportSecurity() bool {
	return !c.insecure
}
