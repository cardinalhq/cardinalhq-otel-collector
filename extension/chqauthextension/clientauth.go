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

	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/auth"
	creds "google.golang.org/grpc/credentials"
)

type chqClientAuth struct {
	config   *ClientAuth
	insecure bool
}

func newClientAuthExtension(cfg *Config, _ extension.Settings) auth.Client {
	chq := chqClientAuth{
		config:   cfg.ClientAuth,
		insecure: cfg.ClientAuth.Insecure,
	}
	return auth.NewClient(
		auth.WithClientRoundTripper(chq.roundTripper),
		auth.WithClientPerRPCCredentials(chq.perRPCCredentials),
	)
}

type chqRoundTripper struct {
	base   http.RoundTripper
	apiKey string
}

func (chq *chqClientAuth) roundTripper(base http.RoundTripper) (http.RoundTripper, error) {
	if chq.config.APIKey == "" {
		return base, nil
	}
	return &chqRoundTripper{
		base:   base,
		apiKey: chq.config.APIKey,
	}, nil
}

func (c *chqRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	newReq := req.Clone(req.Context())
	newReq.Header.Set(apiKeyHeader, c.apiKey)
	return c.base.RoundTrip(newReq)
}

type chqPerRPCAuth struct {
	metadata map[string]string
	insecure bool
}

func (chq *chqClientAuth) perRPCCredentials() (creds.PerRPCCredentials, error) {
	return &chqPerRPCAuth{
		metadata: map[string]string{
			apiKeyHeader: chq.config.APIKey,
		},
		insecure: chq.config.Insecure,
	}, nil
}

func (c *chqPerRPCAuth) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return c.metadata, nil
}

func (c *chqPerRPCAuth) RequireTransportSecurity() bool {
	return !c.insecure
}
