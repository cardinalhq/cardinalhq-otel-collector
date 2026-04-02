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

package chqauthextension

import (
	"context"
	"net/http"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"
	creds "google.golang.org/grpc/credentials"
)

type chqClientAuth struct {
	component.StartFunc
	component.ShutdownFunc

	config   *ClientAuth
	insecure bool
}

var (
	_ extension.Extension      = (*chqClientAuth)(nil)
	_ extensionauth.HTTPClient = (*chqClientAuth)(nil)
	_ extensionauth.GRPCClient = (*chqClientAuth)(nil)
)

func newClientAuthExtension(cfg *Config, _ extension.Settings) (*chqClientAuth, error) {
	chq := chqClientAuth{
		config:   cfg.ClientAuth,
		insecure: cfg.ClientAuth.Insecure,
	}
	return &chq, nil
}

type chqRoundTripper struct {
	base   http.RoundTripper
	apiKey string
}

func (chq *chqClientAuth) RoundTripper(base http.RoundTripper) (http.RoundTripper, error) {
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

func (chq *chqClientAuth) PerRPCCredentials() (creds.PerRPCCredentials, error) {
	metadata := map[string]string{
		apiKeyHeader: chq.config.APIKey,
	}
	return &chqPerRPCAuth{
		metadata: metadata,
		insecure: chq.config.Insecure,
	}, nil
}

func (c *chqPerRPCAuth) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return c.metadata, nil
}

func (c *chqPerRPCAuth) RequireTransportSecurity() bool {
	return !c.insecure
}
