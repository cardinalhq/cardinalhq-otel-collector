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

package fbnauthextension

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/mr-tron/base58"
)

const (
	SignatureAlgoEd25519 = "ed25519"
	// SignatureAlgoXEd25519 verifies XEdDSA signatures made with an x25519
	// key: the token pubkey is the sender's x25519 (Montgomery) public key,
	// converted to Edwards (sign bit 0) and checked with stock Ed25519.
	// This is what freenet nodes send: the token pubkey IS the node's
	// transport identity.
	SignatureAlgoXEd25519 = "xed25519"

	defaultMaxClockSkew = 5 * time.Minute

	// audienceHashBytes is how much of the SHA-256 digest the audience field
	// carries. Truncation is fine: the field binds a token to a URL the
	// operator listed, it is not a collision-resistance boundary.
	audienceHashBytes = 16
)

type PrefixConfig struct {
	SignatureAlgo string `mapstructure:"signature_algo"`
	// Audiences lists the full URLs this collector legitimately answers at,
	// spelled the way senders dial them — if an ingress rewrites the path, the
	// external spelling belongs here, because that is what the sender hashed.
	// A token's audience field must hash to one of these. Empty means the
	// field is not checked, so upgrading does not break deployments before
	// operators have written the list. Only meaningful for prefixes whose
	// tokens carry an audience.
	Audiences []string `mapstructure:"audiences"`
}

type Config struct {
	// MaxClockSkew bounds how far a token's timestamp may be from now,
	// in either direction. Tokens outside the window are rejected.
	MaxClockSkew time.Duration `mapstructure:"max_clock_skew"`
	// Prefixes maps an accepted token prefix (e.g. "freenet") to its
	// expected signature algorithm. Tokens with any other prefix are denied.
	Prefixes map[string]PrefixConfig `mapstructure:"prefixes"`
}

var errNoPrefixes = errors.New("at least one prefix must be configured")

func (cfg *Config) Validate() error {
	if len(cfg.Prefixes) == 0 {
		return errNoPrefixes
	}
	for prefix, pc := range cfg.Prefixes {
		switch pc.SignatureAlgo {
		case SignatureAlgoEd25519, SignatureAlgoXEd25519:
		default:
			return fmt.Errorf("prefix %q: unsupported signature_algo %q (supported: %s, %s)",
				prefix, pc.SignatureAlgo, SignatureAlgoEd25519, SignatureAlgoXEd25519)
		}
		for _, a := range pc.Audiences {
			if _, err := AudienceHash(a); err != nil {
				return fmt.Errorf("prefix %q: audience %q: %w", prefix, a, err)
			}
		}
	}
	if cfg.MaxClockSkew <= 0 {
		return fmt.Errorf("max_clock_skew must be positive, got %s", cfg.MaxClockSkew)
	}
	return nil
}

// AudienceHash computes the value a sender puts in a token's audience field
// for the URL it dialed: base58 of the first 16 bytes of SHA-256 over the
// canonical URL. Both sides must canonicalize identically — this mirrors
// freenet's docs/otel-metrics.md.
//
// Canonical form is "{scheme}://{host}:{port}{path}", with:
//   - scheme and host lowercased
//   - port always explicit (80 for http, 443 for https when omitted)
//   - path verbatim: no trailing-slash collapse, no dot-segment removal
//   - userinfo stripped, query and fragment dropped
//
// Userinfo is stripped rather than signed because a URL carrying credentials
// would otherwise put the operator's password, hashed but grindable, into a
// wire-visible token.
func AudienceHash(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if port == "" {
		switch scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		default:
			return "", fmt.Errorf("scheme %q has no default port; give an explicit http(s) URL", u.Scheme)
		}
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", errors.New("no host in URL")
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]" // IPv6 literals keep their brackets
	}
	sum := sha256.Sum256([]byte(scheme + "://" + host + ":" + port + u.EscapedPath()))
	return base58.Encode(sum[:audienceHashBytes]), nil
}
