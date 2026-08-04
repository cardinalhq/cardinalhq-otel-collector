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
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mr-tron/base58"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensionauth"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/fbnauthextension/internal/metadata"
)

// Token format (all in one Authorization: Bearer value):
//
//	prefix/base58(pubkey)/unix-seconds/base58(signature)
//
// where the signature covers the literal bytes of
// "prefix/base58(pubkey)/unix-seconds" (the token up to, but not including,
// the final slash). Everything is verified locally; any parse or verification
// failure denies the request (fail closed).
//
// There is no per-token uniqueness field. A sender-chosen one bought nothing:
// it is not replay protection unless the verifier tracks it, and it is not
// precomputation resistance because the party who would precompute is the
// party who picks it. Consequently a token is stable for a given (key,
// second) and is replayable inside the skew window — see README. Adding
// replay protection means keying a bounded seen-set on (pubkey, timestamp),
// which is already unique per sender per second.
type fbnServerAuth struct {
	component.StartFunc
	component.ShutdownFunc

	config       *Config
	logger       *zap.Logger
	authAttempts metric.Int64Counter

	// now is replaceable for tests.
	now func() time.Time
}

var (
	_ extension.Extension  = (*fbnServerAuth)(nil)
	_ extensionauth.Server = (*fbnServerAuth)(nil)
)

var (
	errNoAuthHeader = errors.New("no bearer authorization header found")
	errDenied       = errors.New("authentication denied")
)

const tokenParts = 4 // prefix / pubkey / timestamp / signature

func newServerAuthExtension(cfg *Config, params extension.Settings) (*fbnServerAuth, error) {
	m, err := metadata.Meter(params.TelemetrySettings).Int64Counter("auth_attempts")
	if err != nil {
		return nil, err
	}
	return &fbnServerAuth{
		config:       cfg,
		logger:       params.Logger,
		authAttempts: m,
		now:          time.Now,
	}, nil
}

func (fbn *fbnServerAuth) Authenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	token := getBearerToken(headers)
	if token == "" {
		fbn.count(ctx, "no_header")
		return ctx, errNoAuthHeader
	}

	ad, reason := fbn.verifyToken(token)
	fbn.count(ctx, reason)
	if ad == nil {
		// The reason is returned to the sender: these are misconfiguration
		// signals (clock skew, wrong prefix, wrong algorithm) that operators
		// otherwise cannot diagnose from the client side. The reasons are
		// coarse labels only — they never reveal key material or which
		// specific byte of a signature failed.
		fbn.logger.Debug("fbn auth denied", zap.String("reason", reason))
		return ctx, fmt.Errorf("%w: %s", errDenied, reason)
	}

	cl := client.FromContext(ctx)
	cl.Auth = ad
	return client.NewContext(ctx, cl), nil
}

// verifyToken returns the auth data and "ok", or nil and a denial reason.
func (fbn *fbnServerAuth) verifyToken(token string) (*authData, string) {
	parts := strings.Split(token, "/")
	if len(parts) != tokenParts {
		return nil, "malformed"
	}
	prefix, pubkeyB58, tsStr, sigB58 := parts[0], parts[1], parts[2], parts[3]

	pc, ok := fbn.config.Prefixes[prefix]
	if !ok {
		return nil, "unknown_prefix"
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return nil, "bad_timestamp"
	}
	skew := fbn.now().Sub(time.Unix(ts, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > fbn.config.MaxClockSkew {
		return nil, "stale_timestamp"
	}

	pubkey, err := base58.Decode(pubkeyB58)
	if err != nil {
		return nil, "bad_pubkey"
	}
	sig, err := base58.Decode(sigB58)
	if err != nil {
		return nil, "bad_signature"
	}

	signed := token[:strings.LastIndexByte(token, '/')]
	switch pc.SignatureAlgo {
	case SignatureAlgoEd25519:
		if len(pubkey) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
			return nil, "bad_key_size"
		}
		if err := checkEdwardsKey(pubkey); err != nil {
			return nil, keyErrReason(err)
		}
		if !ed25519.Verify(ed25519.PublicKey(pubkey), []byte(signed), sig) {
			return nil, "bad_signature"
		}
	case SignatureAlgoXEd25519:
		if len(pubkey) != 32 || len(sig) != ed25519.SignatureSize {
			return nil, "bad_key_size"
		}
		// Use the key conversion directly rather than xed25519Verify so a bad
		// or low-order key is reported as such instead of "bad_signature".
		vk, err := xed25519VerifyKey(pubkey)
		if err != nil {
			return nil, keyErrReason(err)
		}
		if !ed25519.Verify(vk, []byte(signed), sig) {
			return nil, "bad_signature"
		}
	default:
		// Unreachable given Config.Validate, but fail closed regardless.
		return nil, "bad_algo"
	}

	return &authData{
		prefix:        prefix,
		publicKey:     pubkeyB58,
		timestamp:     ts,
		signature:     sigB58,
		signatureAlgo: pc.SignatureAlgo,
	}, "ok"
}

func (fbn *fbnServerAuth) count(ctx context.Context, result string) {
	fbn.authAttempts.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

func getBearerToken(h map[string][]string) string {
	for k, v := range h {
		if !strings.EqualFold(k, "authorization") || len(v) == 0 {
			continue
		}
		scheme, token, found := strings.Cut(v[0], " ")
		if found && strings.EqualFold(scheme, "bearer") {
			return strings.TrimSpace(token)
		}
	}
	return ""
}

// Attribute names exposed via client.AuthData. The fbn_validator processor
// consumes these; keep the two in sync.
const (
	AttrPrefix        = "prefix"
	AttrPublicKey     = "public_key"
	AttrTimestamp     = "timestamp"
	AttrSignature     = "signature"
	AttrSignatureAlgo = "signature_algo"
)

type authData struct {
	prefix        string
	publicKey     string // base58, as presented in the token
	timestamp     int64
	signature     string // base58, as presented in the token
	signatureAlgo string
}

var _ client.AuthData = (*authData)(nil)

func (a *authData) GetAttribute(name string) any {
	switch name {
	case AttrPrefix:
		return a.prefix
	case AttrPublicKey:
		return a.publicKey
	case AttrTimestamp:
		return a.timestamp
	case AttrSignature:
		return a.signature
	case AttrSignatureAlgo:
		return a.signatureAlgo
	default:
		return nil
	}
}

func (a *authData) GetAttributeNames() []string {
	return []string{AttrPrefix, AttrPublicKey, AttrTimestamp, AttrSignature, AttrSignatureAlgo}
}

// BuildToken assembles and signs a token. Exported for use by clients and
// tests; the collector itself only verifies.
func BuildToken(prefix string, priv ed25519.PrivateKey, ts time.Time) string {
	pub := priv.Public().(ed25519.PublicKey)
	signed := fmt.Sprintf("%s/%s/%d", prefix, base58.Encode(pub), ts.Unix())
	sig := ed25519.Sign(priv, []byte(signed))
	return signed + "/" + base58.Encode(sig)
}
