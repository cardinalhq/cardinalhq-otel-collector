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
//	prefix/base58(pubkey)/audience/unix-seconds/base58(signature)
//
// where the signature covers the literal bytes of everything up to, but not
// including, the final slash. Everything is verified locally; any parse or
// verification failure denies the request (fail closed).
//
// The audience field binds a token to the URL the sender dialed (see
// AudienceHash), so a token minted for collector A does not verify at
// collector B. Without it the legitimate recipient can replay the token
// upstream and impersonate the sender — a different exposure from the
// in-window replay the README documents, which TLS covers.
//
// The pre-audience 4-field form is still accepted during the transition, since
// senders upgrade on their own schedule; the auth_attempts token_fields
// attribute is how you watch the 4-field population drain. A 4-field token
// carries no audience binding, but it cannot be forged from a 5-field one:
// the signature covers the audience field, so stripping it breaks the
// signature.
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

	// audiences maps prefix -> accepted audience hash -> the configured URL it
	// came from, precomputed at startup. A prefix absent here has no audience
	// list configured, which means "do not check".
	audiences map[string]map[string]string

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

const (
	legacyTokenParts = 4 // prefix / pubkey / timestamp / signature
	tokenParts       = 5 // prefix / pubkey / audience / timestamp / signature

	reasonWrongAudience = "wrong_audience"
)

func newServerAuthExtension(cfg *Config, params extension.Settings) (*fbnServerAuth, error) {
	m, err := metadata.Meter(params.TelemetrySettings).Int64Counter("auth_attempts")
	if err != nil {
		return nil, err
	}

	audiences := map[string]map[string]string{}
	for prefix, pc := range cfg.Prefixes {
		if len(pc.Audiences) == 0 {
			continue
		}
		set := make(map[string]string, len(pc.Audiences))
		for _, raw := range pc.Audiences {
			h, err := AudienceHash(raw)
			if err != nil {
				return nil, fmt.Errorf("prefix %q: audience %q: %w", prefix, raw, err)
			}
			set[h] = raw
		}
		audiences[prefix] = set
		// Logged with its source URLs so the opaque hash in a wrong_audience
		// denial can be paired with what this collector expects.
		params.Logger.Info("fbn auth audience bindings",
			zap.String("prefix", prefix), zap.Any("hashes", set))
	}

	return &fbnServerAuth{
		config:       cfg,
		logger:       params.Logger,
		authAttempts: m,
		audiences:    audiences,
		now:          time.Now,
	}, nil
}

func (fbn *fbnServerAuth) Authenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	token := getBearerToken(headers)
	if token == "" {
		fbn.count(ctx, "no_header")
		return ctx, errNoAuthHeader
	}

	ad, reason, parts := fbn.verifyToken(token)
	fbn.count(ctx, reason, attribute.Int("token_fields", len(parts)))
	if ad == nil {
		// The reason is returned to the sender: these are misconfiguration
		// signals (clock skew, wrong prefix, wrong algorithm) that operators
		// otherwise cannot diagnose from the client side. The reasons are
		// coarse labels only — they never reveal key material or which
		// specific byte of a signature failed.
		fields := []zap.Field{zap.String("reason", reason)}
		log := fbn.logger.Debug
		if reason == reasonWrongAudience {
			// The hash is all the sender told us about where it thought it was
			// pointing, so it has to be logged, and at a level operators see
			// without turning on debug: this is a config mismatch, not noise.
			fields = append(fields, zap.String("audience", parts[2]))
			log = fbn.logger.Warn
		}
		log("fbn auth denied", fields...)
		return ctx, fmt.Errorf("%w: %s", errDenied, reason)
	}

	cl := client.FromContext(ctx)
	cl.Auth = ad
	return client.NewContext(ctx, cl), nil
}

// verifyToken returns the auth data and "ok", or nil and a denial reason. The
// split token fields come back either way, for telemetry and denial logging.
func (fbn *fbnServerAuth) verifyToken(token string) (*authData, string, []string) {
	parts := strings.Split(token, "/")
	var prefix, pubkeyB58, audience, tsStr, sigB58 string
	switch len(parts) {
	case legacyTokenParts:
		prefix, pubkeyB58, tsStr, sigB58 = parts[0], parts[1], parts[2], parts[3]
	case tokenParts:
		prefix, pubkeyB58, audience, tsStr, sigB58 = parts[0], parts[1], parts[2], parts[3], parts[4]
	default:
		return nil, "malformed", parts
	}

	pc, ok := fbn.config.Prefixes[prefix]
	if !ok {
		return nil, "unknown_prefix", parts
	}

	// Only checked when the token actually carries the field: a 4-field token
	// is a pre-audience sender, not a sender claiming the empty audience. An
	// empty 5-field audience matches nothing and is denied.
	if accepted, ok := fbn.audiences[prefix]; ok && len(parts) == tokenParts {
		if _, match := accepted[audience]; !match {
			return nil, reasonWrongAudience, parts
		}
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return nil, "bad_timestamp", parts
	}
	skew := fbn.now().Sub(time.Unix(ts, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > fbn.config.MaxClockSkew {
		return nil, "stale_timestamp", parts
	}

	pubkey, err := base58.Decode(pubkeyB58)
	if err != nil {
		return nil, "bad_pubkey", parts
	}
	sig, err := base58.Decode(sigB58)
	if err != nil {
		return nil, "bad_signature", parts
	}

	signed := token[:strings.LastIndexByte(token, '/')]
	switch pc.SignatureAlgo {
	case SignatureAlgoEd25519:
		if len(pubkey) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
			return nil, "bad_key_size", parts
		}
		if err := checkEdwardsKey(pubkey); err != nil {
			return nil, keyErrReason(err), parts
		}
		if !ed25519.Verify(ed25519.PublicKey(pubkey), []byte(signed), sig) {
			return nil, "bad_signature", parts
		}
	case SignatureAlgoXEd25519:
		if len(pubkey) != 32 || len(sig) != ed25519.SignatureSize {
			return nil, "bad_key_size", parts
		}
		// Use the key conversion directly rather than xed25519Verify so a bad
		// or low-order key is reported as such instead of "bad_signature".
		vk, err := xed25519VerifyKey(pubkey)
		if err != nil {
			return nil, keyErrReason(err), parts
		}
		if !ed25519.Verify(vk, []byte(signed), sig) {
			return nil, "bad_signature", parts
		}
	default:
		// Unreachable given Config.Validate, but fail closed regardless.
		return nil, "bad_algo", parts
	}

	return &authData{
		prefix:        prefix,
		publicKey:     pubkeyB58,
		timestamp:     ts,
		signature:     sigB58,
		signatureAlgo: pc.SignatureAlgo,
	}, "ok", parts
}

func (fbn *fbnServerAuth) count(ctx context.Context, result string, extra ...attribute.KeyValue) {
	fbn.authAttempts.Add(ctx, 1,
		metric.WithAttributes(append(extra, attribute.String("result", result))...))
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

// signedPayload assembles the part of a token the signature covers. An empty
// audience produces the pre-audience 4-field form.
func signedPayload(prefix, pubkeyB58, audience string, ts time.Time) string {
	if audience == "" {
		return fmt.Sprintf("%s/%s/%d", prefix, pubkeyB58, ts.Unix())
	}
	return fmt.Sprintf("%s/%s/%s/%d", prefix, pubkeyB58, audience, ts.Unix())
}

// BuildToken assembles and signs a token. audience is the value from
// AudienceHash for the URL being dialed, or "" for the pre-audience form.
// Exported for use by clients and tests; the collector itself only verifies.
func BuildToken(prefix, audience string, priv ed25519.PrivateKey, ts time.Time) string {
	pub := priv.Public().(ed25519.PublicKey)
	signed := signedPayload(prefix, base58.Encode(pub), audience, ts)
	sig := ed25519.Sign(priv, []byte(signed))
	return signed + "/" + base58.Encode(sig)
}
