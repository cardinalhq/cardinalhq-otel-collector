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
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"filippo.io/edwards25519"
	"filippo.io/edwards25519/field"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/extension/extensiontest"
)

func newTestExt(t *testing.T) *fbnServerAuth {
	cfg := createDefaultConfig().(*Config)
	cfg.Prefixes = map[string]PrefixConfig{
		"freenet": {SignatureAlgo: SignatureAlgoEd25519},
		"fnx":     {SignatureAlgo: SignatureAlgoXEd25519},
	}
	require.NoError(t, cfg.Validate())
	ext, err := newServerAuthExtension(cfg, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	return ext
}

// newAudienceExt is newTestExt with an audience list on the "freenet" prefix.
func newAudienceExt(t *testing.T, urls ...string) *fbnServerAuth {
	cfg := createDefaultConfig().(*Config)
	cfg.Prefixes = map[string]PrefixConfig{
		"freenet": {SignatureAlgo: SignatureAlgoEd25519, Audiences: urls},
	}
	require.NoError(t, cfg.Validate())
	ext, err := newServerAuthExtension(cfg, extensiontest.NewNopSettings(extensiontest.NopType))
	require.NoError(t, err)
	return ext
}

func newKey(t *testing.T) ed25519.PrivateKey {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return priv
}

func bearer(token string) map[string][]string {
	return map[string][]string{"Authorization": {"Bearer " + token}}
}

func TestAuthenticate_Valid(t *testing.T) {
	ext := newTestExt(t)
	priv := newKey(t)
	token := BuildToken("freenet", "", priv, time.Now())

	ctx, err := ext.Authenticate(context.Background(), bearer(token))
	require.NoError(t, err)

	auth := client.FromContext(ctx).Auth
	require.NotNil(t, auth)
	assert.Equal(t, "freenet", auth.GetAttribute(AttrPrefix))
	assert.Equal(t, base58.Encode(priv.Public().(ed25519.PublicKey)), auth.GetAttribute(AttrPublicKey))
	assert.Equal(t, SignatureAlgoEd25519, auth.GetAttribute(AttrSignatureAlgo))
	assert.NotEmpty(t, auth.GetAttribute(AttrSignature))
	assert.IsType(t, int64(0), auth.GetAttribute(AttrTimestamp))
}

func TestAuthenticate_CaseInsensitiveHeaderAndScheme(t *testing.T) {
	ext := newTestExt(t)
	token := BuildToken("freenet", "", newKey(t), time.Now())
	_, err := ext.Authenticate(context.Background(),
		map[string][]string{"authorization": {"bearer " + token}})
	require.NoError(t, err)
}

func TestAuthenticate_Denials(t *testing.T) {
	ext := newTestExt(t)
	priv := newKey(t)
	good := BuildToken("freenet", "", priv, time.Now())

	tests := []struct {
		name    string
		headers map[string][]string
		wantErr error
	}{
		{"no header", map[string][]string{}, errNoAuthHeader},
		{"wrong scheme", map[string][]string{"Authorization": {"Basic " + good}}, errNoAuthHeader},
		{"unknown prefix", bearer(BuildToken("other", "", priv, time.Now())), errDenied},
		{"malformed", bearer("freenet/only-two-parts"), errDenied},
		{"stale timestamp", bearer(BuildToken("freenet", "", priv, time.Now().Add(-time.Hour))), errDenied},
		{"future timestamp", bearer(BuildToken("freenet", "", priv, time.Now().Add(time.Hour))), errDenied},
		// Timestamp moved one second inside the skew window: passes the
		// freshness check, so it is the signature that must reject it.
		{"tampered timestamp", bearer(retimestamp(good, 1)), errDenied},
		{"too many parts", bearer(good + "/extra/more"), errDenied},
		{"garbage pubkey", bearer(fmt.Sprintf("freenet/!!!/%d/%s", time.Now().Unix(),
			base58.Encode(make([]byte, ed25519.SignatureSize)))), errDenied},
		{"wrong key signature", bearer(swapSignature(good, newKey(t))), errDenied},
		{"non-numeric timestamp", bearer(fmt.Sprintf("freenet/%s/notanumber/%s",
			base58.Encode(make([]byte, ed25519.PublicKeySize)),
			base58.Encode(make([]byte, ed25519.SignatureSize)))), errDenied},
		// ed25519.Verify panics on a wrong-length key, so the size check is
		// the only thing standing between this token and a collector crash.
		{"short pubkey", bearer(fmt.Sprintf("freenet/%s/%d/%s",
			base58.Encode(make([]byte, 16)), time.Now().Unix(),
			base58.Encode(make([]byte, ed25519.SignatureSize)))), errDenied},
		{"short signature", bearer(fmt.Sprintf("freenet/%s/%d/%s",
			base58.Encode(make([]byte, ed25519.PublicKeySize)), time.Now().Unix(),
			base58.Encode(make([]byte, 16)))), errDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ext.Authenticate(context.Background(), tt.headers)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// The denial reason is returned to the sender so operators can diagnose a
// misconfigured client without server-side debug logging.
func TestAuthenticate_ErrorCarriesReason(t *testing.T) {
	ext := newTestExt(t)
	priv := newKey(t)

	for reason, headers := range map[string]map[string][]string{
		"unknown_prefix":  bearer(BuildToken("other", "", priv, time.Now())),
		"malformed":       bearer("freenet/only-two-parts"),
		"stale_timestamp": bearer(BuildToken("freenet", "", priv, time.Now().Add(-time.Hour))),
	} {
		t.Run(reason, func(t *testing.T) {
			_, err := ext.Authenticate(context.Background(), headers)
			require.ErrorIs(t, err, errDenied)
			assert.Contains(t, err.Error(), reason)
		})
	}
}

// The audience field is what stops the collector a token was sent to from
// replaying it at another collector on the same scheme.
func TestAuthenticate_Audience(t *testing.T) {
	const ourURL = "https://otel.example.com/v1/metrics"
	const theirURL = "https://other.example.com/v1/metrics"
	ours, err := AudienceHash(ourURL)
	require.NoError(t, err)
	theirs, err := AudienceHash(theirURL)
	require.NoError(t, err)
	priv := newKey(t)

	tests := []struct {
		name   string
		ext    *fbnServerAuth
		token  string
		reason string // empty means the token must authenticate
	}{
		{"matching audience", newAudienceExt(t, ourURL),
			BuildToken("freenet", ours, priv, time.Now()), ""},
		{"any listed URL matches", newAudienceExt(t, theirURL, ourURL),
			BuildToken("freenet", ours, priv, time.Now()), ""},
		{"audience for another collector", newAudienceExt(t, ourURL),
			BuildToken("freenet", theirs, priv, time.Now()), reasonWrongAudience},
		// A signed but empty audience field must not read as "unbound", or a
		// sender could opt out of the binding at will.
		{"empty audience field", newAudienceExt(t, ourURL),
			buildEmptyAudienceToken(priv), reasonWrongAudience},
		// Transition: senders upgrade on their own schedule, so a token with no
		// audience field at all is still accepted (with no binding).
		{"pre-audience token", newAudienceExt(t, ourURL),
			BuildToken("freenet", "", priv, time.Now()), ""},
		// No list configured: the field is carried but not checked.
		{"unconfigured audiences", newTestExt(t),
			BuildToken("freenet", theirs, priv, time.Now()), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.ext.Authenticate(context.Background(), bearer(tt.token))
			if tt.reason == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, errDenied)
			assert.Contains(t, err.Error(), tt.reason)
		})
	}
}

// A five-field token whose audience field is the empty string, correctly signed.
func buildEmptyAudienceToken(priv ed25519.PrivateKey) string {
	signed := fmt.Sprintf("freenet/%s//%d",
		base58.Encode(priv.Public().(ed25519.PublicKey)), time.Now().Unix())
	return signed + "/" + base58.Encode(ed25519.Sign(priv, []byte(signed)))
}

// Stripping the audience field to reach the unbound legacy path breaks the
// signature, so the transition window is not a downgrade attack.
func TestAuthenticate_AudienceCannotBeStripped(t *testing.T) {
	const ourURL = "https://otel.example.com/v1/metrics"
	ours, err := AudienceHash(ourURL)
	require.NoError(t, err)
	priv := newKey(t)
	ext := newAudienceExt(t, ourURL)

	parts := strings.Split(BuildToken("freenet", ours, priv, time.Now()), "/")
	stripped := strings.Join([]string{parts[0], parts[1], parts[3], parts[4]}, "/")

	_, err = ext.Authenticate(context.Background(), bearer(stripped))
	require.ErrorIs(t, err, errDenied)
	assert.Contains(t, err.Error(), "bad_signature")
}

// swapSignature re-signs the payload with a different key than the embedded pubkey.
func swapSignature(token string, other ed25519.PrivateKey) string {
	signed := token[:strings.LastIndexByte(token, '/')]
	return signed + "/" + base58.Encode(ed25519.Sign(other, []byte(signed)))
}

// retimestamp shifts a token's timestamp by delta seconds, leaving the
// original signature in place.
func retimestamp(token string, delta int64) string {
	parts := strings.Split(token, "/")
	ts, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		panic(err)
	}
	parts[2] = strconv.FormatInt(ts+delta, 10)
	return strings.Join(parts, "/")
}

// Vector generated by the freenet Rust implementation (xeddsa crate 1.1,
// x25519 secret = 32 bytes of 0x07). Locks the Go XEdDSA construction to the
// Rust one — if either side drifts, this fails.
//
// The message is a token from when the format carried a uniqueness field, so
// it is not a well-formed token under any current layout. That does not weaken
// what the vector pins: xed25519Verify treats the message as opaque bytes, and
// it is the signature construction, not the token layout, that must stay in
// agreement across implementations. Regenerating it over a payload with an
// audience field would be equally valid — the canonical-URL agreement that the
// audience field actually depends on is pinned by TestAudienceHashGolden and
// TestAudienceHashCanonicalization instead.
const (
	rustVectorPubKey  = "2L54SXdEHm5mraF2X2GPid3m4PSkwVehEvhk487mWTx8"
	rustVectorPayload = "freenet/2L54SXdEHm5mraF2X2GPid3m4PSkwVehEvhk487mWTx8/1754300000/testnonce"
	rustVectorSig     = "3q7ePX3kUoq5st5b5HvuiCg3eFgLatwAY2HsfbqmKTVkPiq4MrvDTLxxpj5Vg1wWKyeo2vQZG1Le8jbPVXCA9SZT"
)

func TestXEd25519_RustVectorVerifies(t *testing.T) {
	pub, err := base58.Decode(rustVectorPubKey)
	require.NoError(t, err)
	sig, err := base58.Decode(rustVectorSig)
	require.NoError(t, err)

	assert.True(t, xed25519Verify(pub, []byte(rustVectorPayload), sig))
	assert.False(t, xed25519Verify(pub, []byte(rustVectorPayload+"x"), sig),
		"tampered payload must fail")
	sig[0] ^= 1
	assert.False(t, xed25519Verify(pub, []byte(rustVectorPayload), sig),
		"tampered signature must fail")
}

// End-to-end xed25519 authentication over a current-format token, signed with
// the same x25519 secret as the Rust vector above.
func TestAuthenticate_XEd25519Token(t *testing.T) {
	ext := newTestExt(t)
	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = 0x07
	}

	token, err := BuildTokenXEd25519("fnx", "", secret, time.Now())
	require.NoError(t, err)
	ctx, err := ext.Authenticate(context.Background(), bearer(token))
	require.NoError(t, err)

	auth := client.FromContext(ctx).Auth
	require.NotNil(t, auth)
	assert.Equal(t, rustVectorPubKey, auth.GetAttribute(AttrPublicKey),
		"public key must match the one the Rust signer derives from this secret")
	assert.Equal(t, SignatureAlgoXEd25519, auth.GetAttribute(AttrSignatureAlgo))
}

func TestXEd25519_GoRoundTrip(t *testing.T) {
	ext := newTestExt(t)
	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)

	token, err := BuildTokenXEd25519("fnx", "", secret, time.Now())
	require.NoError(t, err)
	_, err = ext.Authenticate(context.Background(), bearer(token))
	require.NoError(t, err)

	// The same token under an ed25519-configured prefix must fail: a
	// Montgomery key is not a valid Ed25519 key for the same bytes.
	freenetToken := "freenet" + strings.TrimPrefix(token, "fnx")
	_, err = ext.Authenticate(context.Background(), bearer(freenetToken))
	assert.Error(t, err, "signature covers the prefix, so a swapped prefix must fail")
}

// Low-order public keys let anyone forge a signature with no private key:
// verification is [S]B == R + [k]A, so when A has small order and k is even,
// [k]A vanishes and R = [r]B, S = r satisfies it. crypto/ed25519.Verify does
// not reject such keys, so this must be caught before verifying — on both
// algorithms. Without checkEdwardsKey these tokens authenticate.
func TestAuthenticate_LowOrderKeyForgeryRejected(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		pubkey []byte
	}{
		// Montgomery u=0 maps to Edwards y=-1, a point of order 2.
		{"xed25519 u=0", "fnx", make([]byte, 32)},
		// y=-1 (p-1, sign bit 0) encoded directly as an Ed25519 key.
		{"ed25519 y=-1", "freenet", edwardsYMinusOne()},
		// The identity point: order 1.
		{"ed25519 identity", "freenet", func() []byte {
			b := make([]byte, 32)
			b[0] = 1
			return b
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext := newTestExt(t)
			token, ok := forgeLowOrderToken(t, tt.prefix, tt.pubkey)
			if !ok {
				t.Skip("key is not low order under this construction")
			}
			_, err := ext.Authenticate(context.Background(), bearer(token))
			require.Error(t, err, "forged low-order-key token must be denied")
			assert.ErrorIs(t, err, errDenied)
		})
	}
}

func edwardsYMinusOne() []byte {
	b := make([]byte, 32)
	b[0] = 0xec
	for i := 1; i < 31; i++ {
		b[i] = 0xff
	}
	b[31] = 0x7f
	return b
}

// forgeLowOrderToken builds a token that passes ed25519 verification against
// a low-order key without any private key, by walking the timestamp forward
// (staying inside the skew window) until the challenge scalar k is even.
func forgeLowOrderToken(t *testing.T, prefix string, pubkey []byte) (string, bool) {
	t.Helper()

	var vk []byte
	if prefix == "fnx" {
		// Reproduce the Montgomery->Edwards map without the low-order guard.
		if len(pubkey) != 32 {
			return "", false
		}
		u := new(field.Element)
		if _, err := u.SetBytes(pubkey); err != nil {
			return "", false
		}
		one := new(field.Element).One()
		denom := new(field.Element).Add(u, one)
		if denom.Equal(new(field.Element)) == 1 {
			return "", false
		}
		vk = new(field.Element).Multiply(
			new(field.Element).Subtract(u, one),
			new(field.Element).Invert(denom),
		).Bytes()
	} else {
		vk = pubkey
	}

	p, err := new(edwards25519.Point).SetBytes(vk)
	require.NoError(t, err, "test key must be a valid Edwards point")
	if new(edwards25519.Point).MultByCofactor(p).Equal(edwards25519.NewIdentityPoint()) != 1 {
		return "", false
	}

	rb := make([]byte, 64)
	rb[0] = 9
	r, err := edwards25519.NewScalar().SetUniformBytes(rb)
	require.NoError(t, err)
	bigR := new(edwards25519.Point).ScalarBaseMult(r)

	for i := range 64 {
		signed := fmt.Sprintf("%s/%s/%d", prefix, base58.Encode(pubkey), time.Now().Unix()+int64(i))
		h := sha512.New()
		h.Write(bigR.Bytes())
		h.Write(vk)
		h.Write([]byte(signed))
		k, err := edwards25519.NewScalar().SetUniformBytes(h.Sum(nil))
		require.NoError(t, err)
		if k.Bytes()[0]&1 != 0 {
			continue // need k even so [k]A is the identity
		}
		sig := make([]byte, ed25519.SignatureSize)
		copy(sig[:32], bigR.Bytes())
		copy(sig[32:], r.Bytes())
		return signed + "/" + base58.Encode(sig), true
	}
	t.Fatal("no even-k nonce found")
	return "", false
}

func TestXEd25519_UMinusOneFailsClosed(t *testing.T) {
	// u = -1 = p-1 has no (u+1) inverse; must be rejected, not panic.
	pMinusOne := [32]byte{0xec, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}
	_, err := xed25519VerifyKey(pMinusOne[:])
	assert.Error(t, err)
	assert.False(t, xed25519Verify(pMinusOne[:], []byte("m"), make([]byte, 64)))
}
