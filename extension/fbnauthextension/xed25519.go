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

// XEdDSA (Signal's construction, https://signal.org/docs/specifications/xeddsa/)
// lets an x25519 (Montgomery) keypair produce Ed25519-compatible signatures.
// Freenet nodes sign their bearer tokens with their x25519 transport key, so
// the token's pubkey IS the node's one network identity — the same key peers
// and UIs see. Verification needs no exotic crypto: convert the Montgomery
// public key to an Edwards point with sign bit 0, then run stock Ed25519
// verification.

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"time"

	"filippo.io/edwards25519"
	"filippo.io/edwards25519/field"
	"github.com/mr-tron/base58"
)

var (
	errBadMontgomeryKey = errors.New("x25519 public key does not map to an Edwards point")
	errBadEdwardsKey    = errors.New("public key is not a valid Edwards point")
	errLowOrderKey      = errors.New("public key is a low-order point")
)

// checkEdwardsKey rejects public keys that are not valid Edwards points or
// that lie in the small-order subgroup.
//
// crypto/ed25519.Verify is not strict (not ZIP-215) and accepts low-order
// public keys. Verification is [S]B == R + [k]A, so if A has small order and
// k is even then [k]A is the identity and the equation reduces to [S]B == R —
// satisfiable by anyone with R = [r]B, S = r and no private key at all. Half
// of all messages give an even k, so forgery costs about two tries. Reject
// such keys before verifying.
func checkEdwardsKey(pub []byte) error {
	p, err := new(edwards25519.Point).SetBytes(pub)
	if err != nil {
		return errBadEdwardsKey
	}
	if new(edwards25519.Point).MultByCofactor(p).Equal(edwards25519.NewIdentityPoint()) == 1 {
		return errLowOrderKey
	}
	return nil
}

// keyErrReason maps a key-rejection error to its denial/metric reason.
func keyErrReason(err error) string {
	if errors.Is(err, errLowOrderKey) {
		return "low_order_key"
	}
	return "bad_pubkey"
}

// xed25519VerifyKey converts a 32-byte x25519 (Montgomery u-coordinate)
// public key into the Ed25519 verifying key XEdDSA signatures verify under:
// the birational map y = (u-1)/(u+1), sign bit forced to 0.
func xed25519VerifyKey(mont []byte) (ed25519.PublicKey, error) {
	if len(mont) != 32 {
		return nil, errBadMontgomeryKey
	}
	u := make([]byte, 32)
	copy(u, mont)
	u[31] &= 0x7f // RFC 7748: the top bit of the u-coordinate is ignored
	fu, err := new(field.Element).SetBytes(u)
	if err != nil {
		return nil, errBadMontgomeryKey
	}
	one := new(field.Element).One()
	denom := new(field.Element).Add(fu, one)
	if denom.Equal(new(field.Element)) == 1 { // u == -1: no inverse
		return nil, errBadMontgomeryKey
	}
	y := new(field.Element).Multiply(
		new(field.Element).Subtract(fu, one),
		new(field.Element).Invert(denom),
	)
	// y < p so its top bit — the Edwards sign bit — is already 0.
	vk := y.Bytes()
	if err := checkEdwardsKey(vk); err != nil {
		return nil, err
	}
	return ed25519.PublicKey(vk), nil
}

// xed25519Verify checks an XEdDSA signature made with the x25519 private key
// matching mont. Keys that are not valid non-small-order Edwards points are
// rejected by xed25519VerifyKey before any verification is attempted.
func xed25519Verify(mont, message, sig []byte) bool {
	if len(sig) != ed25519.SignatureSize {
		return false
	}
	vk, err := xed25519VerifyKey(mont)
	if err != nil {
		return false
	}
	return ed25519.Verify(vk, message, sig)
}

// hash1Padding is XEdDSA's hash_1 domain separator: 2^256 - 1 - 1 in
// little-endian, i.e. 0xFE followed by 31 bytes of 0xFF.
func hash1Padding() [32]byte {
	var p [32]byte
	for i := range p {
		p[i] = 0xff
	}
	p[0] = 0xfe
	return p
}

// xed25519Sign produces an XEdDSA signature with an x25519 private key.
// z is the 64-byte randomness the construction hedges its nonce with.
// The collector itself only verifies; this exists for BuildTokenXEd25519
// (clients and tests).
func xed25519Sign(x25519Secret, message []byte, z [64]byte) ([]byte, error) {
	a, err := edwards25519.NewScalar().SetBytesWithClamping(x25519Secret)
	if err != nil {
		return nil, err
	}
	bigA := (&edwards25519.Point{}).ScalarBaseMult(a)
	// Force the Edwards sign bit to 0, negating the scalar to match.
	if bigA.Bytes()[31]&0x80 != 0 {
		a.Negate(a)
		bigA.ScalarBaseMult(a)
	}

	pad := hash1Padding()
	h := sha512.New()
	h.Write(pad[:])
	h.Write(a.Bytes())
	h.Write(message)
	h.Write(z[:])
	r, err := edwards25519.NewScalar().SetUniformBytes(h.Sum(nil))
	if err != nil {
		return nil, err
	}
	bigR := (&edwards25519.Point{}).ScalarBaseMult(r)

	h = sha512.New()
	h.Write(bigR.Bytes())
	h.Write(bigA.Bytes())
	h.Write(message)
	k, err := edwards25519.NewScalar().SetUniformBytes(h.Sum(nil))
	if err != nil {
		return nil, err
	}
	s := edwards25519.NewScalar().MultiplyAdd(k, a, r)

	sig := make([]byte, ed25519.SignatureSize)
	copy(sig[:32], bigR.Bytes())
	copy(sig[32:], s.Bytes())
	return sig, nil
}

// BuildTokenXEd25519 assembles and signs a token with an x25519 private key,
// deriving the public key from it. audience is the value from AudienceHash for
// the URL being dialed, or "" for the pre-audience form. Exported for use by
// clients and tests; the collector itself only verifies.
func BuildTokenXEd25519(prefix, audience string, x25519Secret []byte, ts time.Time) (string, error) {
	a, err := edwards25519.NewScalar().SetBytesWithClamping(x25519Secret)
	if err != nil {
		return "", err
	}
	pub := (&edwards25519.Point{}).ScalarBaseMult(a).BytesMontgomery()

	signed := signedPayload(prefix, base58.Encode(pub), audience, ts)
	var z [64]byte
	if _, err := rand.Read(z[:]); err != nil {
		return "", err
	}
	sig, err := xed25519Sign(x25519Secret, []byte(signed), z)
	if err != nil {
		return "", err
	}
	return signed + "/" + base58.Encode(sig), nil
}
