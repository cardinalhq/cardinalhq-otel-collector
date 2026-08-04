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

package fbnvalidatorprocessor

import (
	"errors"
	"fmt"
)

const (
	// ValidationAlgoFreenet validates resources against the authenticated
	// transport public key. A resource must carry the full base58 public key
	// (public_key_attribute), the derived fingerprint — base58 of the first
	// 12 bytes of the decoded key (fingerprint_attribute) — or both; every
	// one present must match, and a resource with neither is dropped. Both
	// expected values are derived from the auth context, never trusted from
	// the payload, so a node cannot claim another node's identity.
	ValidationAlgoFreenet = "freenet"

	defaultPublicKeyAttribute   = "freenet.node.pubkey"
	defaultFingerprintAttribute = "freenet.node.fingerprint"
)

type PrefixConfig struct {
	ValidationAlgorithm string `mapstructure:"validation_algorithm"`
	// PublicKeyAttribute is the resource attribute carrying the full base58
	// public key. Default "freenet.node.pubkey".
	PublicKeyAttribute string `mapstructure:"public_key_attribute"`
	// FingerprintAttribute is the resource attribute carrying the UI-facing
	// fingerprint (base58 of the first 12 bytes of the decoded public key).
	// Default "freenet.node.fingerprint".
	FingerprintAttribute string `mapstructure:"fingerprint_attribute"`
}

type Config struct {
	// Prefixes maps a token prefix (as authenticated by the fbn_auth
	// extension) to how payloads for that prefix are validated. Data whose
	// auth prefix is not listed here is dropped (fail closed).
	Prefixes map[string]PrefixConfig `mapstructure:"prefixes"`
}

var errNoPrefixes = errors.New("at least one prefix must be configured")

func (cfg *Config) Validate() error {
	if len(cfg.Prefixes) == 0 {
		return errNoPrefixes
	}
	for prefix, pc := range cfg.Prefixes {
		if pc.ValidationAlgorithm != ValidationAlgoFreenet {
			return fmt.Errorf("prefix %q: unsupported validation_algorithm %q (supported: %s)",
				prefix, pc.ValidationAlgorithm, ValidationAlgoFreenet)
		}
	}
	// Empty attribute names are defaulted where they are used, not here:
	// Validate checks, it does not mutate.
	return nil
}
