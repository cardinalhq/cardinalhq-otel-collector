# fbn_validator processor

Companion to the `fbn_auth` extension. Enforces that telemetry payloads match
the identity that authenticated the request: each resource must carry the
sender's public key and/or the fingerprint derived from it, and every one it
carries must match the authenticated transport identity — otherwise the
resource is dropped. Both expected values are derived from the verified auth
token, never read from the payload, so a sender cannot claim another node's
identity.

The processor fails closed, at two levels.

**Whole batch refused** — if there is no auth context, the auth prefix has no
configured validation rule, or the authenticated public key cannot be decoded,
nothing is forwarded and a permanent error is returned to the sender. The
sender is told its data was rejected rather than being handed a success it
would take for delivery; the error is permanent because no identity failure
improves on retry. Reasons: `no_auth_context`, `unknown_prefix`, `bad_pubkey`.
A batch whose resources are *all* individually dropped is refused the same way
(`no_matching_resources`). An empty batch is not an auth failure and is simply
skipped.

**Individual resources dropped** — within an accepted batch, resources that
fail the identity check are removed and the rest are forwarded. Reasons:
`pubkey_mismatch`, `fingerprint_mismatch`, `no_identity_attribute`.

## Telemetry

- `resources_dropped` (counter) — one per dropped resource, with a `reason`
  attribute taking any of the values above. Per-resource drops are also logged
  at debug level with the claimed versus authenticated identity; batch
  refusals are logged at warn.

## Validation algorithms

- `freenet` — resources are matched on two attributes:
  - `public_key_attribute` (default `freenet.node.pubkey`) must equal the
    full base58 public key from the auth token.
  - `fingerprint_attribute` (default `freenet.node.fingerprint`) must equal
    `base58(first 12 bytes of the base58-decoded public key)` — the id
    freenet UIs display.

  A resource carrying either attribute is validated against it; carrying
  both requires both to match; carrying neither drops the resource.

## Configuration

```yaml
processors:
  fbn_validator:
    prefixes:
      freenet:
        validation_algorithm: freenet
        # Optional; these are the defaults, matching what freenet nodes send.
        public_key_attribute: freenet.node.pubkey
        fingerprint_attribute: freenet.node.fingerprint
```

Place it early in the pipeline of the dedicated receive path whose receiver
uses the `fbn_auth` extension.
