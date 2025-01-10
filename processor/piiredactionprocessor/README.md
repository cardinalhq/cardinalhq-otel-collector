# piiredactionprocessor

The PII Redaction Processor uses pattern recgnotion and other validation
techniques to detect various types of personally identifiable information
in log messages, and will redact them.

By default, the PII is replaced with the literal string `REDACTED` but this
is configurable.

## PII Types

The following PII formats are supported:

* `email`: Email addresses or identifiers that appear to be email addresses.
* `ipv4`: IPv4 addresses in the a.b.c.d format.
* `phone`: Phone numbers (north american format only at this time).
* `ssn`: Social Security Numbers (ddd-dd-dddd format).
* `ccn`: Credit Card nubmers in a variety of formats.

Credit card numbers are checked for validity, so incorrect numbers may be passed through unchanged.

## Configuration

```yaml
processors:
  piiredaction:
    detectors:
      - email
      - phone
      - ssn
      - ccn
```

Default values are shown, and generally recommended, allowing for no configuration to be necessary.

If new detectors are added in the future, they may be added as on by default if they are safe to use
generally.
