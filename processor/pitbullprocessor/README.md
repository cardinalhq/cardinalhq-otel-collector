# pitbullprocessor

This processor is one of the main workhorses in the Cardinal Ecosystem.
It's role is to apply rules obtained from its defined `chqconfig` extension
to the telemetry passing through it.  This can cause logs and spans to be sampled,
dropped, or transformed.  Metrics can be aggregated or dropped entirely, or
otherwise transformed.

Each incoming rule is applied in order to the telemetry items received and then
statements are applied.  Sampling of logs is handled specially, as is dropping
telemetry entirely.

All rules are written in OTTL, with some specific attributes set to indicate
dropping the entire item or that a datapoint should be aggregated after removing
an attribute.

## Configuration

```yaml
processors:
  pitbull/8bf0a2e0-8cc3-405e-9915-8eb8da17aadc:
    configuration_extension: "chqconfig"
```

All rule configuration is obtained dynamically at run-time from the named `chqconfig` extension.
