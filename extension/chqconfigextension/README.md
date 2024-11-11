# chqconfigextension

This extension is the building block for the Cardinal collector dynamic update system.
This extension will contact a service and poll for configuration of various types of
other components used inside a Cardinal Open Telemetry Collector.

## Configuration

```yaml
extensions:
  chqconfig:
    source:
      auth:
        authenticator: "chqauth"
      endpoint: "https://api.global.aws.cardinalhq.io/api/v2/samplerConfig"
```

While any authentication type can be used, the example uses the Cardinal control pane's
auth requirements and endpoint.

The endpoint can be a http:// https:// or file:// URI method.  All types will be
polled for configuration, so using a local file is possible as well as data from a
service API endpoint.

## How it works

Periodically, the extension polls the endpoint for a configuration change.  When a
change is detected, it is distributed to any users of this extension.  Many Cardinal
processors use this to obtain their runtime configuration.

The Open Telemetry Collector pipelines are not modified by this extension.  It is only
used to inject dynamic rules, rate limiting, and other features into an existing
pipeline.
