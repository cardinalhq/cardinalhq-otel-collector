# CardinalHQ Entity Graph Exporter

This exporter is required for Cardinal's "Chip" AI to function.

This exporter is a component of the Cardinal Ecosystem which detects and
reports relationships between entities found (such as a service, a pod, etc)

While this exporter can be used outside of the Cardinal Ecosystem, it would
require building an API and processing to receive the data.

## Configuration

```yaml
exporters:
  chqentitygraph/f32d43b8-f7c4-47c1-846f-241a7516a095:
    reporting:
      auth:
        authenticator: "chqauth"
      endpoint: "https://stats-receiver.global.aws.cardinalhq.io"
```

In this sample configuration, we are using an authenticator where the outgoing headers are
configured.  If desired, these headers can be set directly:

```yaml
exporters:
  chqentitygraph/f32d43b8-f7c4-47c1-846f-241a7516a095:
    reporting:
      endpoint: "https://stats-receiver.global.aws.cardinalhq.io"
      headers:
        x-cardinalhq-api-key: ${env:CARDINAL_API_KEY}
        x-cardinalhq-collector-id: ${env:CARDINAL_COLLECTOR_ID}
```

When sending this data to Cardinal for Chip AI, these values can be obtained through the
Cardinal UI.

If you are using this to send the data collected to somewhere other than
Cardinal, you would adjust these as needed.

## Building a collector

This exporter is included in the Cardinal Opentelemetry Collector container images.
If you wish to build your own collector with this included, and are using the
[OpenTelemetry Collector Builder](https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder)
add the following lines to your builder manifest file:

```yaml

```

See
[this guide](https://opentelemetry.io/docs/collector/custom-collector/)
on how to build a custom collector image.
