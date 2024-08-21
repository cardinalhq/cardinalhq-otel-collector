# CardinalHQ OpenTelemetry Collector

This helm chart will install a customized Open Telemetry docker image
that implements Cardinal's magic.

It creates a single deployment that can receive OTEL format telemetry
or Datadog telemetry, depending on options set in your `values.yaml` file.

Currently, these protocols and ports are used by default, but can be
changed:

| Name     | Port | Types                 | Description                         |
| -------- | ---- | --------------------- | ----------------------------------- |
| otlpgrpc | 4317 | Metrics, Logs, Traces | Receive OTLP telemetry over GRPC    |
| otlphttp | 4318 | Metrics, Logs, Traces | Receive OTLP telemetry over HTTP    |
| datadog  | 8126 | Metrics, Logs, Traces | Receive Datadog telemetry over GRPC |

## Service Account

A service account is created, although no roles are assigned.  This is to allow
for cloud-specific permissions to be applied to the pods.

## Secrets

Three secrets are required:

* A Datadog API key.  Currently, this is required even if
Datadog is not being sent any telemetry, in which case it
can be a placeholder value.
* A CardinalHQ API key.  This is always required.
* AWS credentials.  If S3 is being used (or an S3 compatible
provider) this must have valid entries.

These secrets should be placed in `values.yaml`.

Alternatively, an existing secret name can be used for one
or more of these, allowing using an external secret manager
to inject these into the namespace.

See the default `values.yaml` file for more details on
these and other settings.

### Helm Install

```sh
% helm install --namespace cardinal foo oci://public.ecr.aws/b1d7g2f3/cardinalhq-otel-collector
```

In addition to the  collector deployment, a `ClusterIP` service
will be created.  This has open ports for the data formats supported for
receivers.
