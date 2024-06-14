# CardinalHQ OpenTelemetry Collector

This helm chart will install a customized Open Telemetry docker image
that implements Cardinal's magic.

It will set up a two-level deployment of the collector pods, where
the L1 pods will receive telemetry in various wire formats such
as `otlphttp`, `otlpgrpc`, and `datadog`.  The L1 directly
handles logs, while metrics and traces are load balanced
using a key to ensure they arrive at the correct L2 balancer
to properly group the data.

In the L2 collector, traces are grouped by trace ID, and are
processed by Cardinal's components.  Metrics are handled in
a similar manner, including converting to a delta temporality
if necessary.

## Installation

Two secrets are required.  One holds your Datadog API key
to allow the collectors to send data to Datadog after filtering
and aggregation.  Another holds the API key for CardinalHQ's
SaaS receiver, to receive the full fidelity data stream,
statistics, and to allow remote configuration.

These secrets should be placed in `values.yaml`.

Alternatively, an existing secret name can be used for one
or both of these, allowing using an external secret manager
to inject these into the namespace.

See the default `values.yaml` file for more details on
these and other settings.

### Helm Install

```sh
% helm install --namespace cardinal foo oci://public.ecr.aws/b1d7g2f3/cardinalhq-otel-collector --version 0.1.3
```

In addition to the two collector deployments for L1 and L2, a `ClusterIP` service
will be created.  This has open ports for the data formats supported for
receivers.

Data should be sent from the Datadog agent or OTLP emitters only to
the L1 service.  The L2 service should only receive data from the L1
tier.
