# chqtagcacheextension

This extension is used as a shared resource across collectors running as a
group to share incoming Datadog host tags.  It is used only by the
`chqdatadogreceiver`.

When running a collector with more than one Kubernetes pod to receive
Datadog agent telemetry, which pod a specific set of data goes to
may be random.  However, the Datadog agent allows setting always-present
tags to decorate telemetry.  This is done not by changing the data, but to
send these tags to a Datadog endpoint, and their back-end applies these
host level tags.

This extension serves that purpose, and when more than one pod is
running as a deployment, these tags are shared and all collectors
can correctly apply those tags.

## Configuration

```yaml
extensions:
  chqtagcache:
    auth:
      authenticator: "chqauth"
    endpoint: "https://api.us-east-2.aws.cardinalhq.io"
```

Any authentication method may be used.  The back-end server uses this authentication
context to segregate the host tags to that collector deployment.

The use of this extension outside of Cardinal's ecosystem is not possible by
implementing the API defined in `cache_http.go`
