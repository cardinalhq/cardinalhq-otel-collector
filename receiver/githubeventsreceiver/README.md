# githubeventsreceiver

This receiver obtains github events and converts them into log items.  This is different
than the community contrib github metrics processor, which is more tracking of
data on the number of open issues.  This receiver is intended to convert actions like
a pull request was opened into an OTEL event that can be processed as part of the event/log processing
pipeline.

Events are sent from Github via a web hook.

## Configuration

```yaml
receivers:
  githubevents:
    endpoint: 0.0.0.0:19418
    secret: "${env:GITHUB_SECRET}"
    path: /events
```

The endpoint defaults to external access on port 19418.

The path is where the github webhook should send events, and each event is validated with the
provided secret before accepting it.
