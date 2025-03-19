# chqsynthetics

This extension is part of the Cardinal Ecosystem.
It's role is to periodically poll HTTP endpoints to ensure they are responding.

While this extension can be used outside the Cardinal Collector Ecosystem,
it would require building a server to deliver the dynamic configuration.

## Configuration

```yaml
extensions:
  chqsynthetics:
    configuration_extension: "chqconfig"
```
