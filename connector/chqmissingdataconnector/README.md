# chqmissingdataconnector

This connector is part of the Cardinal Ecosystem.
Its role is to watch for incoming metrics, and emit a new time series with the
metric name prefixed to indicate the last time an item was seen, as a gauge.

This can be used by simple alerting rules to check that the gauge never exceeds
some chosen value.

## Configuration

```yaml
connectors:
  chqmissingdataconnector:
    maximum_age: 24h
    interval: 1m
    name_prefix: "missingdata"
    resource_attributes_to_copy:
      - service.name
```

Defaults are shown.
