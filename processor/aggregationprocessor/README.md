# aggregationprocessor

This processor aggregates datapoints which are of the same type and have the same
attribute values.  It is typical for a previous processor to drop some attributes,
which could cause datapoints with the same attributes and timestamp to have different
values, and various vendors handle this differently.  This processor combines those
into a single comprehensive datapoint to avoid this problem.

If a previous processor removed an attribute to reduce cardinality of metric datapoints,
it should set a new attriute `_aggregated` to `true`, and this processor will consume it
and aggregate that datapoint, causing it to no longer flow through subsequent
processors or exporters until emitted as an aggregate.

The aggregated datapoints have a timestamp that is truncated to the `interval` configuration.

## Configuration

```yaml
processors:
  aggregation:
    metric_aggregation:
      interval: 10s
```

The interval defines the collection time before a time series is output.  The minimum value
is `1s`.
