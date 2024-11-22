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

After aggregation, the additional attributes are added to the emitted data points.  This can
be used for various purposes, but is intended to allow a way to guarantee unique attributes
for any given timestamp even when aggregating in multiple pods.

Note:  using the loadbalancing exporter is not recommended prior to or after this processor
without adding a collector-instance-unique value to the additional_attributes config, and
is not necessary with a unique field.

## Configuration

```yaml
processors:
  aggregation:
    metric_aggregation:
      interval: 10s
    additional_attributes:
      alice: in-wonderland
      bobs: your-uncle
```

The interval defines the collection time before a time series is output.  The minimum value
is `1s`.
