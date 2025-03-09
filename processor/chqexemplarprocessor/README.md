# chqexemplarprocessor

This processor is a component of the Cardinal Ecosystem collects
exemplars of various signal types with different resource and
item-level attributes, and sends them to Cardinal's Chip AI
for use in building results.

While this processor can be used outside of the Cardinal Ecosystem, it would
require building an API and processing to receive the statistic data.

## Configuration

```yaml
processors:
  chqexemplar:
    endpoint: "https://chip-ingest.global.aws.cardinalhq.io"
```
