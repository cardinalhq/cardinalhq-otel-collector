# chqstatsprocessor

This processor is a component of the Cardinal Ecosystem which computes various
statistics on the data flowing through it.  This is used to compute cost
savings and other data flows for presenting in Cardinal's SaaS UI.

While this processor can be used outside of the Cardinal Ecosystem, it would
require building an API and processing to receive the statistic data.

Various configuration details are obtained at run-time from the named `chqconfig`
extension.

## Configuration

```yaml
processors:
  chqexemplar/f32d43b8-f7c4-47c1-846f-241a7516a095:
    endpoint: "https://stats-receiver.global.aws.cardinalhq.io"
```
