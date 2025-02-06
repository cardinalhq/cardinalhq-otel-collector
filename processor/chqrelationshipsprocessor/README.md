# chqrelationshipsprocessor

This processor is required for Cardinal's "Chip" AI to function.

This processor is a component of the Cardinal Ecosystem which detects and
reports relationships between entities found (such as a service, a pod, etc)

While this processor can be used outside of the Cardinal Ecosystem, it would
require building an API and processing to receive the data.

## Configuration

```yaml
processors:
  chqrelationshipsprocessor/f32d43b8-f7c4-47c1-846f-241a7516a095:
    reporting:
      auth:
        authenticator: "chqauth"
      endpoint: "https://stats-receiver.global.aws.cardinalhq.io"
```
