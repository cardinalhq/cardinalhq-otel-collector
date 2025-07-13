# chqspannerprocessor

This processor is used to sneak in a synthetic server span when we see a call to a hosted database
service.  This is used by Cardinal to make it easy to associate a service that calls a database
with a database "service" that we track.  The net effect is that our call graph closes, and
all the databases served by a given database host can be related to the service, and services
using the same database host can share fate in our analysis of problems.

## Configuration

```yaml
processors:
  chqspanner:
```

No configuration is needed.
