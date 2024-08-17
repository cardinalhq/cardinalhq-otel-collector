# cardinalhq-otel-collector

CardinalHQ **todo**

This repository contains three main components.

* The CardinalHQ decorator processor.
* Example configuration to use the CardinalHQ processor.
* A Docker build of a collector with the CardinalHQ processor, as well as
  a number of other common processors, exporters, and other components.

## CardinalHQ Decorator

The CardinalHQ decorator is intended too be used in ocnjuction with CardinalHQ's
SaaS offering, where full telemetry data is available regardless of what
cost reduction is applied to your upstream telemetry provider.

CardinalHQ's decorator processor examines logs, metrics, and traces and
makes decisions based on configured rules.  These rules are able to
be changed without restarting the collector.

Based on the decisions made, additional attributes are added to the
telemetry.  This is then used to decide what telementry should be sent
to your upstream providers.

CardinalHQ's SaaS product allows controlling a fleet of collectors
to handle all three telemetry types, configures rules to lower your
upstream costs, and will maintain all dimensions and fidelity of your
telemetry.  To use this service, you must [sign up.]

### Logs

Log telemetry is analyzed by service or by pattern.  The patterns
are an integer representation of logs where similr log messages
have the same integer value.

Rate limiting can be applied at the service level or the <service, fingerprint>
level.


### Metrics

Metrics are matched based on a scope, and then tags are removed based on rules.
The resulting metric is aggregated and the lower cardinality aggergate is
periodically transmitted to your upstream provider.


## Getting Started

### Install and Configure CardinalHQ Collector

...



## Repository

... what's here, layout, etc.

## Contributing

...
