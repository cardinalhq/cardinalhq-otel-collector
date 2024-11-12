# cardinalhq-otel-collector

This repository contains three main components.

* Various Cardinal processors, extensions, exporters, and receivers.
* Example configurations and documentation.
* A Docker build of a collector with the CardinalHQ components, as well as
  a number of other commonly used components.

## CardinalHQ Ecosystem

CardinalHQ offers a flexible system where a collector runs in your environment, allowing full control over your data without vendor lock-in. Our custom exporters, processors, and extensions support use cases like automatic PII redaction, cost management, and operational recommendations—all while preserving full data fidelity in commodity storage, such as an S3 bucket. These capabilities enable quick and effective operational diagnosis.

Our SaaS solution offers control over your collector use cases, including remote configuration
of rules (without requiring a collector restart or downtime) and adding or removing exporters and receivers.
We also provide a comprehensive SaaS solution to store and manage your data securely should you prefer
not to use your own object storage.

Whether you choose to store data in your own S3 bucket or utilize our SaaS offering, CardinalHQ enables additional features on top of your data, including a user interface to explore, manage collectors, and configure features without editing YAML files.

The CardinalHQ collector integrates seamlessly with our SaaS platform, ensuring full telemetry data access, regardless of any cost-saving measures applied to your upstream provider. Each component can function independently, but doing so would require manual configuration and, in some cases, setting up custom API endpoints.

The CardinalHQ ecosystem comprises three main components—sources, sinks, and a common processing component—built from OpenTelemetry modules. For example, a Datadog sink can receive telemetry from the common processor, applying filtering, rate-limiting, and aggregation before sending data to Datadog. This structure allows for targeted cost reduction or data management when interacting with upstream vendors.

Cardinal is use-case focused, so instead of building a pipeline, you are configuring a higher level concept.  Cost savings by removing tags is one such use case, while PII redaction is another, and many more exist to
solve real-world telemetry problems.

See <url here> for more, including how to begin using Cardinal's collector and ecosystem.
