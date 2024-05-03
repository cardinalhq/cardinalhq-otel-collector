# CardinalHQ AWS S3 Exporter for OpenTelemetry Collector

## Summary

This exporter writes a Parquet format file to S3.  The format of this file is specific to CardinalHQ's ingest system, although it can be used by others.  No guarantees that the format will not change are made.

It differs from the AWS S3 exporter in that it will only write Parquet, and bundles (in memory) blocks of metrics, logs, and traces in a tabular format prior to exporting them.  The Parquet table is generated in memory, and then written to S3.

Metadata is added to the S3 file to mark the pod's name (if the environment variable `POD_NAME` is set).

Different replicasets of collectors should write to different S3 buckets or prefixes within the same bucket.  The CardinalHQ ingestion step expects all the files written to a given location to be from similar sources.

## Exporter Configuration

The following exporter configuration parameters are supported.

### S3 configuration

| Name |Description | Default |
|:-|:-|--|
| `region` | AWS region. | "us-east-1" |
| `s3_bucket` | S3 bucket | |
| `s3_prefix` | prefix for the S3 key (root directory inside bucket). | |
| `s3_partition` | time granularity of S3 key: hour or minute.  Keep this set to "minute" for CardinalHQ to process the files. | "minute" |
| `role_arn` | the Role ARN to be assumed | |
| `file_prefix` | file prefix defined by user | |
| `endpoint` | overrides the endpoint used by the exporter instead of constructing it from `region` and `s3_bucket` | |
| `s3_force_path_style` | [set this to `true` to force the request to use path-style addressing](http://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html) | false |
| `disable_ssl` | set this to `true` to disable SSL when sending requests | false |

### Timeboxes

Output from each telemetry type is grouped into intervals, with a grace period before emitting
the collected telemetry for that time interval.

| Name |Description | Default |
|:-|:-|--|
| `interval` | The time interval to break files into, in milliseconds. | 0 |
| `grace_period` | The time to wait after an interval would normally close, in milliseconds, in case data is slightly delayed in arrival.  This needs to be sufficient to handle anything upstream that may cause delays. | 0 |

If the interval is set to 0 for a telemetry type, that type is not written.

## Example Configuration

Following example configuration defines to store output in 'eu-central' region and bucket named 'databucket'.

```yaml
exporters:
  awss3:
    s3uploader:
      region: 'eu-central-1'
      s3_bucket: 'databucket'
      s3_prefix: 'metric'
      s3_partition: 'minute'
    timeboxes:
      logs:
        interval: 60000
        grace_period: 10000
      metrics:
        interval: 10000
        grace_period: 20000
      traces:
        interval: 60000
        grace_period: 10000
```

Logs and traces will be stored inside 'databucket' in the following path format.

```console
metric/year=XXXX/month=XX/day=XX/hour=XX/minute=XX
```

## AWS Credential Configuration

This exporter follows default credential resolution for the [aws-sdk-go](https://docs.aws.amazon.com/sdk-for-go/api/index.html).

Follow the [guidelines](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html) for the credential configuration.

### OpenTelemetry Collector Helm Chart for Kubernetes

For example, when using OpenTelemetry Collector Helm Chart you could use `extraEnvs` in the values.yaml.

```yaml
extraEnvs:
- name: AWS_ACCESS_KEY_ID
  value: "< YOUR AWS ACCESS KEY >"
- name: AWS_SECRET_ACCESS_KEY
  value: "< YOUR AWS SECRET ACCESS KEY >"
```

## Uses Code From

The code that implements the S3 writer is a slightly modified version from the OpenTelemetry Contrib AWS S3 exporter.  It was modified to accept a `io.Reader` rather than a `[]bytes` array to prevent additional data copies.
