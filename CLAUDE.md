# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is the CardinalHQ OpenTelemetry Collector, a custom distribution of the OpenTelemetry Collector that includes CardinalHQ-specific components alongside standard OTel components. The repository contains custom receivers, processors, exporters, connectors, and extensions designed for telemetry data management, cost optimization, and operational insights.

## Development Commands

### Core Development Tasks
- `make all` - Build all targets (equivalent to building bin/cardinalhq-otel-collector)
- `make test` - Run all tests across all modules
- `make lint` - Run golangci-lint across all modules with the project's .golangci.yaml config
- `make check` - Run pre-commit checks (test + license-check + lint)
- `make generate` - Generate code across all modules using go generate

### Build and Distribution
- `make bin/cardinalhq-otel-collector` - Build the collector binary
- `make buildfiles` - Generate distribution files using OpenTelemetry collector builder
- `make images` - Build multi-architecture Docker images using goreleaser

### Code Quality
- `make fmt` - Format code using gci (goimports + custom ordering)
- `make tidy` - Run go mod tidy across all modules
- `make license-check` - Verify license headers using license-eye
- `make clean` - Remove built binaries
- `make really-clean` - Remove binaries and distribution directory

### Testing and Benchmarking
- `make test` - Run tests in all modules: `cd <module> && go test ./...`
- `make bench` or `make benchmark` - Run benchmarks across all modules

## Module Architecture

The codebase is organized as a monorepo with multiple Go modules:

### Core Modules
- `internal/` - Shared internal packages including:
  - `boxer/` - Data buffering and storage abstractions
  - `ddpb/` - Datadog protobuf definitions
  - `spantagger/` - Span tagging utilities
  - `trigram/` - Text analysis utilities
  - `wtcache/` - Write-through caching implementation

### Component Modules
Each component type has its own directory with individual Go modules:

#### Custom CardinalHQ Components
- **Receivers**: `chqdatadogreceiver`, `githubeventsreceiver`
- **Exporters**: `chqs3exporter`, `chqdatadogexporter`, `chqservicegraphexporter`, `chqentitygraphexporter`, `chqk8sentitygraphexporter`
- **Processors**: `aggregationprocessor`, `pitbullprocessor`, `fingerprintprocessor`, `piiredactionprocessor`, `summarysplitprocessor`, `extractmetricsprocessor`, `chqexemplarprocessor`, `chqspannerprocessor`
- **Extensions**: `chqauthextension`, `chqconfigextension`, `chqtagcacheextension`, `chqsyntheticsextention`
- **Connectors**: `chqmissingdataconnector`, `chqk8smetricsconnector`

#### Key Features
- **S3 Export**: Efficient data export to S3-compatible storage with parquet encoding
- **Datadog Integration**: Enhanced Datadog receiver and exporter with tag caching
- **PII Redaction**: Automated sensitive data redaction (piiredactionprocessor)
- **Cost Management**: Data sampling and filtering for cost optimization (pitbullprocessor)
- **Graph Generation**: Service and entity relationship graphs from telemetry data

### Distribution
- `distribution/` - **GENERATED DIRECTORY** - Contains auto-generated main.go and components.go for the collector binary. Do not edit or examine files in this directory.
- Built using OpenTelemetry Collector Builder from `cardinalhq-otel-collector.yaml`

## Testing Individual Components

To test a specific component:
```bash
cd <component-directory>  # e.g., cd processor/pitbullprocessor
go test ./...
```

To run tests with verbose output:
```bash
cd <component-directory>
go test -v ./...
```

## Development Workflow

1. **Make changes** to component code
2. **Run tests**: `make test` (or test individual modules)
3. **Run linting**: `make lint`
4. **Format code**: `make fmt`
5. **Build**: `make all`
6. **Run full checks**: `make check`

## Configuration

- **Linting**: Uses .golangci.yaml with custom rules and exclusions
- **License**: All files require Apache 2.0 license headers (checked by license-eye)
- **Builder Config**: cardinalhq-otel-collector.yaml defines which components to include
- **Go Version**: Requires Go 1.24.1
- **OpenTelemetry Version**: Based on v0.132.0

## Key Dependencies

- OpenTelemetry Collector framework (v0.132.0)
- OpenTelemetry Collector Contrib components
- AWS SDK for S3 operations
- Datadog agent libraries
- BadgerDB for local storage
- Parquet for data serialization
