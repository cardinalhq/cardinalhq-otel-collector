// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// IGNORE go:generate mdatagen metadata.yaml

//go:generate protoc --go_out=. --go_opt=paths=source_relative internal/ddpb/metrics.proto

// Package datadogreceiver ingests traces in the Datadog APM format and translates them OpenTelemetry for collector usage
package datadogreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/datadogreceiver"
