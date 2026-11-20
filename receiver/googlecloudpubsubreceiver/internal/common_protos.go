// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/cardinalhq/cardinalhq-otel-collector/receiver/googlecloudpubsubreceiver/internal"

import (
	_ "google.golang.org/genproto/googleapis/cloud/audit" // support decoding Cloud Audit logs
)
