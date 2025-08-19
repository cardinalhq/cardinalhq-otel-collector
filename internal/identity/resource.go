// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"fmt"
	"hash"
	"hash/fnv"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type Resource struct {
	attrs [16]byte
}

func (r Resource) Hash() hash.Hash64 {
	sum := fnv.New64a()
	sum.Write(r.attrs[:])
	return sum
}

func (r Resource) String() string {
	return fmt.Sprintf("resource/%x", r.Hash().Sum64())
}

func OfResource(r pcommon.Resource) Resource {
	// Use the proper pdatautil.MapHash function for deterministic hashing
	attrs := pdatautil.MapHash(r.Attributes())
	return Resource{
		attrs: attrs,
	}
}
