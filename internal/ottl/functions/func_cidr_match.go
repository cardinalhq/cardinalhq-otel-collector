// Copyright 2024 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package functions

import (
	"context"
	"fmt"
	"net"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

// CidrMatchArguments Define the arguments struct for the cidrMatch function
type CidrMatchArguments[K any] struct {
	Subnet ottl.StringGetter[K]
	IP     ottl.StringGetter[K]
}

// NewCidrMatchFactory Factory function to create a new instance of cidrMatch
func NewCidrMatchFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("CidrMatch", &CidrMatchArguments[K]{}, createCidrMatchFunction[K])
}

// Function to create the expression function for cidrMatch
func createCidrMatchFunction[K any](_ ottl.FunctionContext, oArgs ottl.Arguments) (ottl.ExprFunc[K], error) {
	args, ok := oArgs.(*CidrMatchArguments[K])
	if !ok {
		return nil, fmt.Errorf("cidrMatch args must be of type *CidrMatchArguments[K]")
	}
	return cidrMatch(args.Subnet, args.IP), nil
}

func cidrMatch[K any](subnetGetter ottl.StringGetter[K], ipGetter ottl.StringGetter[K]) ottl.ExprFunc[K] {
	return func(ctx context.Context, tCtx K) (any, error) {
		subnet, err := subnetGetter.Get(ctx, tCtx)
		if err != nil {
			return false, fmt.Errorf("failed to get subnet: %v", err)
		}

		ip, err := ipGetter.Get(ctx, tCtx)
		if err != nil {
			return false, fmt.Errorf("failed to get IP: %v", err)
		}

		_, ipNet, err := net.ParseCIDR(subnet)
		if err != nil {
			return false, fmt.Errorf("invalid CIDR subnet: %v", err)
		}

		parsedIP := net.ParseIP(ip)
		if parsedIP == nil {
			return false, fmt.Errorf("invalid IP address: %s", ip)
		}

		if ipNet.Contains(parsedIP) {
			return true, nil
		}

		return false, nil
	}
}
