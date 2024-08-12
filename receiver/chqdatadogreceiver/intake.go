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

package datadogreceiver

import (
	"encoding/json"
	"strings"

	"github.com/cardinalhq/cardinalhq-otel-collector/extension/chqtagcacheextension"
	"go.uber.org/zap"
)

type datadogIntake struct {
	APIKey           string              `json:"apiKey"`
	InternalHostname string              `json:"internalHostname"`
	Meta             DatadogIntakeMeta   `json:"meta"`
	HostTags         map[string][]string `json:"host-tags"`
}

type DatadogIntakeMeta struct {
	Hostname string `json:"hostname"`
}

func (ddr *datadogReceiver) processIntake(apikey string, data []byte) {
	if apikey == "" {
		return // no api key, nothing to do
	}

	var intake datadogIntake
	err := json.Unmarshal(data, &intake)
	if err != nil {
		ddr.gpLogger.Error("Failed to unmarshal intake data", zap.Error(err))
		return
	}

	hostname := intake.InternalHostname
	if hostname == "" {
		hostname = intake.Meta.Hostname
	}
	if hostname == "" {
		return // probably not something we want
	}

	key := apikey + "/" + hostname

	tags := make([]chqtagcacheextension.Tag, 0, len(intake.HostTags))
	for _, v := range intake.HostTags {
		for _, tag := range v {
			items := strings.SplitN(tag, ":", 2)
			if len(items) != 2 {
				continue
			}
			tags = append(tags, chqtagcacheextension.Tag{
				Name:  items[0],
				Value: items[1],
			})
		}
	}

	if len(tags) == 0 {
		return // no host tags to update
	}

	if err := ddr.tagcacheExtension.PutCache(key, tags); err != nil {
		ddr.gpLogger.Error("Failed to put tags in cache", zap.Error(err))
	}
}
