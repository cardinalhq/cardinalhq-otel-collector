package datadogreceiver

import (
	"encoding/json"

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

func (ddr *datadogReceiver) processIntake(data []byte) {
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

	tags := make([]chqtagcacheextension.Tag, 0, len(intake.HostTags))
	for k, v := range intake.HostTags {
		for _, tag := range v {
			tags = append(tags, chqtagcacheextension.Tag{
				Name:  k,
				Value: tag,
			})
		}
	}

	if len(tags) == 0 {
		return // no host tags to update
	}

	ddr.gpLogger.Info("Putting tags in cache", zap.String("key", intake.InternalHostname), zap.Any("tags", tags))

	if err := ddr.tagcacheExtension.PutCache(hostname, tags); err != nil {
		ddr.gpLogger.Error("Failed to put tags in cache", zap.Error(err))
	}
}
