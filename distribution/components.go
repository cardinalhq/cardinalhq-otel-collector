// Code generated by "go.opentelemetry.io/collector/cmd/builder". DO NOT EDIT.

package main

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	datadogconnector "github.com/open-telemetry/opentelemetry-collector-contrib/connector/datadogconnector"
	spanmetricsconnector "github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector"
	forwardconnector "go.opentelemetry.io/collector/connector/forwardconnector"
	datadogexporter "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter"
	loadbalancingexporter "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"
	splunkhecexporter "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunkhecexporter"
	debugexporter "go.opentelemetry.io/collector/exporter/debugexporter"
	nopexporter "go.opentelemetry.io/collector/exporter/nopexporter"
	otlpexporter "go.opentelemetry.io/collector/exporter/otlpexporter"
	otlphttpexporter "go.opentelemetry.io/collector/exporter/otlphttpexporter"
	chqs3exporter "github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter"
	chqdatadogexporter "github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqdatadogexporter"
	healthcheckextension "github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension"
	pprofextension "github.com/open-telemetry/opentelemetry-collector-contrib/extension/pprofextension"
	filestorage "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	headerssetterextension "github.com/open-telemetry/opentelemetry-collector-contrib/extension/headerssetterextension"
	zpagesextension "go.opentelemetry.io/collector/extension/zpagesextension"
	chqauthextension "github.com/cardinalhq/cardinalhq-otel-collector/extension/chqauthextension"
	chqconfigextension "github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension"
	chqtagcacheextension "github.com/cardinalhq/cardinalhq-otel-collector/extension/chqtagcacheextension"
	attributesprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor"
	cumulativetodeltaprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor"
	deltatocumulativeprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatocumulativeprocessor"
	filterprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor"
	groupbytraceprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbytraceprocessor"
	probabilisticsamplerprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/probabilisticsamplerprocessor"
	redactionprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/redactionprocessor"
	resourcedetectionprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourcedetectionprocessor"
	resourceprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor"
	spanprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanprocessor"
	tailsamplingprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"
	transformprocessor "github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	batchprocessor "go.opentelemetry.io/collector/processor/batchprocessor"
	memorylimiterprocessor "go.opentelemetry.io/collector/processor/memorylimiterprocessor"
	aggregationprocessor "github.com/cardinalhq/cardinalhq-otel-collector/processor/aggregationprocessor"
	chqstatsprocessor "github.com/cardinalhq/cardinalhq-otel-collector/processor/chqstatsprocessor"
	pitbullprocessor "github.com/cardinalhq/cardinalhq-otel-collector/processor/pitbullprocessor"
	fingerprintprocessor "github.com/cardinalhq/cardinalhq-otel-collector/processor/fingerprintprocessor"
	summarysplitprocessor "github.com/cardinalhq/cardinalhq-otel-collector/processor/summarysplitprocessor"
	extractmetricsprocessor "github.com/cardinalhq/cardinalhq-otel-collector/processor/extractmetricsprocessor"
	awsfirehosereceiver "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver"
	fluentforwardreceiver "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fluentforwardreceiver"
	githubreceiver "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/githubreceiver"
	googlecloudmonitoringreceiver "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudmonitoringreceiver"
	hostmetricsreceiver "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver"
	influxdbreceiver "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver"
	kafkametricsreceiver "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkametricsreceiver"
	prometheusreceiver "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
	splunkhecreceiver "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunkhecreceiver"
	nopreceiver "go.opentelemetry.io/collector/receiver/nopreceiver"
	otlpreceiver "go.opentelemetry.io/collector/receiver/otlpreceiver"
	routereceiver "github.com/observiq/bindplane-agent/receiver/routereceiver"
	chqdatadogreceiver "github.com/cardinalhq/cardinalhq-otel-collector/receiver/chqdatadogreceiver"
	githubeventsreceiver "github.com/cardinalhq/cardinalhq-otel-collector/receiver/githubeventsreceiver"
)

func components() (otelcol.Factories, error) {
	var err error
	factories := otelcol.Factories{}

	factories.Extensions, err = extension.MakeFactoryMap(
		healthcheckextension.NewFactory(),
		pprofextension.NewFactory(),
		filestorage.NewFactory(),
		headerssetterextension.NewFactory(),
		zpagesextension.NewFactory(),
		chqauthextension.NewFactory(),
		chqconfigextension.NewFactory(),
		chqtagcacheextension.NewFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}
	factories.ExtensionModules = make(map[component.Type]string, len(factories.Extensions))
	factories.ExtensionModules[healthcheckextension.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension v0.110.0"
	factories.ExtensionModules[pprofextension.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/extension/pprofextension v0.110.0"
	factories.ExtensionModules[filestorage.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage v0.110.0"
	factories.ExtensionModules[headerssetterextension.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/extension/headerssetterextension v0.110.0"
	factories.ExtensionModules[zpagesextension.NewFactory().Type()] = "go.opentelemetry.io/collector/extension/zpagesextension v0.111.0"
	factories.ExtensionModules[chqauthextension.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/extension/chqauthextension v0.110.0"
	factories.ExtensionModules[chqconfigextension.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension v0.110.0"
	factories.ExtensionModules[chqtagcacheextension.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/extension/chqtagcacheextension v0.110.0"

	factories.Receivers, err = receiver.MakeFactoryMap(
		awsfirehosereceiver.NewFactory(),
		fluentforwardreceiver.NewFactory(),
		githubreceiver.NewFactory(),
		googlecloudmonitoringreceiver.NewFactory(),
		hostmetricsreceiver.NewFactory(),
		influxdbreceiver.NewFactory(),
		kafkametricsreceiver.NewFactory(),
		prometheusreceiver.NewFactory(),
		splunkhecreceiver.NewFactory(),
		nopreceiver.NewFactory(),
		otlpreceiver.NewFactory(),
		routereceiver.NewFactory(),
		chqdatadogreceiver.NewFactory(),
		githubeventsreceiver.NewFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}
	factories.ReceiverModules = make(map[component.Type]string, len(factories.Receivers))
	factories.ReceiverModules[awsfirehosereceiver.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver v0.110.0"
	factories.ReceiverModules[fluentforwardreceiver.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fluentforwardreceiver v0.110.0"
	factories.ReceiverModules[githubreceiver.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/githubreceiver v0.110.0"
	factories.ReceiverModules[googlecloudmonitoringreceiver.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudmonitoringreceiver v0.110.0"
	factories.ReceiverModules[hostmetricsreceiver.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver v0.110.0"
	factories.ReceiverModules[influxdbreceiver.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver v0.110.0"
	factories.ReceiverModules[kafkametricsreceiver.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkametricsreceiver v0.110.0"
	factories.ReceiverModules[prometheusreceiver.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver v0.110.0"
	factories.ReceiverModules[splunkhecreceiver.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/splunkhecreceiver v0.110.0"
	factories.ReceiverModules[nopreceiver.NewFactory().Type()] = "go.opentelemetry.io/collector/receiver/nopreceiver v0.111.0"
	factories.ReceiverModules[otlpreceiver.NewFactory().Type()] = "go.opentelemetry.io/collector/receiver/otlpreceiver v0.111.0"
	factories.ReceiverModules[routereceiver.NewFactory().Type()] = "github.com/observiq/bindplane-agent/receiver/routereceiver v1.62.0"
	factories.ReceiverModules[chqdatadogreceiver.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/receiver/chqdatadogreceiver v0.110.0"
	factories.ReceiverModules[githubeventsreceiver.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/receiver/githubeventsreceiver v0.110.0"

	factories.Exporters, err = exporter.MakeFactoryMap(
		datadogexporter.NewFactory(),
		loadbalancingexporter.NewFactory(),
		splunkhecexporter.NewFactory(),
		debugexporter.NewFactory(),
		nopexporter.NewFactory(),
		otlpexporter.NewFactory(),
		otlphttpexporter.NewFactory(),
		chqs3exporter.NewFactory(),
		chqdatadogexporter.NewFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}
	factories.ExporterModules = make(map[component.Type]string, len(factories.Exporters))
	factories.ExporterModules[datadogexporter.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter v0.110.0"
	factories.ExporterModules[loadbalancingexporter.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter v0.110.0"
	factories.ExporterModules[splunkhecexporter.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/splunkhecexporter v0.110.0"
	factories.ExporterModules[debugexporter.NewFactory().Type()] = "go.opentelemetry.io/collector/exporter/debugexporter v0.111.0"
	factories.ExporterModules[nopexporter.NewFactory().Type()] = "go.opentelemetry.io/collector/exporter/nopexporter v0.111.0"
	factories.ExporterModules[otlpexporter.NewFactory().Type()] = "go.opentelemetry.io/collector/exporter/otlpexporter v0.111.0"
	factories.ExporterModules[otlphttpexporter.NewFactory().Type()] = "go.opentelemetry.io/collector/exporter/otlphttpexporter v0.111.0"
	factories.ExporterModules[chqs3exporter.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter v0.110.0"
	factories.ExporterModules[chqdatadogexporter.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqdatadogexporter v0.110.0"

	factories.Processors, err = processor.MakeFactoryMap(
		attributesprocessor.NewFactory(),
		cumulativetodeltaprocessor.NewFactory(),
		deltatocumulativeprocessor.NewFactory(),
		filterprocessor.NewFactory(),
		groupbytraceprocessor.NewFactory(),
		probabilisticsamplerprocessor.NewFactory(),
		redactionprocessor.NewFactory(),
		resourcedetectionprocessor.NewFactory(),
		resourceprocessor.NewFactory(),
		spanprocessor.NewFactory(),
		tailsamplingprocessor.NewFactory(),
		transformprocessor.NewFactory(),
		batchprocessor.NewFactory(),
		memorylimiterprocessor.NewFactory(),
		aggregationprocessor.NewFactory(),
		chqstatsprocessor.NewFactory(),
		pitbullprocessor.NewFactory(),
		fingerprintprocessor.NewFactory(),
		summarysplitprocessor.NewFactory(),
		extractmetricsprocessor.NewFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}
	factories.ProcessorModules = make(map[component.Type]string, len(factories.Processors))
	factories.ProcessorModules[attributesprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor v0.110.0"
	factories.ProcessorModules[cumulativetodeltaprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor v0.110.0"
	factories.ProcessorModules[deltatocumulativeprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/deltatocumulativeprocessor v0.110.0"
	factories.ProcessorModules[filterprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/filterprocessor v0.110.0"
	factories.ProcessorModules[groupbytraceprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/groupbytraceprocessor v0.110.0"
	factories.ProcessorModules[probabilisticsamplerprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/probabilisticsamplerprocessor v0.110.0"
	factories.ProcessorModules[redactionprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/redactionprocessor v0.110.0"
	factories.ProcessorModules[resourcedetectionprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourcedetectionprocessor v0.110.0"
	factories.ProcessorModules[resourceprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor v0.110.0"
	factories.ProcessorModules[spanprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanprocessor v0.110.0"
	factories.ProcessorModules[tailsamplingprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor v0.110.0"
	factories.ProcessorModules[transformprocessor.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor v0.110.0"
	factories.ProcessorModules[batchprocessor.NewFactory().Type()] = "go.opentelemetry.io/collector/processor/batchprocessor v0.111.0"
	factories.ProcessorModules[memorylimiterprocessor.NewFactory().Type()] = "go.opentelemetry.io/collector/processor/memorylimiterprocessor v0.111.0"
	factories.ProcessorModules[aggregationprocessor.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/processor/aggregationprocessor v0.110.0"
	factories.ProcessorModules[chqstatsprocessor.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/processor/chqstatsprocessor v0.110.0"
	factories.ProcessorModules[pitbullprocessor.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/processor/pitbullprocessor v0.110.0"
	factories.ProcessorModules[fingerprintprocessor.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/processor/fingerprintprocessor v0.110.0"
	factories.ProcessorModules[summarysplitprocessor.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/processor/summarysplitprocessor v0.110.0"
	factories.ProcessorModules[extractmetricsprocessor.NewFactory().Type()] = "github.com/cardinalhq/cardinalhq-otel-collector/processor/extractmetricsprocessor v0.110.0"

	factories.Connectors, err = connector.MakeFactoryMap(
		datadogconnector.NewFactory(),
		spanmetricsconnector.NewFactory(),
		forwardconnector.NewFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}
	factories.ConnectorModules = make(map[component.Type]string, len(factories.Connectors))
	factories.ConnectorModules[datadogconnector.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/connector/datadogconnector v0.110.0"
	factories.ConnectorModules[spanmetricsconnector.NewFactory().Type()] = "github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector v0.110.0"
	factories.ConnectorModules[forwardconnector.NewFactory().Type()] = "go.opentelemetry.io/collector/connector/forwardconnector v0.111.0"

	return factories, nil
}