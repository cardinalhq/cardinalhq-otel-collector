module github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqs3exporter

go 1.23

toolchain go1.23.3

require (
	github.com/DataDog/sketches-go v1.4.6
	github.com/aws/aws-sdk-go v1.55.5
	github.com/cardinalhq/cardinalhq-otel-collector/internal v0.0.0
	github.com/cardinalhq/oteltools v0.4.20
	github.com/hashicorp/go-multierror v1.1.1
	github.com/parquet-go/parquet-go v0.24.0
	github.com/rs/xid v1.6.0
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/collector/component v0.116.0
	go.opentelemetry.io/collector/component/componenttest v0.116.0
	go.opentelemetry.io/collector/consumer v1.22.0
	go.opentelemetry.io/collector/exporter v0.116.0
	go.opentelemetry.io/collector/exporter/exportertest v0.116.0
	go.opentelemetry.io/collector/otelcol/otelcoltest v0.116.0
	go.opentelemetry.io/collector/pdata v1.22.0
	go.opentelemetry.io/otel/metric v1.33.0
	go.opentelemetry.io/otel/trace v1.33.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
	golang.org/x/exp v0.0.0-20250103163809-dd03c70a0a45
)

require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/ebitengine/purego v0.8.1 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.2.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.25.1 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/knadh/koanf/providers/confmap v0.1.0 // indirect
	github.com/knadh/koanf/v2 v2.1.2 // indirect
	github.com/lufia/plan9stats v0.0.0-20240909124753-873cd0166683 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_golang v1.20.5 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.61.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/shirou/gopsutil/v4 v4.24.12 // indirect
	github.com/spf13/cobra v1.8.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tklauser/go-sysconf v0.3.14 // indirect
	github.com/tklauser/numcpus v0.9.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector/client v1.22.0 // indirect
	go.opentelemetry.io/collector/component/componentstatus v0.116.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.22.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.116.0 // indirect
	go.opentelemetry.io/collector/confmap v1.22.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/envprovider v1.22.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/fileprovider v1.22.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/httpprovider v1.22.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/yamlprovider v1.22.0 // indirect
	go.opentelemetry.io/collector/connector v0.116.0 // indirect
	go.opentelemetry.io/collector/connector/connectortest v0.116.0 // indirect
	go.opentelemetry.io/collector/connector/xconnector v0.116.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.116.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.116.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.116.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.116.0 // indirect
	go.opentelemetry.io/collector/extension v0.116.0 // indirect
	go.opentelemetry.io/collector/extension/experimental/storage v0.116.0 // indirect
	go.opentelemetry.io/collector/extension/extensioncapabilities v0.116.0 // indirect
	go.opentelemetry.io/collector/extension/extensiontest v0.116.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.22.0 // indirect
	go.opentelemetry.io/collector/internal/fanoutconsumer v0.116.0 // indirect
	go.opentelemetry.io/collector/otelcol v0.116.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.116.0 // indirect
	go.opentelemetry.io/collector/pdata/testdata v0.116.0 // indirect
	go.opentelemetry.io/collector/pipeline v0.116.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.116.0 // indirect
	go.opentelemetry.io/collector/processor v0.116.0 // indirect
	go.opentelemetry.io/collector/processor/processortest v0.116.0 // indirect
	go.opentelemetry.io/collector/processor/xprocessor v0.116.0 // indirect
	go.opentelemetry.io/collector/receiver v0.116.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.116.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.116.0 // indirect
	go.opentelemetry.io/collector/semconv v0.116.0 // indirect
	go.opentelemetry.io/collector/service v0.116.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.8.0 // indirect
	go.opentelemetry.io/contrib/config v0.13.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.33.0 // indirect
	go.opentelemetry.io/otel v1.33.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.33.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.33.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.55.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.33.0 // indirect
	go.opentelemetry.io/otel/log v0.9.0 // indirect
	go.opentelemetry.io/otel/sdk v1.33.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.33.0 // indirect
	go.opentelemetry.io/proto/otlp v1.4.0 // indirect
	golang.org/x/net v0.33.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	gonum.org/v1/gonum v0.15.1 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250102185135-69823020774d // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250102185135-69823020774d // indirect
	google.golang.org/grpc v1.69.2 // indirect
	google.golang.org/protobuf v1.36.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/cardinalhq/cardinalhq-otel-collector/internal v0.0.0 => ../../internal
	// see main yaml config for when to remove this
	go.opentelemetry.io/contrib/config v0.12.0 => go.opentelemetry.io/contrib/config v0.10.0
)
