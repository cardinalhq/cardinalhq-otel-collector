module github.com/cardinalhq/cardinalhq-otel-collector/exporter/chqk8sentitygraphexporter

go 1.24

toolchain go1.24.1

require (
	github.com/cardinalhq/oteltools v0.30.19
	github.com/deckarep/golang-set/v2 v2.8.0
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/collector/component v1.34.0
	go.opentelemetry.io/collector/config/confighttp v0.128.0
	go.opentelemetry.io/collector/consumer v1.34.0
	go.opentelemetry.io/collector/exporter v0.128.0
	go.opentelemetry.io/collector/otelcol/otelcoltest v0.128.0
	go.opentelemetry.io/collector/pdata v1.34.0
	go.opentelemetry.io/otel/metric v1.36.0
	go.opentelemetry.io/otel/trace v1.36.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
	google.golang.org/protobuf v1.36.6
	k8s.io/api v0.32.3
	k8s.io/apimachinery v0.32.3
	k8s.io/utils v0.0.0-20250321185631-1f6e0b77f77e
)

require (
	github.com/DataDog/sketches-go v1.4.7 // indirect
	github.com/alecthomas/participle/v2 v2.1.4 // indirect
	github.com/apache/datasketches-go v0.0.0-20250526164709-4e9f74ff6296 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.2 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/db47h/ragel/v2 v2.2.4 // indirect
	github.com/ebitengine/purego v0.8.4 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/foxboron/go-tpm-keyfiles v0.0.0-20250323135004-b31fac66206e // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/fxamacker/cbor/v2 v2.8.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.3.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/go-tpm v0.9.5 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/providers/confmap v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20240909124753-873cd0166683 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.128.0 // indirect
	github.com/oschwald/maxminddb-golang/v2 v2.0.0-beta.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_golang v1.22.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.64.0 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/shirou/gopsutil/v4 v4.25.5 // indirect
	github.com/spf13/cobra v1.9.1 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/tklauser/go-sysconf v0.3.14 // indirect
	github.com/tklauser/numcpus v0.9.0 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector/client v1.34.0 // indirect
	go.opentelemetry.io/collector/component/componentstatus v0.128.0 // indirect
	go.opentelemetry.io/collector/component/componenttest v0.128.0 // indirect
	go.opentelemetry.io/collector/config/configauth v0.128.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.34.0 // indirect
	go.opentelemetry.io/collector/config/configmiddleware v0.128.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.34.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.34.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.128.0 // indirect
	go.opentelemetry.io/collector/config/configtls v1.34.0 // indirect
	go.opentelemetry.io/collector/confmap v1.34.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/envprovider v1.34.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/fileprovider v1.34.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/httpprovider v1.34.0 // indirect
	go.opentelemetry.io/collector/confmap/provider/yamlprovider v1.34.0 // indirect
	go.opentelemetry.io/collector/confmap/xconfmap v0.128.0 // indirect
	go.opentelemetry.io/collector/connector v0.128.0 // indirect
	go.opentelemetry.io/collector/connector/connectortest v0.128.0 // indirect
	go.opentelemetry.io/collector/connector/xconnector v0.128.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.128.0 // indirect
	go.opentelemetry.io/collector/consumer/consumertest v0.128.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.128.0 // indirect
	go.opentelemetry.io/collector/exporter/exportertest v0.128.0 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.128.0 // indirect
	go.opentelemetry.io/collector/extension v1.34.0 // indirect
	go.opentelemetry.io/collector/extension/extensionauth v1.34.0 // indirect
	go.opentelemetry.io/collector/extension/extensioncapabilities v0.128.0 // indirect
	go.opentelemetry.io/collector/extension/extensionmiddleware v0.128.0 // indirect
	go.opentelemetry.io/collector/extension/extensiontest v0.128.0 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.128.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.34.0 // indirect
	go.opentelemetry.io/collector/internal/fanoutconsumer v0.128.0 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.128.0 // indirect
	go.opentelemetry.io/collector/otelcol v0.128.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.128.0 // indirect
	go.opentelemetry.io/collector/pdata/testdata v0.128.0 // indirect
	go.opentelemetry.io/collector/pipeline v0.128.0 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.128.0 // indirect
	go.opentelemetry.io/collector/processor v1.34.0 // indirect
	go.opentelemetry.io/collector/processor/processortest v0.128.0 // indirect
	go.opentelemetry.io/collector/processor/xprocessor v0.128.0 // indirect
	go.opentelemetry.io/collector/receiver v1.34.0 // indirect
	go.opentelemetry.io/collector/receiver/receivertest v0.128.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.128.0 // indirect
	go.opentelemetry.io/collector/service v0.128.0 // indirect
	go.opentelemetry.io/collector/service/hostcapabilities v0.128.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.11.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.61.0 // indirect
	go.opentelemetry.io/contrib/otelconf v0.16.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.36.0 // indirect
	go.opentelemetry.io/otel v1.36.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.12.2 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.12.2 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.36.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.36.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.36.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.36.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.36.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.58.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.12.2 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.36.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.36.0 // indirect
	go.opentelemetry.io/otel/log v0.12.2 // indirect
	go.opentelemetry.io/otel/sdk v1.36.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.12.2 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.36.0 // indirect
	go.opentelemetry.io/proto/otlp v1.6.0 // indirect
	golang.org/x/crypto v0.38.0 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/net v0.40.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.25.0 // indirect
	gonum.org/v1/gonum v0.16.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250519155744-55703ea1f237 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250519155744-55703ea1f237 // indirect
	google.golang.org/grpc v1.72.2 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	sigs.k8s.io/json v0.0.0-20241014173422-cfa47c3a1cc8 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.6.0 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)

replace (
	github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension v0.0.0 => ../../extension/chqconfigextension
	github.com/cardinalhq/cardinalhq-otel-collector/internal v0.0.0 => ../../internal
)
