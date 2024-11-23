module github.com/cardinalhq/cardinalhq-otel-collector/receiver/chqdatadogreceiver

go 1.23

toolchain go1.23.3

require (
	github.com/DataDog/datadog-agent/pkg/proto v0.59.0
	github.com/barweiss/go-tuple v1.1.2
	github.com/stretchr/testify v1.9.0
	github.com/vmihailenco/msgpack/v4 v4.3.13
	go.opentelemetry.io/collector/component v0.114.0
	go.opentelemetry.io/collector/config/confighttp v0.114.0
	go.opentelemetry.io/collector/consumer v0.114.0
	go.opentelemetry.io/collector/pdata v1.20.0
	go.opentelemetry.io/collector/receiver v0.114.0
	go.opentelemetry.io/collector/semconv v0.114.0
	go.opentelemetry.io/otel/metric v1.32.0
	go.opentelemetry.io/otel/trace v1.32.0
	go.uber.org/goleak v1.3.0
	google.golang.org/protobuf v1.35.2
)

require (
	github.com/cardinalhq/cardinalhq-otel-collector/extension/chqtagcacheextension v0.0.0
	github.com/cardinalhq/cardinalhq-otel-collector/internal v0.0.0
	github.com/mitchellh/mapstructure v1.5.0
	go.opentelemetry.io/collector/client v1.20.0
	go.opentelemetry.io/collector/component/componentstatus v0.114.0
	go.opentelemetry.io/collector/component/componenttest v0.114.0
	go.opentelemetry.io/collector/consumer/consumertest v0.114.0
	go.opentelemetry.io/collector/receiver/receivertest v0.114.0
	golang.org/x/exp v0.0.0-20241108190413-2d47ceb2692f
)

require (
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.114.0 // indirect
	go.opentelemetry.io/collector/consumer/consumerprofiles v0.114.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.114.0 // indirect
	go.opentelemetry.io/collector/pipeline v0.114.0 // indirect
	go.opentelemetry.io/collector/receiver/receiverprofiles v0.114.0 // indirect
)

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/philhofer/fwd v1.1.3-0.20240916144458-20a13a1f6b7c // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/tinylib/msgp v1.2.4 // indirect
	github.com/vmihailenco/tagparser v0.1.2 // indirect
	go.opentelemetry.io/collector/config/configauth v0.114.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.20.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.20.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.114.0 // indirect
	go.opentelemetry.io/collector/config/configtls v1.20.0 // indirect
	go.opentelemetry.io/collector/config/internal v0.114.0 // indirect
	go.opentelemetry.io/collector/extension v0.114.0 // indirect
	go.opentelemetry.io/collector/extension/auth v0.114.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.57.0 // indirect
	go.opentelemetry.io/otel v1.32.0
	go.opentelemetry.io/otel/sdk v1.32.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.32.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0
	golang.org/x/net v0.31.0 // indirect
	golang.org/x/sys v0.27.0 // indirect
	golang.org/x/text v0.20.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241118233622-e639e219e697 // indirect
	google.golang.org/grpc v1.68.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/cardinalhq/cardinalhq-otel-collector/extension/chqtagcacheextension v0.0.0 => ../../extension/chqtagcacheextension
	github.com/cardinalhq/cardinalhq-otel-collector/internal v0.0.0 => ../../internal
)

retract (
	v0.76.2
	v0.76.1
)
