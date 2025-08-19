module github.com/cardinalhq/cardinalhq-otel-collector/receiver/chqdatadogreceiver

go 1.24

toolchain go1.24.1

require (
	github.com/DataDog/datadog-agent/pkg/proto v0.64.2
	github.com/barweiss/go-tuple v1.1.2
	github.com/stretchr/testify v1.10.0
	github.com/vmihailenco/msgpack/v4 v4.3.13
	go.opentelemetry.io/collector/component v1.34.0
	go.opentelemetry.io/collector/config/confighttp v0.128.0
	go.opentelemetry.io/collector/consumer v1.34.0
	go.opentelemetry.io/collector/pdata v1.38.0
	go.opentelemetry.io/collector/receiver v1.34.0
	go.opentelemetry.io/collector/semconv v0.128.0
	go.opentelemetry.io/otel/metric v1.36.0
	go.opentelemetry.io/otel/trace v1.36.0
	go.uber.org/goleak v1.3.0
	google.golang.org/protobuf v1.36.7
)

require (
	github.com/cardinalhq/cardinalhq-otel-collector/extension/chqtagcacheextension v0.0.0
	github.com/cardinalhq/cardinalhq-otel-collector/internal v0.0.0
	github.com/mitchellh/mapstructure v1.5.0
	go.opentelemetry.io/collector/client v1.34.0
	go.opentelemetry.io/collector/component/componentstatus v0.128.0
	go.opentelemetry.io/collector/component/componenttest v0.128.0
	go.opentelemetry.io/collector/consumer/consumertest v0.128.0
	go.opentelemetry.io/collector/receiver/receiverhelper v0.128.0
	go.opentelemetry.io/collector/receiver/receivertest v0.128.0
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0
)

require (
	github.com/foxboron/go-tpm-keyfiles v0.0.0-20250323135004-b31fac66206e // indirect
	github.com/google/go-tpm v0.9.5 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector/config/configmiddleware v0.128.0 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.128.0 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.128.0 // indirect
	go.opentelemetry.io/collector/extension/extensionauth v1.34.0 // indirect
	go.opentelemetry.io/collector/extension/extensionmiddleware v0.128.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.34.0 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.128.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.128.0 // indirect
	go.opentelemetry.io/collector/pipeline v0.128.0 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.128.0 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.11.0 // indirect
	go.opentelemetry.io/otel/log v0.12.2 // indirect
	golang.org/x/crypto v0.38.0 // indirect
)

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/philhofer/fwd v1.1.3-0.20240916144458-20a13a1f6b7c // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/tinylib/msgp v1.2.5 // indirect
	github.com/vmihailenco/tagparser v0.1.2 // indirect
	go.opentelemetry.io/collector/config/configauth v0.128.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.34.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.34.0 // indirect
	go.opentelemetry.io/collector/config/configtls v1.34.0 // indirect
	go.opentelemetry.io/collector/extension v1.34.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.61.0 // indirect
	go.opentelemetry.io/otel v1.36.0
	go.opentelemetry.io/otel/sdk v1.36.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.36.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0
	golang.org/x/net v0.40.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.25.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250528174236-200df99c418a // indirect
	google.golang.org/grpc v1.74.2 // indirect
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
