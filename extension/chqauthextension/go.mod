module github.com/cardinalhq/cardinalhq-otel-collector/extension/chqauthextension

go 1.24

toolchain go1.24.1

require (
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/collector/client v1.27.0
	go.opentelemetry.io/collector/component v1.27.0
	go.opentelemetry.io/collector/config/confighttp v0.121.0
	go.opentelemetry.io/collector/extension v1.27.0
	go.opentelemetry.io/collector/extension/extensionauth v0.121.0
	go.opentelemetry.io/otel v1.35.0
	go.opentelemetry.io/otel/metric v1.35.0
	go.opentelemetry.io/otel/trace v1.35.0
	go.uber.org/zap v1.27.0
	google.golang.org/grpc v1.71.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/rs/cors v1.11.1 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector/config/configauth v0.121.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.27.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.27.0 // indirect
	go.opentelemetry.io/collector/config/configtls v1.27.0 // indirect
	go.opentelemetry.io/collector/pdata v1.27.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.37.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/protobuf v1.36.5 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/cardinalhq/cardinalhq-otel-collector/internal v0.0.0 => ../../internal
