module github.com/cardinalhq/cardinalhq-otel-collector/extension/chqconfigextension

go 1.24

toolchain go1.24.1

require (
	github.com/cardinalhq/oteltools v0.16.5
	github.com/stretchr/testify v1.10.0
	go.opentelemetry.io/collector/component v1.27.0
	go.opentelemetry.io/collector/config/confighttp v0.121.0
	go.opentelemetry.io/collector/extension v1.27.0
	go.opentelemetry.io/otel/metric v1.35.0
	go.opentelemetry.io/otel/trace v1.35.0
	go.uber.org/zap v1.27.0
)

require (
	github.com/alecthomas/participle/v2 v2.1.1 // indirect
	github.com/antchfx/xmlquery v1.4.4 // indirect
	github.com/antchfx/xpath v1.3.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/elastic/go-grok v0.3.1 // indirect
	github.com/elastic/lunes v0.1.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/magefile/mage v1.15.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.121.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.121.0 // indirect
	github.com/oschwald/geoip2-golang v1.11.0 // indirect
	github.com/oschwald/maxminddb-golang v1.13.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/ua-parser/uap-go v0.0.0-20250213224047-9c035f085b90 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector/client v1.27.0 // indirect
	go.opentelemetry.io/collector/config/configauth v0.121.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.27.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.27.0 // indirect
	go.opentelemetry.io/collector/config/configtls v1.27.0 // indirect
	go.opentelemetry.io/collector/extension/extensionauth v0.121.0 // indirect
	go.opentelemetry.io/collector/pdata v1.27.0 // indirect
	go.opentelemetry.io/collector/semconv v0.121.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0 // indirect
	go.opentelemetry.io/otel v1.35.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/exp v0.0.0-20250305212735-054e65f0b394 // indirect
	golang.org/x/net v0.37.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250303144028-a0af3efb3deb // indirect
	google.golang.org/grpc v1.71.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/cardinalhq/cardinalhq-otel-collector/internal v0.0.0 => ../../internal
