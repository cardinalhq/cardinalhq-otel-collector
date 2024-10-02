module github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor

go 1.22.3

toolchain go1.22.4

require (
	github.com/cardinalhq/cardinalhq-otel-collector/internal v0.0.0
	github.com/cespare/xxhash/v2 v2.3.0
	go.opentelemetry.io/collector/component v0.110.0
	go.opentelemetry.io/collector/consumer v0.110.0
	go.opentelemetry.io/collector/pdata v1.16.0
	go.opentelemetry.io/collector/processor v0.110.0
	go.opentelemetry.io/otel v1.30.0
	go.opentelemetry.io/otel/metric v1.30.0
	go.opentelemetry.io/otel/trace v1.30.0
	go.uber.org/zap v1.27.0
)

require (
	github.com/db47h/ragel/v2 v2.2.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.110.0 // indirect
	go.opentelemetry.io/collector/consumer/consumerprofiles v0.110.0 // indirect
	go.opentelemetry.io/collector/internal/globalsignal v0.110.0 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.110.0 // indirect
	go.opentelemetry.io/collector/pipeline v0.110.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/exp v0.0.0-20240909161429-701f63a606c0 // indirect
	golang.org/x/net v0.29.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
	golang.org/x/text v0.18.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240930140551-af27646dc61f // indirect
	google.golang.org/grpc v1.67.1 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/cardinalhq/cardinalhq-otel-collector/internal v0.0.0 => ../../internal
