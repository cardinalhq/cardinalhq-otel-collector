module github.com/cardinalhq/cardinalhq-otel-collector/processor/chqdecoratorprocessor

go 1.22.2

require (
	github.com/aws/aws-sdk-go-v2 v1.27.1
	github.com/aws/aws-sdk-go-v2/config v1.27.17
	github.com/aws/aws-sdk-go-v2/credentials v1.17.17
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.16.22
	github.com/aws/aws-sdk-go-v2/service/s3 v1.54.4
	github.com/aws/smithy-go v1.20.2
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/db47h/ragel/v2 v2.2.4
	github.com/hashicorp/go-multierror v1.1.1
	github.com/stretchr/testify v1.9.0
	go.opentelemetry.io/collector/component v0.101.0
	go.opentelemetry.io/collector/consumer v0.101.0
	go.opentelemetry.io/collector/pdata v1.8.0
	go.opentelemetry.io/collector/processor v0.101.0
	go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws v0.52.0
	go.opentelemetry.io/otel v1.27.0
	go.opentelemetry.io/otel/metric v1.27.0
	go.opentelemetry.io/otel/trace v1.27.0
	go.uber.org/zap v1.27.0
	golang.org/x/exp v0.0.0-20240531132922-fd00a4e0eefc
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.2 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.3.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.32.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.11.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.3.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.9.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.11.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.17.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.32.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.20.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.24.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.28.11 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.0.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/knadh/koanf/maps v0.1.1 // indirect
	github.com/knadh/koanf/providers/confmap v0.1.0 // indirect
	github.com/knadh/koanf/v2 v2.1.1 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/collector v0.101.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.101.0 // indirect
	go.opentelemetry.io/collector/confmap v0.101.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157 // indirect
	google.golang.org/grpc v1.64.0 // indirect
	google.golang.org/protobuf v1.34.1 // indirect
)
