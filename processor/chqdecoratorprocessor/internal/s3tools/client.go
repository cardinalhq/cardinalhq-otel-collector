// Copyright 2024 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3tools

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
)

type S3Client struct {
	S3Client *s3.Client
	endpoint *url.URL
}

const acceptEncodingHeader = "Accept-Encoding"

type acceptEncodingKey struct{}

type Config struct {
	AccessKey string
	SecretKey string
	Region    string
	Endpoint  string
	Provider  string
}

// Override the default endpoint resolver if needed
func (mgr *S3Client) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	return smithyendpoints.Endpoint{
		URI: *mgr.endpoint,
	}, nil
}

func makeS3Client(conf Config, cfg aws.Config, endpoint string) (*S3Client, error) {
	client := &S3Client{}

	if endpoint != "" {
		uri, err := url.Parse(endpoint)
		if err != nil {
			return nil, err
		}
		client.endpoint = uri
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if client.endpoint != nil {
			o.EndpointResolverV2 = client
			o.EndpointOptions.DisableHTTPS = client.endpoint.Scheme == "http"
		}
		if conf.Provider == "gcp" {
			SignForGCP(o)
		}
	})
	client.S3Client = s3Client

	return client, nil
}

func newChainProvider(providers ...aws.CredentialsProvider) aws.CredentialsProvider {
	return aws.NewCredentialsCache(
		aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			var errs []error
			for _, p := range providers {
				creds, err := p.Retrieve(ctx)
				if err == nil {
					return creds, nil
				}
				errs = append(errs, err)
			}
			return aws.Credentials{}, fmt.Errorf("no valid providers in chain: %s", errs)
		}),
	)
}

func makeAWSConfig(ctx context.Context, conf Config) (aws.Config, error) {
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		if tr.TLSClientConfig == nil {
			tr.TLSClientConfig = &tls.Config{}
		}
		tr.TLSClientConfig.MinVersion = tls.VersionTLS13
	})

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithHTTPClient(httpClient),
		awsconfig.WithClientLogMode(aws.LogDeprecatedUsage),
		awsconfig.WithRegion(conf.Region),
	)
	if err != nil {
		return aws.Config{}, fmt.Errorf("unable to load aws config: %w", err)
	}
	cfg.Credentials = newChainProvider(
		cfg.Credentials,
		credentials.NewStaticCredentialsProvider(conf.AccessKey, conf.SecretKey, ""),
	)

	otelaws.AppendMiddlewares(&cfg.APIOptions)

	return cfg, nil
}

func NewS3Client(ctx context.Context, conf Config) (*S3Client, error) {
	awsconfig, err := makeAWSConfig(ctx, conf)
	if err != nil {
		return nil, fmt.Errorf("unable to make aws config: %w", err)
	}
	s3client, err := makeS3Client(conf, awsconfig, conf.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to make s3 client: %w", err)
	}
	return s3client, nil
}

func GetAcceptEncodingKey(ctx context.Context) (v string) {
	v, _ = middleware.GetStackValue(ctx, acceptEncodingKey{}).(string)
	return v
}

func SetAcceptEncodingKey(ctx context.Context, value string) context.Context {
	return middleware.WithStackValue(ctx, acceptEncodingKey{}, value)
}

var dropAcceptEncodingHeader = middleware.FinalizeMiddlewareFunc("DropAcceptEncodingHeader",
	func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
		req, ok := in.Request.(*smithyhttp.Request)
		if !ok {
			return out, metadata, &v4.SigningError{Err: fmt.Errorf("unexpected request middleware type %T", in.Request)}
		}

		ae := req.Header.Get(acceptEncodingHeader)
		ctx = SetAcceptEncodingKey(ctx, ae)
		req.Header.Del(acceptEncodingHeader)
		in.Request = req

		return next.HandleFinalize(ctx, in)
	},
)

var replaceAcceptEncodingHeader = middleware.FinalizeMiddlewareFunc("ReplaceAcceptEncodingHeader",
	func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
		req, ok := in.Request.(*smithyhttp.Request)
		if !ok {
			return out, metadata, &v4.SigningError{Err: fmt.Errorf("unexpected request middleware type %T", in.Request)}
		}

		ae := GetAcceptEncodingKey(ctx)
		req.Header.Set(acceptEncodingHeader, ae)
		in.Request = req

		return next.HandleFinalize(ctx, in)
	},
)

func SignForGCP(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		if err := stack.Finalize.Insert(dropAcceptEncodingHeader, "Signing", middleware.Before); err != nil {
			return err
		}

		if err := stack.Finalize.Insert(replaceAcceptEncodingHeader, "Signing", middleware.After); err != nil {
			return err
		}

		return nil
	})
}
