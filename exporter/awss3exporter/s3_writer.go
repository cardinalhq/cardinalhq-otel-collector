// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awss3exporter // import "github.com/cardinalhq/cardinalhq-otel-collector/exporter/awss3exporter"

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.uber.org/zap"

	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/awss3exporter/internal/notify"
	"github.com/cardinalhq/cardinalhq-otel-collector/exporter/awss3exporter/internal/upload"
)

// notifyAdapter bridges notify.Notifier (used by the exporter) to
// upload.EventNotifier (used by the upload manager). Keeping the bridge in
// this root-package file avoids pulling internal/notify into internal/upload
// or vice versa.
type notifyAdapter struct {
	n notify.Notifier
}

func (a *notifyAdapter) Enqueue(ctx context.Context, e upload.NotifyEvent) bool {
	return a.n.Enqueue(ctx, notify.Event{Bucket: e.Bucket, Key: e.Key, Size: e.Size})
}

func newUploadManager(
	ctx context.Context,
	conf *Config,
	logger *zap.Logger,
	metadata string,
	format string,
	isCompressed bool,
	notifier notify.Notifier,
) (upload.Manager, error) {
	configOpts := []func(*config.LoadOptions) error{}

	if region := conf.S3Uploader.Region; region != "" {
		configOpts = append(configOpts, config.WithRegion(region))
	}

	switch conf.S3Uploader.RetryMode {
	case "nop":
		configOpts = append(configOpts, config.WithRetryer(func() aws.Retryer {
			return aws.NopRetryer{}
		}))
	default:
		configOpts = append(configOpts, config.WithRetryMode(aws.RetryMode(conf.S3Uploader.RetryMode)))
	}

	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, err
	}

	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.EndpointOptions = s3.EndpointResolverOptions{
				DisableHTTPS: conf.S3Uploader.DisableSSL,
			}
			o.UsePathStyle = conf.S3Uploader.S3ForcePathStyle
			o.Retryer = retry.AddWithMaxAttempts(o.Retryer, conf.S3Uploader.RetryMaxAttempts)
			o.Retryer = retry.AddWithMaxBackoffDelay(o.Retryer, conf.S3Uploader.RetryMaxBackoff)

			if conf.S3Uploader.EnableGCSCompatibility {
				enableGCSCompatibility(o)
			}
		},
	}

	if conf.S3Uploader.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(conf.S3Uploader.Endpoint)
		})
	}

	if arn := conf.S3Uploader.RoleArn; arn != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.Credentials = stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), arn)
		})
	}

	if endpoint := conf.S3Uploader.Endpoint; endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	var managerOpts []upload.ManagerOpt
	if conf.S3Uploader.ACL != "" {
		managerOpts = append(managerOpts,
			upload.WithACL(s3types.ObjectCannedACL(conf.S3Uploader.ACL)))
	}

	if conf.S3Uploader.EnableGCSCompatibility {
		managerOpts = append(managerOpts,
			upload.WithGCSCompatibility(true))
	}

	if notifier != nil {
		managerOpts = append(managerOpts, upload.WithNotifier(&notifyAdapter{n: notifier}))
	}

	var uniqueKeyFunc func() string
	switch conf.S3Uploader.UniqueKeyFuncName {
	case "uuidv7":
		uniqueKeyFunc = upload.GenerateUUIDv7
	default:
		uniqueKeyFunc = nil
	}

	var s3PartitionTimeLocation *time.Location
	if conf.S3Uploader.S3PartitionTimezone != "" {
		s3PartitionTimeLocation, err = time.LoadLocation(conf.S3Uploader.S3PartitionTimezone)
		if err != nil {
			return nil, fmt.Errorf("invalid S3 partition timezone: %w", err)
		}
	} else {
		s3PartitionTimeLocation = time.Local
	}

	return upload.NewS3Manager(
		logger,
		conf.S3Uploader.S3Bucket,
		&upload.PartitionKeyBuilder{
			PartitionBasePrefix:   conf.S3Uploader.S3BasePrefix,
			PartitionPrefix:       conf.S3Uploader.S3Prefix,
			PartitionFormat:       conf.S3Uploader.S3PartitionFormat,
			PartitionTimeLocation: s3PartitionTimeLocation,
			FilePrefix:            conf.S3Uploader.FilePrefix,
			FileFormat:            format,
			Metadata:              metadata,
			Compression:           conf.S3Uploader.Compression,
			UniqueKeyFunc:         uniqueKeyFunc,
			IsCompressed:          isCompressed,
		},
		s3.NewFromConfig(cfg, s3Opts...),
		s3types.StorageClass(conf.S3Uploader.StorageClass),
		managerOpts...,
	), nil
}

func enableGCSCompatibility(o *s3.Options) {
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired

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

const acceptEncodingHeader = "Accept-Encoding"

type acceptEncodingKey struct{}

func getAcceptEncodingKey(ctx context.Context) (v string) {
	v, _ = middleware.GetStackValue(ctx, acceptEncodingKey{}).(string)
	return v
}

func setAcceptEncodingKey(ctx context.Context, value string) context.Context {
	return middleware.WithStackValue(ctx, acceptEncodingKey{}, value)
}

var dropAcceptEncodingHeader = middleware.FinalizeMiddlewareFunc("DropAcceptEncodingHeader",
	func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
		req, ok := in.Request.(*smithyhttp.Request)
		if !ok {
			return out, metadata, &v4.SigningError{Err: fmt.Errorf("unexpected request middleware type %T", in.Request)}
		}

		ae := req.Header.Get(acceptEncodingHeader)
		ctx = setAcceptEncodingKey(ctx, ae)
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

		ae := getAcceptEncodingKey(ctx)
		req.Header.Set(acceptEncodingHeader, ae)
		in.Request = req

		return next.HandleFinalize(ctx, in)
	},
)
