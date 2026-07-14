package r2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var (
	ErrObjectNotFound    = errors.New("remote object not found")
	ErrObjectExists      = errors.New("remote object already exists")
	ErrPublisherConflict = errors.New("publisher claim conflict")
	ErrMetadataTooLarge  = errors.New("remote metadata exceeds configured limit")
)

type RemoteObject struct {
	Key  string
	Size int64
}

type ObjectBackend interface {
	PutIfAbsent(ctx context.Context, key string, body []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, prefix string) ([]RemoteObject, error)
}

// ReadBackend is deliberately separate from ObjectBackend so an ArchiveReader
// cannot depend on a remote write method at compile time.
type ReadBackend interface {
	List(ctx context.Context, prefix string) ([]RemoteObject, error)
	Get(ctx context.Context, key string) ([]byte, error)
	Open(ctx context.Context, key string) (io.ReadCloser, int64, error)
}

type S3ReadBackendConfig struct {
	Bucket           string
	Endpoint         string
	Region           string
	AccessKeyEnv     string
	SecretKeyEnv     string
	MaxMetadataBytes int64
}

type S3ReadBackend struct {
	client           *s3.Client
	bucket           string
	maxMetadataBytes int64
}

func NewS3ReadBackend(ctx context.Context, settings S3ReadBackendConfig) (*S3ReadBackend, error) {
	if settings.Bucket == "" || settings.Endpoint == "" || settings.AccessKeyEnv == "" || settings.SecretKeyEnv == "" {
		return nil, fmt.Errorf("read-only S3 configuration is incomplete")
	}
	accessKey, accessOK := os.LookupEnv(settings.AccessKeyEnv)
	secretKey, secretOK := os.LookupEnv(settings.SecretKeyEnv)
	if !accessOK || !secretOK || accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("read-only S3 credentials are unavailable")
	}
	if settings.Region == "" {
		settings.Region = "auto"
	}
	if settings.MaxMetadataBytes <= 0 {
		settings.MaxMetadataBytes = 1 << 20
	}
	awsConfig, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(settings.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load read-only S3 configuration")
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.UsePathStyle = true
		options.BaseEndpoint = aws.String(settings.Endpoint)
	})
	return &S3ReadBackend{client: client, bucket: settings.Bucket, maxMetadataBytes: settings.MaxMetadataBytes}, nil
}

func (b *S3ReadBackend) Get(ctx context.Context, key string) ([]byte, error) {
	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, classifyRemoteError(err)
	}
	defer output.Body.Close()
	if output.ContentLength != nil && *output.ContentLength > b.maxMetadataBytes {
		return nil, ErrMetadataTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(output.Body, b.maxMetadataBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read remote metadata")
	}
	if int64(len(body)) > b.maxMetadataBytes {
		return nil, ErrMetadataTooLarge
	}
	return body, nil
}

func (b *S3ReadBackend) Open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, 0, classifyRemoteError(err)
	}
	size := int64(-1)
	if output.ContentLength != nil {
		size = *output.ContentLength
	}
	return output.Body, size, nil
}

func (b *S3ReadBackend) List(ctx context.Context, prefix string) ([]RemoteObject, error) {
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{Bucket: aws.String(b.bucket), Prefix: aws.String(prefix)})
	var result []RemoteObject
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, classifyRemoteError(err)
		}
		for _, object := range page.Contents {
			if object.Key == nil {
				continue
			}
			size := int64(-1)
			if object.Size != nil {
				size = *object.Size
			}
			result = append(result, RemoteObject{Key: *object.Key, Size: size})
		}
	}
	return result, nil
}

type S3BackendConfig struct {
	Bucket   string
	Endpoint string
	Region   string
}

type S3Backend struct {
	client *s3.Client
	bucket string
}

func NewS3Backend(ctx context.Context, settings S3BackendConfig) (*S3Backend, error) {
	if settings.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}
	if settings.Region == "" {
		settings.Region = "auto"
	}
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(settings.Region))
	if err != nil {
		return nil, fmt.Errorf("load S3 configuration")
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.UsePathStyle = true
		if settings.Endpoint != "" {
			options.BaseEndpoint = aws.String(settings.Endpoint)
		}
	})
	return &S3Backend{client: client, bucket: settings.Bucket}, nil
}

func (b *S3Backend) PutIfAbsent(ctx context.Context, key string, body []byte) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		IfNoneMatch: aws.String("*"),
	})
	if err == nil {
		return nil
	}
	return classifyRemoteError(err)
}

func (b *S3Backend) Get(ctx context.Context, key string) ([]byte, error) {
	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, classifyRemoteError(err)
	}
	defer output.Body.Close()
	body, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("read remote object")
	}
	return body, nil
}

func (b *S3Backend) List(ctx context.Context, prefix string) ([]RemoteObject, error) {
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(prefix),
	})
	var result []RemoteObject
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, classifyRemoteError(err)
		}
		for _, object := range page.Contents {
			if object.Key != nil {
				size := int64(-1)
				if object.Size != nil {
					size = *object.Size
				}
				result = append(result, RemoteObject{Key: *object.Key, Size: size})
			}
		}
	}
	return result, nil
}

func classifyRemoteError(err error) error {
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return ErrObjectNotFound
		case "PreconditionFailed", "ConditionalRequestConflict", "Conflict":
			return ErrObjectExists
		default:
			return fmt.Errorf("remote object operation failed with code %s", apiError.ErrorCode())
		}
	}
	var statusError interface{ HTTPStatusCode() int }
	if errors.As(err, &statusError) {
		switch statusError.HTTPStatusCode() {
		case 404:
			return ErrObjectNotFound
		case 409, 412:
			return ErrObjectExists
		}
	}
	return fmt.Errorf("remote object operation failed")
}
