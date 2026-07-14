package r2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var (
	ErrObjectNotFound    = errors.New("remote object not found")
	ErrObjectExists      = errors.New("remote object already exists")
	ErrPublisherConflict = errors.New("publisher claim conflict")
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
