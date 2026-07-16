package r2

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	appcredentials "tick-data-platform/internal/credentials"
)

var (
	ErrObjectNotFound      = errors.New("remote object not found")
	ErrObjectExists        = errors.New("remote object already exists")
	ErrPublisherConflict   = errors.New("publisher claim conflict")
	ErrMetadataTooLarge    = errors.New("remote metadata exceeds configured limit")
	ErrResourceLimit       = errors.New("remote resource exceeds configured limit")
	ErrRemotePermission    = errors.New("remote permission denied")
	ErrLocalObjectChanged  = errors.New("local object changed before publication")
	ErrImmutableCollision  = errors.New("remote immutable object content differs")
	ErrRemoteCheckMismatch = errors.New("remote object content differs")
)

const remoteOutcomeProbeTimeout = 30 * time.Second

type RemoteObject struct {
	Key  string
	Size int64
}

type RemoteObjectCommit struct {
	ETag string
}

type RemoteObjectVerification struct {
	ETag string
}

type ObjectBackend interface {
	PutIfAbsent(ctx context.Context, key string, body []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Open(ctx context.Context, key string) (io.ReadCloser, int64, error)
	List(ctx context.Context, prefix string) ([]RemoteObject, error)
}

// WriteBackend is the production R2/S3 API boundary for immutable publication.
// It deliberately exposes object writes as conditional, verified file transfers
// instead of a generic remote command surface.
type WriteBackend interface {
	ObjectBackend
	PutFileIfAbsent(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectCommit, error)
	VerifyFile(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectVerification, error)
}

// BoundedObjectBackend is the stronger read boundary required by replay
// publication. The raw M2 publisher keeps using ObjectBackend; replay
// publication must never use its unbounded Get or List methods for remote
// acceptance decisions.
type BoundedObjectBackend interface {
	ObjectBackend
	GetLimited(ctx context.Context, key string, maxBytes uint64) ([]byte, error)
	ListLimited(ctx context.Context, prefix string, maxObjects uint64) ([]RemoteObject, error)
}

// ReplayRemoteObjectList is a bounded inventory result. Complete is part of
// the trust boundary: a missing key is Absent only when the backend proves the
// whole requested prefix was returned.
type ReplayRemoteObjectList struct {
	Objects  []RemoteObject
	Complete bool
}

// ReplayRemoteReadBackend is the only remote object capability available to
// the replay observer. It deliberately neither embeds ObjectBackend nor
// exposes Put, unbounded Get/List, or an unbounded Open.
type ReplayRemoteReadBackend interface {
	ListLimited(ctx context.Context, prefix string, maxObjects uint64) (ReplayRemoteObjectList, error)
	OpenLimited(ctx context.Context, key string, maxBytes uint64) (io.ReadCloser, int64, error)
}

// ReplayRemoteReadAdapter narrows the legacy bounded backend to the R3
// observer interface without changing the M2 backend contract.
type ReplayRemoteReadAdapter struct {
	backend BoundedObjectBackend
}

func NewReplayRemoteReadAdapter(backend BoundedObjectBackend) (*ReplayRemoteReadAdapter, error) {
	if backend == nil {
		return nil, fmt.Errorf("replay remote read backend is nil")
	}
	return &ReplayRemoteReadAdapter{backend: backend}, nil
}

func (a *ReplayRemoteReadAdapter) ListLimited(ctx context.Context, prefix string, maxObjects uint64) (ReplayRemoteObjectList, error) {
	objects, err := a.backend.ListLimited(ctx, prefix, maxObjects)
	if err != nil {
		return ReplayRemoteObjectList{}, err
	}
	return ReplayRemoteObjectList{Objects: objects, Complete: true}, nil
}

func (a *ReplayRemoteReadAdapter) OpenLimited(ctx context.Context, key string, maxBytes uint64) (io.ReadCloser, int64, error) {
	if maxBytes == 0 || maxBytes >= uint64(^uint64(0)>>1) {
		return nil, 0, fmt.Errorf("%w: invalid object byte limit", ErrResourceLimit)
	}
	body, size, err := a.backend.Open(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return &boundedReplayReadCloser{Reader: io.LimitReader(body, int64(maxBytes)+1), Closer: body}, size, nil
}

// ReadBackendReplayAdapter narrows the read-only delivery capability to the
// bounded observer capability used by retention and handover. It never adds a
// write method or an unbounded inventory operation.
type ReadBackendReplayAdapter struct {
	backend ReadBackend
}

func NewReplayRemoteReadAdapterFromReadBackend(backend ReadBackend) (*ReadBackendReplayAdapter, error) {
	if backend == nil {
		return nil, fmt.Errorf("read-only backend is nil")
	}
	return &ReadBackendReplayAdapter{backend: backend}, nil
}

func (a *ReadBackendReplayAdapter) ListLimited(ctx context.Context, prefix string, maxObjects uint64) (ReplayRemoteObjectList, error) {
	objects, err := a.backend.ListLimited(ctx, prefix, maxObjects)
	if err != nil {
		return ReplayRemoteObjectList{}, err
	}
	return ReplayRemoteObjectList{Objects: objects, Complete: true}, nil
}

func (a *ReadBackendReplayAdapter) OpenLimited(ctx context.Context, key string, maxBytes uint64) (io.ReadCloser, int64, error) {
	if maxBytes == 0 || maxBytes >= uint64(^uint64(0)>>1) {
		return nil, 0, fmt.Errorf("%w: invalid object byte limit", ErrResourceLimit)
	}
	body, size, err := a.backend.Open(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	if body == nil {
		return nil, 0, fmt.Errorf("remote object body is nil")
	}
	return &boundedReplayReadCloser{Reader: io.LimitReader(body, int64(maxBytes)+1), Closer: body}, size, nil
}

type boundedReplayReadCloser struct {
	io.Reader
	io.Closer
}

// ReadBackend is deliberately separate from ObjectBackend so an ArchiveReader
// cannot depend on a remote write method at compile time. Listing is bounded
// at the capability boundary; callers must not receive an unbounded remote
// inventory before applying their own response limits.
type ReadBackend interface {
	ListLimited(ctx context.Context, prefix string, maxObjects uint64) ([]RemoteObject, error)
	GetLimited(ctx context.Context, key string, maxBytes uint64) ([]byte, error)
	Open(ctx context.Context, key string) (io.ReadCloser, int64, error)
}

type S3ReadBackendConfig struct {
	Bucket           string
	Endpoint         string
	Region           string
	MaxMetadataBytes int64
}

type S3ReadBackend struct {
	client           *s3.Client
	bucket           string
	maxMetadataBytes int64
}

// NewS3ReadBackendWithProvider constructs a read-only backend after loading
// credentials exactly once from the supplied provider. The backend never
// consults process environment variables or the AWS default credential chain.
func NewS3ReadBackendWithProvider(ctx context.Context, settings S3ReadBackendConfig, provider appcredentials.Provider) (*S3ReadBackend, error) {
	if provider == nil {
		return nil, fmt.Errorf("read-only credential provider is required")
	}
	loaded, err := provider.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load read-only R2 credentials: %w", err)
	}
	return NewS3ReadBackendWithCredentials(ctx, settings, loaded)
}

// NewS3ReadBackendWithCredentials constructs a read-only backend from
// credentials that have already crossed the provider boundary. Keeping this
// constructor separate makes it possible to test the AWS SDK boundary without
// teaching it how credentials are stored.
func NewS3ReadBackendWithCredentials(ctx context.Context, settings S3ReadBackendConfig, loaded appcredentials.Credentials) (*S3ReadBackend, error) {
	if settings.Bucket == "" || settings.Endpoint == "" || loaded.AccessKeyID == "" || loaded.SecretAccessKey == "" {
		return nil, fmt.Errorf("read-only S3 configuration is incomplete")
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
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(loaded.AccessKeyID, loaded.SecretAccessKey, "")),
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

func (b *S3ReadBackend) GetLimited(ctx context.Context, key string, maxBytes uint64) ([]byte, error) {
	if maxBytes == 0 || maxBytes > uint64(^uint64(0)>>1) {
		return nil, fmt.Errorf("%w: invalid metadata byte limit", ErrResourceLimit)
	}
	limit := int64(maxBytes)
	if limit > b.maxMetadataBytes {
		limit = b.maxMetadataBytes
	}
	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, classifyRemoteError(err)
	}
	defer output.Body.Close()
	if output.ContentLength != nil && *output.ContentLength > limit {
		return nil, ErrMetadataTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(output.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read remote metadata: %w", err)
	}
	if int64(len(body)) > limit {
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

func (b *S3ReadBackend) ListLimited(ctx context.Context, prefix string, maxObjects uint64) ([]RemoteObject, error) {
	if maxObjects == 0 || maxObjects > uint64(^uint64(0)>>1) {
		return nil, fmt.Errorf("%w: invalid object count limit", ErrResourceLimit)
	}
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{Bucket: aws.String(b.bucket), Prefix: aws.String(prefix)})
	result := make([]RemoteObject, 0, minInt64(maxObjects, 256))
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, classifyRemoteError(err)
		}
		for _, object := range page.Contents {
			if object.Key == nil {
				continue
			}
			if uint64(len(result)) >= maxObjects {
				return nil, fmt.Errorf("%w: read inventory exceeds configured limit", ErrResourceLimit)
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
	return newS3Backend(ctx, settings, "", "")
}

// NewS3BackendWithEnv constructs the write-capable backend with credentials
// selected explicitly by environment-variable name. It is used by isolated
// handover verification so old and new writer credentials never share ambient
// process credential state.
func NewS3BackendWithEnv(ctx context.Context, settings S3BackendConfig, accessKeyEnv, secretKeyEnv string) (*S3Backend, error) {
	if accessKeyEnv == "" || secretKeyEnv == "" {
		return nil, fmt.Errorf("explicit S3 credential environment names are required")
	}
	return newS3Backend(ctx, settings, accessKeyEnv, secretKeyEnv)
}

func newS3Backend(ctx context.Context, settings S3BackendConfig, accessKeyEnv, secretKeyEnv string) (*S3Backend, error) {
	if settings.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}
	if settings.Endpoint != "" {
		if err := ValidateHTTPSHostEndpoint(settings.Endpoint); err != nil {
			return nil, err
		}
	}
	if settings.Region == "" {
		settings.Region = "auto"
	}
	loadOptions := []func(*config.LoadOptions) error{config.WithRegion(settings.Region)}
	if accessKeyEnv != "" || secretKeyEnv != "" {
		if accessKeyEnv == "" || secretKeyEnv == "" {
			return nil, fmt.Errorf("S3 credential environment names are incomplete")
		}
		accessKey, accessOK := os.LookupEnv(accessKeyEnv)
		secretKey, secretOK := os.LookupEnv(secretKeyEnv)
		if !accessOK || !secretOK || accessKey == "" || secretKey == "" {
			return nil, fmt.Errorf("S3 credentials are unavailable")
		}
		loadOptions = append(loadOptions, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}
	awsConfig, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load S3 configuration")
	}
	return newS3BackendFromConfig(awsConfig, settings), nil
}

func newS3BackendWithCredentials(ctx context.Context, settings S3BackendConfig, accessKey, secretKey string) (*S3Backend, error) {
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("S3 credentials are incomplete")
	}
	if settings.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}
	if settings.Endpoint == "" {
		return nil, fmt.Errorf("S3 endpoint is required")
	}
	if err := ValidateHTTPSHostEndpoint(settings.Endpoint); err != nil {
		return nil, err
	}
	if settings.Region == "" {
		settings.Region = "auto"
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	awsConfig := aws.Config{
		Region:      settings.Region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}
	return newS3BackendFromConfig(awsConfig, settings), nil
}

func newS3BackendFromConfig(awsConfig aws.Config, settings S3BackendConfig) *S3Backend {
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.UsePathStyle = true
		if settings.Endpoint != "" {
			options.BaseEndpoint = aws.String(settings.Endpoint)
		}
	})
	return &S3Backend{client: client, bucket: settings.Bucket}
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

func (b *S3Backend) PutFileIfAbsent(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectCommit, error) {
	if expectedBytes > uint64(^uint64(0)>>1) {
		return RemoteObjectCommit{}, fmt.Errorf("%w: local object is too large", ErrResourceLimit)
	}
	for attempt := 0; attempt < 2; attempt++ {
		operationCtx := ctx
		cancel := func() {}
		if attempt > 0 {
			operationCtx, cancel = remoteOutcomeProbeContext(ctx)
		}
		commit, err := b.putFileIfAbsentOnce(operationCtx, key, path, expectedSHA256, expectedBytes)
		cancel()
		if err == nil {
			var verifyErr error
			if attempt > 0 {
				_, verifyErr = b.verifyAfterUnknownPut(ctx, key, path, expectedSHA256, expectedBytes)
			} else {
				_, verifyErr = b.VerifyFile(ctx, key, path, expectedSHA256, expectedBytes)
			}
			if verifyErr != nil {
				return RemoteObjectCommit{}, verifyErr
			}
			return commit, nil
		}
		classified := classifyRemoteError(err)
		if errors.Is(classified, ErrObjectExists) {
			return b.verifyExistingImmutableObject(ctx, key, path, expectedSHA256, expectedBytes)
		}
		if isUnknownRemoteWriteOutcome(err, classified) {
			verification, verifyErr := b.verifyAfterUnknownPut(ctx, key, path, expectedSHA256, expectedBytes)
			if verifyErr == nil {
				return RemoteObjectCommit{ETag: verification.ETag}, nil
			}
			if errors.Is(verifyErr, ErrObjectNotFound) && attempt == 0 {
				continue
			}
			if errors.Is(verifyErr, ErrRemoteCheckMismatch) {
				return RemoteObjectCommit{}, ErrImmutableCollision
			}
			return RemoteObjectCommit{}, verifyErr
		}
		return RemoteObjectCommit{}, classified
	}
	return RemoteObjectCommit{}, ErrObjectNotFound
}

func (b *S3Backend) putFileIfAbsentOnce(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectCommit, error) {
	file, contentMD5, err := openVerifiedLocalFile(path, expectedSHA256, expectedBytes)
	if err != nil {
		return RemoteObjectCommit{}, err
	}
	defer file.Close()
	size := int64(expectedBytes)
	output, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(key),
		Body:          file,
		ContentMD5:    aws.String(base64.StdEncoding.EncodeToString(contentMD5[:])),
		ContentLength: &size,
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		return RemoteObjectCommit{}, err
	}
	return RemoteObjectCommit{ETag: aws.ToString(output.ETag)}, nil
}

func (b *S3Backend) verifyExistingImmutableObject(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectCommit, error) {
	verification, verifyErr := b.VerifyFile(ctx, key, path, expectedSHA256, expectedBytes)
	if verifyErr == nil {
		return RemoteObjectCommit{ETag: verification.ETag}, nil
	}
	if errors.Is(verifyErr, ErrRemoteCheckMismatch) {
		return RemoteObjectCommit{}, ErrImmutableCollision
	}
	return RemoteObjectCommit{}, verifyErr
}

func (b *S3Backend) verifyAfterUnknownPut(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectVerification, error) {
	probeCtx, cancel := remoteOutcomeProbeContext(ctx)
	defer cancel()
	return b.VerifyFile(probeCtx, key, path, expectedSHA256, expectedBytes)
}

func remoteOutcomeProbeContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), remoteOutcomeProbeTimeout)
}

func isUnknownRemoteWriteOutcome(original, classified error) bool {
	if errors.Is(original, context.Canceled) || errors.Is(original, context.DeadlineExceeded) || errors.Is(classified, context.Canceled) || errors.Is(classified, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(original, &netErr) && netErr.Timeout()
}

func (b *S3Backend) VerifyFile(ctx context.Context, key, path string, expectedSHA256 [32]byte, expectedBytes uint64) (RemoteObjectVerification, error) {
	if err := verifyLocalFileIdentity(path, expectedSHA256, expectedBytes); err != nil {
		return RemoteObjectVerification{}, err
	}
	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return RemoteObjectVerification{}, classifyRemoteError(err)
	}
	defer output.Body.Close()
	if output.ContentLength != nil && (*output.ContentLength < 0 || uint64(*output.ContentLength) != expectedBytes) {
		return RemoteObjectVerification{}, ErrRemoteCheckMismatch
	}
	hash := sha256.New()
	read, err := io.Copy(hash, io.LimitReader(output.Body, int64(expectedBytes)+1))
	if err != nil {
		return RemoteObjectVerification{}, fmt.Errorf("read remote object for verification: %w", err)
	}
	if read < 0 || uint64(read) != expectedBytes {
		return RemoteObjectVerification{}, ErrRemoteCheckMismatch
	}
	var got [32]byte
	copy(got[:], hash.Sum(nil))
	if got != expectedSHA256 {
		return RemoteObjectVerification{}, ErrRemoteCheckMismatch
	}
	return RemoteObjectVerification{ETag: aws.ToString(output.ETag)}, nil
}

func verifyLocalFileIdentity(path string, expectedSHA256 [32]byte, expectedBytes uint64) error {
	file, _, err := openVerifiedLocalFile(path, expectedSHA256, expectedBytes)
	if err != nil {
		return err
	}
	return file.Close()
}

func openVerifiedLocalFile(path string, expectedSHA256 [32]byte, expectedBytes uint64) (*os.File, [16]byte, error) {
	var contentMD5 [16]byte
	if path == "" || expectedSHA256 == ([32]byte{}) {
		return nil, contentMD5, fmt.Errorf("%w: local object identity is incomplete", ErrLocalObjectChanged)
	}
	if expectedBytes > uint64(^uint64(0)>>1) {
		return nil, contentMD5, fmt.Errorf("%w: local object is too large", ErrResourceLimit)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, contentMD5, fmt.Errorf("%w: open local object: %v", ErrLocalObjectChanged, err)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != expectedBytes {
		_ = file.Close()
		return nil, contentMD5, fmt.Errorf("%w: local object size changed", ErrLocalObjectChanged)
	}
	shaHash := sha256.New()
	md5Hash := md5.New()
	read, err := io.Copy(io.MultiWriter(shaHash, md5Hash), io.LimitReader(file, int64(expectedBytes)+1))
	if err != nil || read < 0 || uint64(read) != expectedBytes {
		_ = file.Close()
		return nil, contentMD5, fmt.Errorf("%w: local object bytes changed", ErrLocalObjectChanged)
	}
	var got [32]byte
	copy(got[:], shaHash.Sum(nil))
	if got != expectedSHA256 {
		_ = file.Close()
		return nil, contentMD5, fmt.Errorf("%w: local object digest changed", ErrLocalObjectChanged)
	}
	copy(contentMD5[:], md5Hash.Sum(nil))
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, contentMD5, fmt.Errorf("%w: rewind local object: %v", ErrLocalObjectChanged, err)
	}
	return file, contentMD5, nil
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

func (b *S3Backend) GetLimited(ctx context.Context, key string, maxBytes uint64) ([]byte, error) {
	if maxBytes == 0 || maxBytes >= uint64(^uint64(0)>>1) {
		return nil, fmt.Errorf("%w: invalid metadata byte limit", ErrResourceLimit)
	}
	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, classifyRemoteError(err)
	}
	defer output.Body.Close()
	if output.ContentLength != nil && (*output.ContentLength < 0 || uint64(*output.ContentLength) > maxBytes) {
		return nil, fmt.Errorf("%w: metadata object is oversized", ErrResourceLimit)
	}
	body, err := io.ReadAll(io.LimitReader(output.Body, int64(maxBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("read remote metadata: %w", err)
	}
	if uint64(len(body)) > maxBytes {
		return nil, fmt.Errorf("%w: metadata object is oversized", ErrResourceLimit)
	}
	return body, nil
}

func (b *S3Backend) Open(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, classifyRemoteError(err)
	}
	size := int64(-1)
	if output.ContentLength != nil {
		size = *output.ContentLength
	}
	return output.Body, size, nil
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

func (b *S3Backend) ListLimited(ctx context.Context, prefix string, maxObjects uint64) ([]RemoteObject, error) {
	if maxObjects == 0 || maxObjects > uint64(^uint64(0)>>1) {
		return nil, fmt.Errorf("%w: invalid object count limit", ErrResourceLimit)
	}
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(prefix),
	})
	result := make([]RemoteObject, 0, minInt64(maxObjects, 256))
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, classifyRemoteError(err)
		}
		for _, object := range page.Contents {
			if object.Key == nil {
				continue
			}
			if uint64(len(result)) >= maxObjects {
				return nil, fmt.Errorf("%w: derivative object count exceeds limit", ErrResourceLimit)
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

func minInt64(value uint64, maximum int) int {
	if value > uint64(maximum) {
		return maximum
	}
	return int(value)
}

func classifyRemoteError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("remote object operation canceled or timed out: %w", err)
	}
	var statusError interface{ HTTPStatusCode() int }
	if errors.As(err, &statusError) {
		switch statusError.HTTPStatusCode() {
		case 404:
			return ErrObjectNotFound
		case 409, 412:
			return ErrObjectExists
		case 401, 403:
			return ErrRemotePermission
		}
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return ErrObjectNotFound
		case "PreconditionFailed", "ConditionalRequestConflict", "Conflict":
			return ErrObjectExists
		case "AccessDenied", "Unauthorized", "Forbidden", "InvalidAccessKeyId", "InvalidClientTokenId", "InvalidToken", "ExpiredToken", "MissingAuthenticationToken", "SignatureDoesNotMatch":
			return ErrRemotePermission
		default:
			return fmt.Errorf("remote object operation failed with code %s", apiError.ErrorCode())
		}
	}
	return fmt.Errorf("remote object operation failed")
}
