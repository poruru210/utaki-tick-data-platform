package publication

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/r2"
)

// RemotePublisher is the narrow capability the resident worker needs from the
// SDK-backed publication boundary.
type RemotePublisher interface {
	Publish(context.Context, r2.PublicationInput) (r2.VerificationReceipt, error)
}

type RemoteIntentSource interface {
	ListUnfinished(context.Context) ([]r2.UnfinishedPublication, error)
}

// PendingPublicationSink receives the same durable backlog measurement used by
// the operator status view. The sink is intentionally one-way so the uploader
// cannot change ingest policy or inspect Gateway internals.
type PendingPublicationSink interface {
	SetPendingPublication(segments, bytes uint64)
}

type UploaderConfig struct {
	Catalog       *Catalog
	Publisher     RemotePublisher
	RemoteIntents RemoteIntentSource
	ReceiptRoot   string
	ScanInterval  time.Duration
	RetryMin      time.Duration
	RetryMax      time.Duration
	Clock         Clock
	PendingSink   PendingPublicationSink
	Priority      PublicationPriorityReader
}

// Uploader publishes only canonical manifests already recorded by the local
// Catalog. It owns retry scheduling, while r2.Publisher owns remote intent,
// conditional object writes, and full-byte verification.
type Uploader struct {
	catalog       *Catalog
	publisher     RemotePublisher
	remoteIntents RemoteIntentSource
	receiptRoot   string
	scanInterval  time.Duration
	retryMin      time.Duration
	retryMax      time.Duration
	clock         Clock
	pendingSink   PendingPublicationSink
	priority      PublicationPriorityReader

	processMu      sync.Mutex
	mu             sync.Mutex
	started        bool
	starting       bool
	cancel         context.CancelFunc
	done           chan struct{}
	errors         chan error
	lastError      error
	lastErrorClass string
}

func NewUploader(config UploaderConfig) (*Uploader, error) {
	if config.Catalog == nil || config.Publisher == nil || config.ReceiptRoot == "" {
		return nil, fmt.Errorf("publication uploader dependencies are incomplete")
	}
	if config.ScanInterval <= 0 || config.RetryMin <= 0 || config.RetryMax <= 0 || config.RetryMin > config.RetryMax {
		return nil, fmt.Errorf("publication uploader retry policy is invalid")
	}
	if config.Clock == nil {
		return nil, fmt.Errorf("publication uploader clock is required")
	}
	return &Uploader{
		catalog:       config.Catalog,
		publisher:     config.Publisher,
		remoteIntents: config.RemoteIntents,
		receiptRoot:   config.ReceiptRoot,
		scanInterval:  config.ScanInterval,
		retryMin:      config.RetryMin,
		retryMax:      config.RetryMax,
		clock:         config.Clock,
		pendingSink:   config.PendingSink,
		priority:      config.Priority,
		errors:        make(chan error, 1),
	}, nil
}

func (u *Uploader) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if u == nil {
		return fmt.Errorf("publication uploader is nil")
	}
	u.mu.Lock()
	if u.started || u.starting {
		u.mu.Unlock()
		return fmt.Errorf("publication uploader is already started")
	}
	u.starting = true
	u.mu.Unlock()
	runtimeCtx, cancel := context.WithCancel(context.Background())
	u.mu.Lock()
	u.started = true
	u.starting = false
	u.cancel = cancel
	u.done = make(chan struct{})
	u.mu.Unlock()
	go u.run(runtimeCtx)
	return nil
}

func (u *Uploader) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if u == nil {
		return nil
	}
	u.mu.Lock()
	if !u.started {
		u.mu.Unlock()
		return nil
	}
	cancel := u.cancel
	done := u.done
	u.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	u.mu.Lock()
	u.started = false
	u.cancel = nil
	u.mu.Unlock()
	return nil
}

func (u *Uploader) Errors() <-chan error {
	if u == nil {
		return nil
	}
	return u.errors
}

func (u *Uploader) LastError() error {
	if u == nil {
		return nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lastError
}

// LastErrorClass exposes only the bounded operational classification. Raw
// SDK errors remain private to the worker and cannot enter status output.
func (u *Uploader) LastErrorClass() string {
	if u == nil {
		return ""
	}
	u.mu.Lock()
	class := u.lastErrorClass
	err := u.lastError
	u.mu.Unlock()
	if class != "" {
		return class
	}
	if err == nil {
		return ""
	}
	classified, _ := classifyPublicationError(err)
	return classified
}

// PublishDue is exported for deterministic fake-backend tests. At most one
// transient failure is scheduled per call; the next tick observes durable
// backoff instead of retrying in a tight loop.
func (u *Uploader) PublishDue(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u.processMu.Lock()
	defer u.processMu.Unlock()
	if err := u.publishUnfinished(ctx); err != nil {
		return err
	}
	records, err := u.catalog.ListDueManifests(ctx, u.clock().UTC())
	if err != nil {
		return err
	}
	for _, record := range records {
		input, err := u.inputFor(ctx, record)
		if err != nil {
			return err
		}
		_, err = u.publisher.Publish(ctx, input)
		if err == nil {
			if err := u.markPublished(ctx, input.Manifest); err != nil {
				return err
			}
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		class, retry := classifyPublicationError(err)
		if !retry {
			u.recordFailure(err, class)
			return fmt.Errorf("publication %s: %w", class, err)
		}
		next := u.clock().UTC().Add(retryDelay(record.Attempts+1, u.retryMin, u.retryMax))
		if err := u.catalog.MarkManifestRetry(ctx, record.Date, record.Revision, class, next, u.clock().UTC()); err != nil {
			return err
		}
		u.recordRetry(class)
		return nil
	}
	return u.refreshPending(ctx)
}

func (u *Uploader) publishUnfinished(ctx context.Context) error {
	if u.remoteIntents == nil {
		return nil
	}
	unfinished, err := u.remoteIntents.ListUnfinished(ctx)
	if err != nil {
		return err
	}
	now := u.clock().UTC()
	for _, pending := range unfinished {
		input := pending.Input
		if err := u.ensureManifestRecord(ctx, input); err != nil {
			return err
		}
		record, found, err := u.catalog.ManifestAt(ctx, input.Manifest.Date, input.Manifest.Revision)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("unfinished remote intent has no local manifest record")
		}
		if record.State == ManifestStatePublished {
			return fmt.Errorf("%w: local manifest is published while remote intent is unfinished", archive.ErrIntegrity)
		}
		if record.State == ManifestStateRetryWait && record.NextRetryAt.After(now) {
			continue
		}
		_, err = u.publisher.Publish(ctx, input)
		if err == nil {
			if err := u.markPublished(ctx, input.Manifest); err != nil {
				return err
			}
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		class, retry := classifyPublicationError(err)
		if !retry {
			u.recordFailure(err, class)
			return fmt.Errorf("publication %s: %w", class, err)
		}
		next := now.Add(retryDelay(record.Attempts+1, u.retryMin, u.retryMax))
		if err := u.catalog.MarkManifestRetry(ctx, input.Manifest.Date, input.Manifest.Revision, class, next, now); err != nil {
			return err
		}
		u.recordRetry(class)
		return nil
	}
	return u.refreshPending(ctx)
}

func (u *Uploader) ensureManifestRecord(ctx context.Context, input r2.PublicationInput) error {
	return u.catalog.EnsureManifest(ctx, ManifestRecord{
		Identity:  ManifestIdentity(input.Manifest.Date, input.Manifest.Revision, input.Manifest.ManifestSHA256),
		Date:      input.Manifest.Date,
		Revision:  input.Manifest.Revision,
		Path:      input.ManifestPath,
		SHA256:    input.Manifest.ManifestSHA256,
		Bytes:     uint64(len(input.ManifestBytes)),
		State:     ManifestStateSpooled,
		UpdatedAt: u.clock().UTC(),
	})
}

func (u *Uploader) markPublished(ctx context.Context, manifest archive.RawDayManifest) error {
	if err := u.catalog.MarkManifestPublished(ctx, manifest, u.clock().UTC()); err != nil {
		return err
	}
	u.mu.Lock()
	u.lastError = nil
	u.lastErrorClass = ""
	u.mu.Unlock()
	return nil
}

func (u *Uploader) refreshPending(ctx context.Context) error {
	if u.pendingSink == nil {
		return nil
	}
	stats, err := u.catalog.PendingStats(ctx)
	if err != nil {
		return err
	}
	u.pendingSink.SetPendingPublication(stats.PendingSegments, stats.PendingBytes)
	return nil
}

func (u *Uploader) run(ctx context.Context) {
	defer func() {
		u.mu.Lock()
		done := u.done
		u.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()
	if err := u.PublishDue(ctx); err != nil && !errors.Is(err, context.Canceled) {
		u.reportError(err)
		return
	}
	ticker := time.NewTicker(u.scanInterval)
	defer ticker.Stop()
	priorityWakeups := (<-chan struct{})(nil)
	if u.priority != nil {
		priorityWakeups = u.priority.PriorityWakeups()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := u.PublishDue(ctx); err != nil && !errors.Is(err, context.Canceled) {
				u.reportError(err)
				return
			}
		case <-priorityWakeups:
			if u.priority == nil || !u.priority.PublicationWorkerPriority() {
				continue
			}
			if err := u.PublishDue(ctx); err != nil && !errors.Is(err, context.Canceled) {
				u.reportError(err)
				return
			}
		}
	}
}

func (u *Uploader) reportError(err error) {
	class, _ := classifyPublicationError(err)
	u.recordFailure(err, class)
	select {
	case u.errors <- err:
	default:
	}
}

func (u *Uploader) recordRetry(class string) {
	u.mu.Lock()
	u.lastErrorClass = class
	u.mu.Unlock()
}

func (u *Uploader) recordFailure(err error, class string) {
	u.mu.Lock()
	u.lastError = err
	u.lastErrorClass = class
	u.mu.Unlock()
}

func (u *Uploader) inputFor(ctx context.Context, record ManifestRecord) (r2.PublicationInput, error) {
	manifest, err := readCanonicalManifest(record.Path, record.SHA256, record.Bytes)
	if err != nil {
		return r2.PublicationInput{}, fmt.Errorf("load manifest for publication: %w", err)
	}
	manifestBytes, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		return r2.PublicationInput{}, err
	}
	segments, err := u.catalog.ListSegments(ctx)
	if err != nil {
		return r2.PublicationInput{}, err
	}
	paths := make(map[string]string, len(segments))
	for _, segment := range segments {
		paths[segment.RawKey] = segment.RawPath
	}
	for _, object := range manifest.ChainObjects {
		if paths[object.Key] == "" {
			return r2.PublicationInput{}, fmt.Errorf("manifest raw object path is missing")
		}
	}
	return r2.PublicationInput{
		Manifest:      manifest,
		ManifestBytes: manifestBytes,
		ManifestPath:  record.Path,
		ObjectPaths:   paths,
		ReceiptPath:   filepath.Join(u.receiptRoot, "raw-day", "date="+manifest.Date, fmt.Sprintf("receipt-%020d-%x.json", manifest.Revision, manifest.ManifestSHA256)),
	}, nil
}

func classifyPublicationError(err error) (string, bool) {
	switch {
	case errors.Is(err, r2.ErrImmutableCollision), errors.Is(err, r2.ErrPublisherConflict):
		return "collision", false
	case errors.Is(err, r2.ErrRemoteCheckMismatch):
		return "remote_integrity", false
	case errors.Is(err, r2.ErrRemotePermission):
		return "permission", false
	case errors.Is(err, archive.ErrIntegrity), errors.Is(err, r2.ErrLocalObjectChanged), errors.Is(err, r2.ErrResourceLimit):
		return "local_integrity", false
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "remote_timeout", true
	default:
		return "remote_transient", true
	}
}

func retryDelay(attempt uint64, minimum, maximum time.Duration) time.Duration {
	if attempt == 0 {
		attempt = 1
	}
	delay := minimum
	for step := uint64(1); step < attempt && delay < maximum; step++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}
