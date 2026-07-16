package publication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/wal"
)

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type TickerFactory func(time.Duration) Ticker

type realTicker struct{ ticker *time.Ticker }

func (t *realTicker) C() <-chan time.Time { return t.ticker.C }
func (t *realTicker) Stop()               { t.ticker.Stop() }

func newRealTicker(interval time.Duration) Ticker {
	return &realTicker{ticker: time.NewTicker(interval)}
}

// LocalPipelineConfig describes the local half of publication. It deliberately
// has no R2 write capability; the SDK uploader is connected by the composition
// root through ManifestGate and the separate Uploader.
type LocalPipelineConfig struct {
	WAL           *wal.Store
	Catalog       *Catalog
	RawOutboxRoot string
	ManifestRoot  string
	Scope         archive.ScopeConfig
	SealMaxBytes  uint64
	SealInterval  time.Duration
	ScanInterval  time.Duration
	Clock         Clock
	TickerFactory TickerFactory
	ManifestGate  ManifestPublicationGate
}

// LocalPipeline seals active WAL, promotes verified segments, and spools
// canonical provisional manifests. Each call is idempotent with respect to
// already-published local bytes.
type LocalPipeline struct {
	wal           *wal.Store
	catalog       *Catalog
	rawOutboxRoot string
	planner       *Planner
	sealMaxBytes  uint64
	sealInterval  time.Duration
	scanInterval  time.Duration
	clock         Clock
	tickerFactory TickerFactory

	processMu sync.Mutex
	mu        sync.RWMutex
	started   bool
	starting  bool
	cancel    context.CancelFunc
	done      chan struct{}
	errors    chan error
	lastError error
	lastSeal  time.Time
}

func NewLocalPipeline(config LocalPipelineConfig) (*LocalPipeline, error) {
	if config.WAL == nil || config.Catalog == nil || config.RawOutboxRoot == "" || config.ManifestRoot == "" {
		return nil, fmt.Errorf("local publication pipeline dependencies are incomplete")
	}
	if config.SealMaxBytes == 0 || config.SealInterval <= 0 || config.ScanInterval <= 0 {
		return nil, fmt.Errorf("local publication pipeline thresholds are invalid")
	}
	if config.Clock == nil {
		return nil, fmt.Errorf("local publication pipeline clock is required")
	}
	tickerFactory := config.TickerFactory
	if tickerFactory == nil {
		tickerFactory = newRealTicker
	}
	planner, err := NewPlannerWithGate(config.Scope, config.ManifestRoot, config.Catalog, config.Clock, config.ManifestGate)
	if err != nil {
		return nil, err
	}
	return &LocalPipeline{
		wal:           config.WAL,
		catalog:       config.Catalog,
		rawOutboxRoot: config.RawOutboxRoot,
		planner:       planner,
		sealMaxBytes:  config.SealMaxBytes,
		sealInterval:  config.SealInterval,
		scanInterval:  config.ScanInterval,
		clock:         config.Clock,
		tickerFactory: tickerFactory,
		errors:        make(chan error, 1),
	}, nil
}

func (p *LocalPipeline) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil {
		return fmt.Errorf("local publication pipeline is nil")
	}
	p.mu.Lock()
	if p.started || p.starting {
		p.mu.Unlock()
		return fmt.Errorf("local publication pipeline is already started")
	}
	p.starting = true
	p.mu.Unlock()
	if err := p.ProcessOnce(ctx); err != nil {
		p.mu.Lock()
		p.starting = false
		p.mu.Unlock()
		return err
	}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	p.mu.Lock()
	p.started = true
	p.starting = false
	p.cancel = cancel
	p.done = make(chan struct{})
	p.mu.Unlock()
	go p.run(runtimeCtx)
	return nil
}

func (p *LocalPipeline) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil {
		return nil
	}
	p.mu.RLock()
	started := p.started
	p.mu.RUnlock()
	if !started {
		return nil
	}
	finalErr := p.ProcessOnce(ctx)
	p.mu.Lock()
	cancel := p.cancel
	done := p.done
	p.mu.Unlock()
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
	p.mu.Lock()
	p.started = false
	p.cancel = nil
	p.mu.Unlock()
	if finalErr != nil && !errors.Is(finalErr, wal.ErrEmptySegment) {
		return finalErr
	}
	return nil
}

// ProcessOnce is exported for network-free integration tests and startup
// reconciliation. The caller must have started the supplied WAL and Catalog.
func (p *LocalPipeline) ProcessOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil {
		return fmt.Errorf("local publication pipeline is nil")
	}
	p.processMu.Lock()
	defer p.processMu.Unlock()
	if err := p.planner.Reconcile(ctx); err != nil {
		return err
	}
	if err := p.maybeSeal(); err != nil {
		return err
	}
	if err := p.promoteSealed(ctx); err != nil {
		return err
	}
	return p.spoolAffectedManifests(ctx)
}

func (p *LocalPipeline) Errors() <-chan error {
	if p == nil {
		return nil
	}
	return p.errors
}

func (p *LocalPipeline) LastError() error {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastError
}

func (p *LocalPipeline) run(ctx context.Context) {
	ticker := p.tickerFactory(p.scanInterval)
	if ticker == nil {
		err := fmt.Errorf("local publication ticker factory returned nil")
		p.mu.Lock()
		p.lastError = err
		p.mu.Unlock()
		select {
		case p.errors <- err:
		default:
		}
		return
	}
	defer ticker.Stop()
	defer func() {
		p.mu.Lock()
		done := p.done
		p.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			if err := p.ProcessOnce(ctx); err != nil {
				p.mu.Lock()
				p.lastError = err
				p.mu.Unlock()
				select {
				case p.errors <- err:
				default:
				}
				return
			}
		}
	}
}

func (p *LocalPipeline) maybeSeal() error {
	path := p.wal.Path()
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat active WAL for seal: %w", err)
	}
	now := p.clock().UTC()
	if p.lastSeal.IsZero() {
		p.lastSeal = info.ModTime().UTC()
		if p.lastSeal.IsZero() || p.lastSeal.After(now) {
			p.lastSeal = now
		}
	}
	if uint64(info.Size()) < p.sealMaxBytes && now.Sub(p.lastSeal) < p.sealInterval {
		return nil
	}
	_, err = p.wal.Seal()
	if errors.Is(err, wal.ErrEmptySegment) {
		p.lastSeal = now
		return nil
	}
	if err != nil {
		return err
	}
	p.lastSeal = now
	return nil
}

func (p *LocalPipeline) promoteSealed(ctx context.Context) error {
	segments := p.wal.SealedSegments()
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if segment.Path == "" {
			return fmt.Errorf("sealed WAL segment has no path")
		}
		raw, err := archive.PromoteSealedSegment(p.rawOutboxRoot, segment.Path)
		if err != nil {
			return err
		}
		dates, err := AffectedDates(raw.Segment)
		if err != nil {
			return err
		}
		record := SegmentRecord{
			Identity:      SegmentIdentity(raw.SHA256),
			SealedPath:    segment.Path,
			RawKey:        raw.Key,
			RawPath:       raw.Path,
			SHA256:        raw.SHA256,
			Bytes:         uint64(raw.Bytes),
			StartSequence: raw.Segment.StartSequence,
			EndSequence:   raw.Segment.LastSequence,
			AffectedDates: dates,
			State:         SegmentStatePromoted,
			UpdatedAt:     p.clock().UTC(),
		}
		if err := p.catalog.UpsertSegment(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func (p *LocalPipeline) spoolAffectedManifests(ctx context.Context) error {
	records, err := p.catalog.ListSegments(ctx)
	if err != nil {
		return err
	}
	objects := make([]archive.RawObject, 0, len(records))
	dates := make(map[string]struct{})
	for _, record := range records {
		segment, err := wal.VerifySealedSegment(record.RawPath)
		if err != nil {
			return fmt.Errorf("verify catalogued raw object: %w", err)
		}
		if segment.ObjectSHA256 != record.SHA256 || uint64(segment.FileBytes) != record.Bytes ||
			segment.StartSequence != record.StartSequence || segment.LastSequence != record.EndSequence {
			return fmt.Errorf("catalogued raw object metadata changed")
		}
		objects = append(objects, archive.RawObject{
			Key: record.RawKey, Path: record.RawPath, SHA256: record.SHA256,
			Bytes: int64(record.Bytes), Segment: segment,
		})
		for _, date := range record.AffectedDates {
			dates[date] = struct{}{}
		}
	}
	orderedDates := make([]string, 0, len(dates))
	for date := range dates {
		orderedDates = append(orderedDates, date)
	}
	sortStrings(orderedDates)
	for _, date := range orderedDates {
		if _, _, err := p.planner.Plan(ctx, date, objects); err != nil {
			return err
		}
	}
	return nil
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
