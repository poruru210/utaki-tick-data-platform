// Package app is the only production composition root. Domain packages do not
// import Fx; this package connects their ordinary Go lifecycle contracts.
package app

import (
	"context"
	"path/filepath"
	"time"

	"go.uber.org/fx"

	"tick-data-platform/internal/archive"
	appconfig "tick-data-platform/internal/config"
	"tick-data-platform/internal/credentials"
	"tick-data-platform/internal/ingest"
	"tick-data-platform/internal/journal"
	"tick-data-platform/internal/operations"
	"tick-data-platform/internal/publication"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/retention"
	"tick-data-platform/internal/wal"
)

func ProductionOptions(configValue appconfig.Config) fx.Option {
	return fx.Options(
		fx.Supply(configValue),
		BaseOptions(),
		FileCredentialModule,
	)
}

func TestOptions(configValue appconfig.Config, provider credentials.Provider) fx.Option {
	return fx.Options(
		fx.Supply(configValue),
		BaseOptions(),
		fx.Supply(fx.Annotate(provider, fx.As(new(credentials.Provider)))),
	)
}

// TestOptionsWithRemoteBackend keeps the production lifecycle graph while
// replacing only the write capability with a network-free fake. The
// credential-bound component is still started so credential ownership and
// lifecycle ordering remain covered by the test.
func TestOptionsWithRemoteBackend(configValue appconfig.Config, provider credentials.Provider, backend r2.WriteBackend) fx.Option {
	return fx.Options(
		fx.Supply(configValue),
		CoreOptions(),
		fx.Provide(newCredentialBackend, newPublicationJournal, newLayout),
		fx.Supply(fx.Annotate(provider, fx.As(new(credentials.Provider)))),
		fx.Supply(fx.Annotate(backend, fx.As(new(r2.WriteBackend)))),
	)
}

func BaseOptions() fx.Option {
	return fx.Options(CoreOptions(), RemoteModule)
}

func CoreOptions() fx.Option {
	return fx.Options(ConfigModule, StorageModule, PublicationModule, fx.Provide(newStatusService), RuntimeModule)
}

var ConfigModule = fx.Module(
	"config",
	fx.Invoke(validateConfig),
)

var FileCredentialModule = fx.Module(
	"credentials.file",
	fx.Provide(
		credentialFileConfig,
		fx.Annotate(credentials.NewFileProvider, fx.As(new(credentials.Provider))),
	),
)

var StorageModule = fx.Module(
	"storage",
	fx.Provide(
		newWALRecovery,
		newWALStore,
		newJournalStore,
		newDiskState,
		newGateway,
		newPublicationCatalog,
	),
)

var RemoteModule = fx.Module(
	"remote",
	fx.Provide(newRemoteBackend, newPublicationJournal, newLayout),
)

var PublicationModule = fx.Module(
	"publication",
	fx.Provide(newManifestPublicationGate, newLocalPipeline, newPublisher, newUploader),
)

var RuntimeModule = fx.Module(
	"runtime",
	fx.Invoke(registerLifecycle),
)

func validateConfig(configValue appconfig.Config) error {
	if err := configValue.ValidateForRun(); err != nil {
		return err
	}
	return nil
}

func credentialFileConfig(configValue appconfig.Config) (credentials.FileConfig, error) {
	return credentials.FileConfig{
		Path:       configValue.Credentials.Path,
		Protection: credentials.ProtectionMode(configValue.Credentials.Protection),
	}, nil
}

func newWALRecovery(configValue appconfig.Config) (*retention.WALRecovery, error) {
	return retention.NewWALRecovery(configValue.WALRoot)
}

func newWALStore(configValue appconfig.Config, recovery *retention.WALRecovery) (*wal.Store, error) {
	return wal.NewStore(configValue.WALRoot, configValue.GatewayInstanceID, recovery)
}

func newJournalStore(configValue appconfig.Config) (*journal.Store, error) {
	gateway := configValue.Gateway()
	return journal.NewStore(gateway.JournalPath, gateway.GatewayInstanceID, gateway.InitialFromMSC, gateway.InitialBatchCount)
}

func newDiskState(configValue appconfig.Config) (*ingest.DiskStateMachine, error) {
	gateway := configValue.Gateway()
	return ingest.NewDiskStateMachine(gateway.WALRoot, ingest.DiskWatermarks{
		HighFreeBytes: gateway.DiskHighFreeBytes, CriticalFreeBytes: gateway.DiskCriticalFreeBytes,
		EmergencyFreeBytes: gateway.DiskEmergencyFreeBytes,
		MaxPendingSegments: configValue.Publication.MaxPendingSegments,
		MaxPendingBytes:    configValue.Publication.MaxPendingBytes,
	}, ingest.OSDiskUsageProvider{})
}

func newGateway(configValue appconfig.Config, store *wal.Store, journalStore *journal.Store, disk *ingest.DiskStateMachine) (*ingest.Gateway, error) {
	return ingest.NewGateway(toIngestConfig(configValue), store, journalStore, disk)
}

type publicationRuntimeConfig struct {
	fx.In
	Clock         publication.Clock         `optional:"true"`
	TickerFactory publication.TickerFactory `optional:"true"`
}

func newPublicationCatalog(configValue appconfig.Config, runtime publicationRuntimeConfig) (*publication.Catalog, error) {
	clock := runtime.Clock
	if clock == nil {
		clock = time.Now
	}
	return publication.NewCatalogWithClock(configValue.Publication.CatalogPath, clock)
}

func newLocalPipeline(configValue appconfig.Config, store *wal.Store, catalog *publication.Catalog, disk *ingest.DiskStateMachine, manifestGate publication.ManifestPublicationGate, runtime publicationRuntimeConfig) (*publication.LocalPipeline, error) {
	clock := runtime.Clock
	if clock == nil {
		clock = time.Now
	}
	return publication.NewLocalPipeline(publication.LocalPipelineConfig{
		WAL:           store,
		Catalog:       catalog,
		RawOutboxRoot: configValue.RawOutboxRoot,
		ManifestRoot:  configValue.Publication.ManifestRoot,
		Scope:         toArchiveScope(configValue),
		SealMaxBytes:  configValue.Publication.SealMaxBytes,
		SealInterval:  time.Duration(configValue.Publication.SealIntervalMS) * time.Millisecond,
		ScanInterval:  time.Duration(configValue.Publication.ScanIntervalMS) * time.Millisecond,
		Clock:         clock,
		TickerFactory: runtime.TickerFactory,
		ManifestGate:  manifestGate,
		PendingSink:   disk,
		Priority:      disk,
	})
}

type remoteBackendResult struct {
	fx.Out
	Backend *r2.CredentialBackend
	Writer  r2.WriteBackend
}

func newRemoteBackend(configValue appconfig.Config, provider credentials.Provider) (remoteBackendResult, error) {
	backend, err := newCredentialBackend(configValue, provider)
	if err != nil {
		return remoteBackendResult{}, err
	}
	return remoteBackendResult{Backend: backend, Writer: backend}, nil
}

func newCredentialBackend(configValue appconfig.Config, provider credentials.Provider) (*r2.CredentialBackend, error) {
	return r2.NewCredentialBackend(r2.S3BackendConfig{
		Bucket: configValue.R2.Bucket, Endpoint: configValue.R2.Endpoint, Region: configValue.R2.Region,
	}, provider)
}

func newPublicationJournal(configValue appconfig.Config) (*r2.PublicationJournal, error) {
	return r2.NewPublicationJournal(configValue.Publication.RemoteJournalPath)
}

func newLayout(configValue appconfig.Config) (r2.Layout, error) {
	return r2.NewLayout(configValue.R2.ImmutableRoot, toArchiveScope(configValue))
}

func newPublisher(configValue appconfig.Config, layout r2.Layout, backend r2.WriteBackend, journal *r2.PublicationJournal) (*r2.Publisher, error) {
	return r2.NewPublisherWithClock(layout, backend, journal, filepath.Join(configValue.Publication.ReceiptRoot, "publication.lock"), time.Now)
}

func newUploader(configValue appconfig.Config, catalog *publication.Catalog, publisher *r2.Publisher, remoteJournal *r2.PublicationJournal, disk *ingest.DiskStateMachine) (*publication.Uploader, error) {
	return publication.NewUploader(publication.UploaderConfig{
		Catalog:       catalog,
		Publisher:     publisher,
		RemoteIntents: remoteJournal,
		ReceiptRoot:   configValue.Publication.ReceiptRoot,
		ScanInterval:  time.Duration(configValue.Publication.ScanIntervalMS) * time.Millisecond,
		RetryMin:      time.Duration(configValue.Publication.RetryMinMS) * time.Millisecond,
		RetryMax:      time.Duration(configValue.Publication.RetryMaxMS) * time.Millisecond,
		Clock:         time.Now,
		PendingSink:   disk,
		Priority:      disk,
	})
}

type gatewayStatusReader struct{ gateway *ingest.Gateway }

func (r gatewayStatusReader) IngestStatus(ctx context.Context) (operations.IngestStatus, error) {
	if err := ctx.Err(); err != nil {
		return operations.IngestStatus{}, err
	}
	snapshot, err := r.gateway.Status()
	if err != nil {
		return operations.IngestStatus{}, err
	}
	return operations.IngestStatus{
		ReadyForACK:     snapshot.ReadyForACK,
		ActiveSession:   snapshot.ActiveSession,
		AcceptedBatches: snapshot.Metrics.AcceptedBatches,
		Connections:     snapshot.Metrics.Connections,
	}, nil
}

type publicationStatusReader struct {
	catalog       *publication.Catalog
	uploader      *publication.Uploader
	remoteJournal *r2.PublicationJournal
}

func (r publicationStatusReader) PublicationStatus(ctx context.Context) (operations.PublicationStatus, error) {
	if err := ctx.Err(); err != nil {
		return operations.PublicationStatus{}, err
	}
	stats, err := r.catalog.PendingStats(ctx)
	if err != nil {
		return operations.PublicationStatus{}, err
	}
	verifiedAt, found, err := r.remoteJournal.LastRemoteVerifiedAt(ctx)
	if err != nil {
		return operations.PublicationStatus{}, err
	}
	result := operations.PublicationStatus{
		PendingSegments:  stats.PendingSegments,
		PendingBytes:     stats.PendingBytes,
		PendingManifests: stats.PendingManifests,
		RetryCount:       stats.RetryCount,
		LastErrorClass:   r.uploader.LastErrorClass(),
		RemoteAvailable:  true,
	}
	if !stats.OldestPendingAt.IsZero() {
		result.OldestPendingAtUnixMS = uint64(stats.OldestPendingAt.UnixMilli())
	}
	if found {
		result.LastSuccessfulVerificationUnixMS = uint64(verifiedAt.UnixMilli())
	}
	if result.LastErrorClass == operations.ErrorClassRemoteTransient || result.LastErrorClass == operations.ErrorClassRemoteTimeout || result.LastErrorClass == operations.ErrorClassPermission {
		result.RemoteAvailable = false
	}
	return result, nil
}

type diskStatusReader struct {
	disk    *ingest.DiskStateMachine
	catalog *publication.Catalog
}

func (r diskStatusReader) DiskStatus(ctx context.Context) (operations.DiskStatus, error) {
	if err := ctx.Err(); err != nil {
		return operations.DiskStatus{}, err
	}
	if r.catalog != nil {
		stats, err := r.catalog.PendingStats(ctx)
		if err != nil {
			return operations.DiskStatus{}, err
		}
		r.disk.SetPendingPublication(stats.PendingSegments, stats.PendingBytes)
	}
	state := r.disk.Refresh()
	return operations.DiskStatus{
		Class:           string(state.Class),
		FreeBytes:       state.FreeBytes,
		TotalBytes:      state.TotalBytes,
		Ready:           state.Ready,
		ACKAllowed:      state.ACKAllowed,
		BlockedReason:   state.BlockedReason,
		PendingSegments: state.PendingSegments,
		PendingBytes:    state.PendingBytes,
		WorkerPriority:  state.WorkerPriority,
	}, nil
}

func newStatusService(gateway *ingest.Gateway, catalog *publication.Catalog, uploader *publication.Uploader, remoteJournal *r2.PublicationJournal, disk *ingest.DiskStateMachine) (*operations.StatusService, error) {
	return operations.NewStatusService(
		gatewayStatusReader{gateway: gateway},
		publicationStatusReader{catalog: catalog, uploader: uploader, remoteJournal: remoteJournal},
		diskStatusReader{disk: disk, catalog: catalog},
	)
}

// NewLocalStatusService builds the versioned status view for one-shot local
// commands. It deliberately has no R2 network or credential dependency; the
// remote journal is only the local durable verification ledger.
func NewLocalStatusService(gateway *ingest.Gateway, catalog *publication.Catalog, remoteJournal *r2.PublicationJournal, disk *ingest.DiskStateMachine) (*operations.StatusService, error) {
	return newStatusService(gateway, catalog, nil, remoteJournal, disk)
}

func toIngestConfig(configValue appconfig.Config) ingest.Config {
	return ingest.ConfigFromGatewayConfig(configValue.Gateway())
}

func toArchiveScope(configValue appconfig.Config) archive.ScopeConfig {
	values := configValue.Gateway()
	return archive.ScopeConfig{
		DatasetID:               values.DatasetID,
		CampaignID:              values.CampaignID,
		ProviderID:              values.ProviderID,
		StableFeedID:            values.StableFeedID,
		ExactSourceSymbol:       values.ExactSourceSymbol,
		BrokerServerFingerprint: values.BrokerServerFingerprint,
		GatewayBuildIdentity:    values.GatewayBuildIdentity,
		ProducerBuildIdentity:   values.ProducerBuildID,
		DayDefinitionID:         values.DayDefinitionID,
		SettlePolicy:            values.SettlePolicy,
		PublisherID:             values.PublisherID,
		PublisherEpoch:          values.PublisherEpoch,
		ProtocolLimits: archive.ProtocolLimits{
			MaxFrameBytes: values.MaxFrameBytes,
			MaxRecords:    values.MaxRecords,
		},
	}
}

func registerLifecycle(lifecycle fx.Lifecycle, shutdown fx.Shutdowner, recovery *retention.WALRecovery, store *wal.Store, journalStore *journal.Store, catalog *publication.Catalog, remoteJournal *r2.PublicationJournal, backend *r2.CredentialBackend, pipeline *publication.LocalPipeline, uploader *publication.Uploader, gateway *ingest.Gateway, _ *operations.StatusService) {
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return recovery.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return recovery.Stop(ctx) },
	})
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return store.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return store.Stop(ctx) },
	})
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return journalStore.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return journalStore.Stop(ctx) },
	})
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return catalog.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return catalog.Stop(ctx) },
	})
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return remoteJournal.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return remoteJournal.Stop(ctx) },
	})
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return backend.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return backend.Stop(ctx) },
	})
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return pipeline.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return pipeline.Stop(ctx) },
	})
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return uploader.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return uploader.Stop(ctx) },
	})
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return gateway.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return gateway.Stop(ctx) },
	})
	var cancelMonitor context.CancelFunc
	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			monitorCtx, cancel := context.WithCancel(context.Background())
			cancelMonitor = cancel
			gatewayErrors := gateway.Errors()
			publicationErrors := pipeline.Errors()
			uploaderErrors := uploader.Errors()
			go func() {
				select {
				case err := <-gatewayErrors:
					if err != nil {
						_ = shutdown.Shutdown()
					}
				case err := <-publicationErrors:
					if err != nil {
						_ = shutdown.Shutdown()
					}
				case err := <-uploaderErrors:
					if err != nil {
						_ = shutdown.Shutdown()
					}
				case <-monitorCtx.Done():
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			if cancelMonitor != nil {
				cancelMonitor()
			}
			return nil
		},
	})
}

func NewProductionApp(configValue appconfig.Config) (*fx.App, error) {
	application := fx.New(ProductionOptions(configValue))
	if err := application.Err(); err != nil {
		return nil, err
	}
	return application, nil
}
