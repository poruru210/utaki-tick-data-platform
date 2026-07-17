package app

import (
	"context"
	"fmt"

	"tick-data-platform/internal/ingest"
	"tick-data-platform/internal/journal"
	"tick-data-platform/internal/retention"
	"tick-data-platform/internal/wal"
)

// LocalGatewayRuntime composes the gateway resources for one-shot local
// commands. The production resident process uses the Fx graph instead; this
// type exists so commands do not bypass the explicit constructor and
// Start/Stop contracts with an implicit lifecycle.
type LocalGatewayRuntime struct {
	recovery *retention.WALRecovery
	wal      *wal.Store
	journal  *journal.Store
	gateway  *ingest.Gateway

	recoveryStarted bool
	walStarted      bool
	journalStarted  bool
	started         bool
}

func NewLocalGatewayRuntime(config ingest.Config, watermarks ingest.DiskWatermarks) (*LocalGatewayRuntime, error) {
	defaults := ingest.DefaultDiskWatermarks()
	if watermarks.HighFreeBytes == 0 {
		watermarks.HighFreeBytes = defaults.HighFreeBytes
	}
	if watermarks.CriticalFreeBytes == 0 {
		watermarks.CriticalFreeBytes = defaults.CriticalFreeBytes
	}
	if watermarks.EmergencyFreeBytes == 0 {
		watermarks.EmergencyFreeBytes = defaults.EmergencyFreeBytes
	}
	recovery, err := retention.NewWALRecovery(config.WALRoot)
	if err != nil {
		return nil, err
	}
	store, err := wal.NewStore(config.WALRoot, config.GatewayInstanceID, recovery)
	if err != nil {
		return nil, err
	}
	journalStore, err := journal.NewStore(config.JournalPath, config.GatewayInstanceID, config.InitialFromMSC, config.InitialBatchCount)
	if err != nil {
		return nil, err
	}
	disk, err := ingest.NewDiskStateMachine(config.WALRoot, watermarks, ingest.OSDiskUsageProvider{})
	if err != nil {
		return nil, err
	}
	gateway, err := ingest.NewGateway(config, store, journalStore, disk)
	if err != nil {
		return nil, err
	}
	return &LocalGatewayRuntime{
		recovery: recovery,
		wal:      store,
		journal:  journalStore,
		gateway:  gateway,
	}, nil
}

func (r *LocalGatewayRuntime) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || r.gateway == nil {
		return fmt.Errorf("local gateway runtime is nil")
	}
	if r.started {
		return fmt.Errorf("local gateway runtime is already started")
	}
	if err := r.recovery.Start(ctx); err != nil {
		return err
	}
	r.recoveryStarted = true
	if err := r.wal.Start(ctx); err != nil {
		_ = r.recovery.Stop(context.Background())
		r.recoveryStarted = false
		return err
	}
	r.walStarted = true
	if err := r.journal.Start(ctx); err != nil {
		_ = r.wal.Stop(context.Background())
		_ = r.recovery.Stop(context.Background())
		r.walStarted = false
		r.recoveryStarted = false
		return err
	}
	r.journalStarted = true
	if err := r.gateway.Recover(ctx); err != nil {
		_ = r.journal.Stop(context.Background())
		_ = r.wal.Stop(context.Background())
		_ = r.recovery.Stop(context.Background())
		r.journalStarted = false
		r.walStarted = false
		r.recoveryStarted = false
		return err
	}
	r.started = true
	return nil
}

func (r *LocalGatewayRuntime) Gateway() *ingest.Gateway {
	if r == nil {
		return nil
	}
	return r.gateway
}

func (r *LocalGatewayRuntime) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil {
		return nil
	}
	var first error
	if r.gateway != nil {
		if err := r.gateway.Stop(ctx); err != nil && first == nil {
			first = err
		}
	}
	if r.journalStarted {
		if err := r.journal.Stop(ctx); err != nil && first == nil {
			first = err
		}
		r.journalStarted = false
	}
	if r.walStarted {
		if err := r.wal.Stop(ctx); err != nil && first == nil {
			first = err
		}
		r.walStarted = false
	}
	if r.recoveryStarted {
		if err := r.recovery.Stop(ctx); err != nil && first == nil {
			first = err
		}
		r.recoveryStarted = false
	}
	r.started = false
	return first
}
