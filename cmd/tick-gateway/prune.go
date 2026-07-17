package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"tick-data-platform/internal/archive"
	appconfig "tick-data-platform/internal/config"
	"tick-data-platform/internal/credentials"
	"tick-data-platform/internal/ingest"
	"tick-data-platform/internal/operations"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/publication"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/retention"
)

func runPruneLocal(configValue appconfig.Config, args []string) error {
	return runPruneLocalWithObserver(configValue, args, nil)
}

// runPruneLocalWithObserver keeps the command's complete inventory, binding,
// planning, and execution path intact while allowing a bounded fake observer
// in command-level tests. Production always passes nil and constructs the
// credential-bound read-only observer below.
func runPruneLocalWithObserver(configValue appconfig.Config, args []string, observerOverride retention.ReadOnlyRemoteObserver) error {
	if err := configValue.ValidateForRun(); err != nil {
		return err
	}
	config := ingest.ConfigFromGatewayConfig(configValue.Gateway())
	dryRun := hasFlag(args, "--dry-run")
	execute := hasFlag(args, "--execute")
	if dryRun == execute {
		return fmt.Errorf("exactly one of --dry-run or --execute is required")
	}
	retentionPath, err := flagValue(args, "--retention-config")
	if err != nil {
		return fmt.Errorf("prune-local requires --retention-config")
	}
	remoteConfig, err := retention.LoadConfig(retentionPath)
	if err != nil {
		return err
	}
	limits := operations.DefaultResourceLimits
	clock, err := retention.LoadLatestWallClock(config.WALRoot)
	if err != nil {
		return fmt.Errorf("load durable retention clock: %w", err)
	}
	current, err := prunePlanWallTime(args, execute, clock.ObservedWallTimeUnixMS)
	if err != nil {
		return err
	}
	artifacts, err := retention.InventoryWALSegments(config.WALRoot, limits.MaxPruneCandidates, limits.MaxProofBytes)
	if err != nil {
		return err
	}
	if config.RawOutboxRoot != "" {
		outboxArtifacts, inventoryErr := retention.InventoryFiles(config.RawOutboxRoot, retention.ArtifactRawOutbox, limits.MaxPruneCandidates, limits.MaxProofBytes)
		if inventoryErr != nil {
			return inventoryErr
		}
		artifacts = append(artifacts, outboxArtifacts...)
	}
	expectedStart := uint64(1)
	if checkpoint, checkpointErr := retention.LoadLatestCheckpoint(config.WALRoot); checkpointErr == nil {
		if checkpoint.EndSequence == ^uint64(0) {
			return fmt.Errorf("WAL sequence space is terminally pruned")
		}
		expectedStart = checkpoint.EndSequence + 1
	} else if !errors.Is(checkpointErr, retention.ErrCheckpointAbsent) {
		return checkpointErr
	}

	scope, err := remoteConfig.Scope()
	if err != nil {
		return err
	}
	if err := validatePruneScopeBinding(config, scope); err != nil {
		return err
	}
	layout, err := r2.NewLayout(remoteConfig.ImmutableRoot, scope)
	if err != nil {
		return err
	}
	remoteJournal, err := r2.NewPublicationJournal(configValue.Publication.RemoteJournalPath)
	if err != nil {
		return fmt.Errorf("construct publication journal: %w", err)
	}
	if err := remoteJournal.Start(context.Background()); err != nil {
		return fmt.Errorf("start publication journal: %w", err)
	}
	defer remoteJournal.Stop(context.Background())
	catalog, err := publication.NewCatalog(configValue.Publication.CatalogPath)
	if err != nil {
		return fmt.Errorf("construct publication catalog: %w", err)
	}
	if err := catalog.Start(context.Background()); err != nil {
		_ = remoteJournal.Stop(context.Background())
		return fmt.Errorf("start publication catalog: %w", err)
	}
	defer catalog.Stop(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(limits.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	var observer retention.ReadOnlyRemoteObserver = observerOverride
	if observer == nil {
		provider, err := credentials.NewFileProvider(remoteConfig.CredentialFileConfig())
		if err != nil {
			return err
		}
		backend, err := r2.NewS3ReadBackendWithProvider(ctx, r2.S3ReadBackendConfig{
			Bucket: remoteConfig.Bucket, Endpoint: remoteConfig.Endpoint, Region: remoteConfig.Region,
			MaxMetadataBytes: int64(limits.MaxProofBytes),
		}, provider)
		if err != nil {
			return err
		}
		observerBackend, err := r2.NewReplayRemoteReadAdapterFromReadBackend(backend)
		if err != nil {
			return err
		}
		observer, err = retention.NewRawRetentionObserver(observerBackend, layout, remoteConfig.Date, func() uint64 {
			return clock.ObservedWallTimeUnixMS
		}, remoteConfig.GraceMS)
		if err != nil {
			return err
		}
	}
	var recovered []retention.CandidateFact
	if config.RawOutboxRoot != "" {
		recovered, err = retention.InventoryPruneCompletions(config.RawOutboxRoot, retention.ArtifactRawOutbox, limits.MaxPruneCandidates, limits.MaxProofBytes)
		if err != nil {
			return err
		}
	}
	recoveryIDs := make(map[string]struct{}, len(recovered))
	for _, candidate := range recovered {
		if err := retention.ValidateRawCompletionScope(candidate, layout, remoteConfig.Date); err != nil {
			return fmt.Errorf("retention completion scope: %w", err)
		}
		id, idErr := candidate.Artifact.StableID()
		if idErr != nil {
			return fmt.Errorf("retention completion candidate identity: %w", idErr)
		}
		recoveryIDs[id] = struct{}{}
	}
	observedArtifacts := make([]retention.LocalArtifact, 0, len(artifacts))
	for _, artifact := range artifacts {
		id, idErr := artifact.StableID()
		if idErr != nil {
			return fmt.Errorf("retention artifact identity: %w", idErr)
		}
		if _, recovered := recoveryIDs[id]; recovered {
			continue
		}
		observedArtifacts = append(observedArtifacts, artifact)
	}
	candidates, err := retention.ObserveCandidates(ctx, observer, observedArtifacts, limits)
	if err != nil {
		return err
	}
	if uint64(len(candidates))+uint64(len(recovered)) > limits.MaxPruneCandidates {
		return fmt.Errorf("retention candidates exceed configured limit")
	}
	candidates = append(candidates, recovered...)
	if err := bindPruneRemoteVerification(candidates, config, layout, remoteJournal); err != nil {
		return err
	}
	disk, err := ingest.NewDiskStateMachine(config.WALRoot, ingest.DiskWatermarks{
		HighFreeBytes: config.DiskHighFreeBytes, CriticalFreeBytes: config.DiskCriticalFreeBytes,
		EmergencyFreeBytes: config.DiskEmergencyFreeBytes,
	}, ingest.OSDiskUsageProvider{})
	if err != nil {
		return err
	}
	diskState := disk.Refresh()
	plan, err := retention.BuildPrunePlan(retention.PlannerInput{
		Candidates: candidates, CurrentWallTimeUnixMS: current,
		DurableWallTimeUnixMS: clock.ObservedWallTimeUnixMS, WALExpectedStart: expectedStart,
		GraceMS: remoteConfig.GraceMS, MaxCandidates: limits.MaxPruneCandidates,
		ProofLimits: retention.ProofLimits{MaxProofObjects: limits.MaxProofObjects, MaxProofBytes: limits.MaxProofBytes, MaxManifestNodes: limits.MaxManifestNodes},
		Disk:        diskState.Class, RequireRemoteVerified: true,
	})
	if err != nil {
		return err
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return err
	}
	digest := hex.EncodeToString(plan.PlanDigest[:])
	if execute {
		provided, err := flagValue(args, "--plan-digest")
		if err != nil {
			return fmt.Errorf("--execute requires --plan-digest")
		}
		providedDigest, err := parsePlanDigest(provided)
		if err != nil || providedDigest != plan.PlanDigest {
			return fmt.Errorf("plan digest mismatch: current=%s", digest)
		}
		if len(plan.Actions) == 0 {
			return fmt.Errorf("prune plan has no executable action")
		}
		executor, err := retention.NewPruneExecutor(retention.PruneRoots{WALRoot: config.WALRoot, RawOutboxRoot: config.RawOutboxRoot}, nil)
		if err != nil {
			return err
		}
		report, err := executor.Execute(ctx, plan, candidates)
		if err != nil {
			return err
		}
		if err := recordPruneLocalCompletions(ctx, config, report, candidates, catalog); err != nil {
			return err
		}
	}
	output := map[string]any{
		"canonical_plan": string(canonical), "plan_digest": digest,
		"plan_current_wall_time_unix_ms": current,
		"dry_run":                        dryRun, "executed": execute,
		"disk_class": string(diskState.Class), "durable_wall_time_unix_ms": clock.ObservedWallTimeUnixMS,
		"remote_verified_required": true,
	}
	return json.NewEncoder(os.Stdout).Encode(output)
}

type pruneRemoteVerificationReader interface {
	FindRemoteVerifiedObject(string, [32]byte, uint64) (r2.PublicationObjectStateRecord, bool, error)
	FindRemoteVerifiedObjectAtPath(string, string, [32]byte, uint64) (r2.PublicationObjectStateRecord, bool, error)
}

func bindPruneRemoteVerification(candidates []retention.CandidateFact, config ingest.Config, layout r2.Layout, journal pruneRemoteVerificationReader) error {
	if journal == nil {
		return fmt.Errorf("prune remote verification reader is nil")
	}
	for index := range candidates {
		remoteKey, err := layout.RemoteKey(archive.RawWALObjectKey(candidates[index].Artifact.ContentSHA256))
		if err != nil {
			return err
		}
		if candidates[index].Artifact.Kind == retention.ArtifactRawOutbox {
			localPath := filepath.Join(config.RawOutboxRoot, filepath.FromSlash(candidates[index].Artifact.TrustedPath))
			_, candidates[index].RemoteVerified, err = journal.FindRemoteVerifiedObjectAtPath(remoteKey, localPath, candidates[index].Artifact.ContentSHA256, candidates[index].Artifact.Bytes)
		} else {
			_, candidates[index].RemoteVerified, err = journal.FindRemoteVerifiedObject(remoteKey, candidates[index].Artifact.ContentSHA256, candidates[index].Artifact.Bytes)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type pruneLocalPrunedRecorder interface {
	MarkSegmentLocalPruned(context.Context, string, string, [32]byte, uint64) error
}

func recordPruneLocalCompletions(ctx context.Context, config ingest.Config, report retention.PruneExecutionReport, candidates []retention.CandidateFact, catalog pruneLocalPrunedRecorder) error {
	if catalog == nil {
		return fmt.Errorf("prune local completion recorder is nil")
	}
	byID := make(map[string]retention.CandidateFact, len(candidates))
	for _, candidate := range candidates {
		id, err := candidate.Artifact.StableID()
		if err != nil {
			return err
		}
		byID[id] = candidate
	}
	for _, id := range report.Completed {
		candidate, found := byID[id]
		if !found {
			return fmt.Errorf("prune completion candidate %q is missing", id)
		}
		if candidate.Artifact.Kind != retention.ArtifactRawOutbox {
			continue
		}
		localPath := filepath.Join(config.RawOutboxRoot, filepath.FromSlash(candidate.Artifact.TrustedPath))
		if err := catalog.MarkSegmentLocalPruned(ctx, candidate.Artifact.TrustedPath, localPath, candidate.Artifact.ContentSHA256, candidate.Artifact.Bytes); err != nil {
			return err
		}
	}
	return nil
}

func parsePlanDigest(value string) ([32]byte, error) {
	return protocol.ParseHashHex(value)
}

func prunePlanWallTime(args []string, execute bool, durable uint64) (uint64, error) {
	if execute {
		value, err := flagValue(args, "--plan-time-unix-ms")
		if err != nil {
			return 0, fmt.Errorf("--execute requires --plan-time-unix-ms from the dry-run output")
		}
		current, err := strconv.ParseUint(value, 10, 64)
		if err != nil || current == 0 {
			return 0, fmt.Errorf("--plan-time-unix-ms must be a positive unsigned integer")
		}
		return current, nil
	}
	current := uint64(time.Now().UnixMilli())
	if current == 0 || durable == 0 {
		return 0, fmt.Errorf("current wall clock is unavailable")
	}
	return current, nil
}

func validatePruneScopeBinding(config ingest.Config, scope archive.ScopeConfig) error {
	if config.DatasetID == "" || config.CampaignID == "" || config.ProviderID == "" || config.StableFeedID == "" || config.ExactSourceSymbol == "" || config.BrokerServerFingerprint == "" || config.GatewayBuildIdentity == "" || config.ProducerBuildID == "" || config.DayDefinitionID == "" || config.SettlePolicy == "" || config.PublisherID == "" || config.PublisherEpoch == 0 {
		return fmt.Errorf("gateway config does not declare the complete retention scope")
	}
	if config.DatasetID != scope.DatasetID || config.CampaignID != scope.CampaignID || config.ProviderID != scope.ProviderID || config.StableFeedID != scope.StableFeedID || config.ExactSourceSymbol != scope.ExactSourceSymbol || config.BrokerServerFingerprint != scope.BrokerServerFingerprint || config.GatewayBuildIdentity != scope.GatewayBuildIdentity || config.ProducerBuildID != scope.ProducerBuildIdentity || config.DayDefinitionID != scope.DayDefinitionID || config.SettlePolicy != scope.SettlePolicy || config.PublisherID != scope.PublisherID || config.PublisherEpoch != scope.PublisherEpoch || scope.ProtocolLimits.MaxFrameBytes != config.MaxFrameBytes || scope.ProtocolLimits.MaxRecords != config.MaxRecords || scope.ProtocolLimits.MaxStringBytes != protocol.MaxStringBytes {
		return fmt.Errorf("gateway and retention scopes do not match")
	}
	return nil
}
