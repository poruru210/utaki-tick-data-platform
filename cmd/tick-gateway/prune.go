package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/credentials"
	"tick-data-platform/internal/ingest"
	"tick-data-platform/internal/operations"
	"tick-data-platform/internal/protocol"
	"tick-data-platform/internal/r2"
	"tick-data-platform/internal/retention"
)

func runPruneLocal(config ingest.Config, args []string) error {
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(limits.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
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
	observer, err := retention.NewRawRetentionObserver(observerBackend, layout, remoteConfig.Date, func() uint64 {
		return clock.ObservedWallTimeUnixMS
	}, remoteConfig.GraceMS)
	if err != nil {
		return err
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
		Disk:        diskState.Class,
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
		if _, err := executor.Execute(ctx, plan, candidates); err != nil {
			return err
		}
	}
	output := map[string]any{
		"canonical_plan": string(canonical), "plan_digest": digest,
		"plan_current_wall_time_unix_ms": current,
		"dry_run":                        dryRun, "executed": execute,
		"disk_class": string(diskState.Class), "durable_wall_time_unix_ms": clock.ObservedWallTimeUnixMS,
	}
	return json.NewEncoder(os.Stdout).Encode(output)
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
