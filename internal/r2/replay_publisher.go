package r2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"tick-data-platform/internal/parquet"
	"tick-data-platform/internal/protocol"
)

var (
	ErrReplayPublicationRetry     = errors.New("replay publication requires retry")
	ErrReplayPublicationIntegrity = errors.New("replay publication integrity stop")
	ErrReplayPublicationResource  = errors.New("replay publication resource stop")
	ErrReplayPublicationRounds    = errors.New("replay publication round limit reached")
)

type ReplayPublicationInput struct {
	Conversion                  parquet.ConversionSpec
	Limits                      protocol.ReplayPublicationLimits
	RawManifestBytes            []byte
	RawObjectPaths              map[string]string
	Parts                       []parquet.PartArtifact
	PartManifestBytes           [][]byte
	ReplayManifestBytes         []byte
	PreviousReplayManifestBytes []byte
	ReceiptPath                 string
}

type ReplayPublisher struct {
	layout       Layout
	remote       ReplayRemoteReadBackend
	writer       ReplayActionWriter
	executor     ReplayActionExecutor
	events       ReplayEventStore
	receiptStore ReplayReceiptStore
	lockPath     string
	hooks        replayPublicationHooks
}

type replayPublicationHooks struct {
	afterLock        func() error
	afterObservation func(round uint64, observation ReplayRemoteObservation) error
	afterExecute     func(round uint64, result ReplayActionResult) error
}

func NewReplayPublisher(
	layout Layout,
	remote ReplayRemoteReadBackend,
	writer ReplayActionWriter,
	events ReplayEventStore,
	receiptStore ReplayReceiptStore,
	lockRoot string,
) (*ReplayPublisher, error) {
	if remote == nil || writer == nil {
		return nil, fmt.Errorf("replay publisher dependencies are incomplete")
	}
	if receiptStore == nil {
		receiptStore = FileReplayReceiptStore{}
	}
	if lockRoot == "" {
		return nil, fmt.Errorf("replay publisher lock root is empty")
	}
	lockPath, err := PublicationLockPath(filepath.Clean(lockRoot), layout.Scope)
	if err != nil {
		return nil, err
	}
	executor, err := NewNarrowReplayActionExecutor(writer)
	if err != nil {
		return nil, err
	}
	return &ReplayPublisher{
		layout: layout, remote: remote, writer: writer, executor: executor,
		events: events, receiptStore: receiptStore, lockPath: lockPath,
	}, nil
}

func (p *ReplayPublisher) Publish(ctx context.Context, input ReplayPublicationInput) (ReplayVerificationReceipt, error) {
	// Static/local verification and Protocol budget feasibility occur before
	// the publication lock. No remote capability is available to the sealer.
	bundle, err := SealReplayPublicationBundle(ReplayPublicationBundleInput{
		Layout: p.layout, Conversion: input.Conversion, Limits: input.Limits,
		RawManifest: input.RawManifestBytes, RawObjectPaths: cloneStringMap(input.RawObjectPaths),
		Parts: append([]parquet.PartArtifact(nil), input.Parts...), PartManifests: cloneByteSlices(input.PartManifestBytes),
		ReplayManifest:         append([]byte(nil), input.ReplayManifestBytes...),
		PreviousReplayManifest: append([]byte(nil), input.PreviousReplayManifestBytes...),
		ReceiptPath:            input.ReceiptPath,
	})
	if err != nil {
		return ReplayVerificationReceipt{}, err
	}
	if input.ReceiptPath == "" {
		return ReplayVerificationReceipt{}, fmt.Errorf("replay receipt path is empty")
	}
	budget, err := NewReplayObservationBudget(bundle.Contract.Limits)
	if err != nil {
		return ReplayVerificationReceipt{}, err
	}

	lock, err := AcquirePublicationLock(p.lockPath)
	if err != nil {
		return ReplayVerificationReceipt{}, err
	}
	guard := &publisherObservationGuard{lock: lock, bundleDigest: bundle.Digest}
	defer func() {
		guard.released = true
		_ = lock.Close()
	}()
	if p.hooks.afterLock != nil {
		if err := p.hooks.afterLock(); err != nil {
			return ReplayVerificationReceipt{}, err
		}
	}
	if err := verifyReplayLocalSources(bundle); err != nil {
		return ReplayVerificationReceipt{}, fmt.Errorf("%w: local bundle changed after lock: %v", ErrReplayPublicationIntegrity, err)
	}
	observer, err := NewReplayBoundedObserver(p.remote, guard)
	if err != nil {
		return ReplayVerificationReceipt{}, err
	}
	_ = p.appendEvent(ctx, bundle, ReplayPublicationEvent{Kind: ReplayEventBundleRegistered, BundleDigest: bundle.Digest, ErrorClass: ReplayActionErrorNone})

	for round := uint64(1); round <= bundle.Contract.Limits.MaxPublicationRounds; round++ {
		observation, err := observer.ObserveWithBudget(ctx, bundle, budget)
		if err != nil {
			return ReplayVerificationReceipt{}, err
		}
		if p.hooks.afterObservation != nil {
			if err := p.hooks.afterObservation(round, observation); err != nil {
				return ReplayVerificationReceipt{}, err
			}
		}
		observationDigest, err := replayObservationDiagnosticDigest(observation)
		if err != nil {
			return ReplayVerificationReceipt{}, err
		}
		if observation.FinalDigest != ([32]byte{}) {
			observationDigest = observation.FinalDigest
		}
		_ = p.appendEvent(ctx, bundle, ReplayPublicationEvent{Kind: ReplayEventObservationCompleted, BundleDigest: bundle.Digest, ObservationDigest: observationDigest, ErrorClass: ReplayActionErrorNone})
		decision, err := ReconcileReplayPublication(bundle, observation)
		if err != nil {
			return ReplayVerificationReceipt{}, err
		}
		switch decision.Kind {
		case ReplayDecisionReadyForReceipt:
			if err := guard.AssertHeld(bundle); err != nil {
				return ReplayVerificationReceipt{}, err
			}
			receipt, err := BuildReplayVerificationReceipt(bundle, observation)
			if err != nil {
				return ReplayVerificationReceipt{}, err
			}
			if err := p.receiptStore.SaveNoClobber(ctx, input.ReceiptPath, receipt); err != nil {
				return ReplayVerificationReceipt{}, err
			}
			_ = p.appendEvent(ctx, bundle, ReplayPublicationEvent{
				Kind: ReplayEventReceiptSaved, BundleDigest: bundle.Digest,
				ObservationDigest: observation.FinalDigest, ActionPlanDigest: decision.ActionPlanDigest,
				ErrorClass: ReplayActionErrorNone,
			})
			return receipt, nil
		case ReplayDecisionRetry:
			return ReplayVerificationReceipt{}, fmt.Errorf("%w: %s", ErrReplayPublicationRetry, decision.ReasonCode)
		case ReplayDecisionIntegrityStop:
			return ReplayVerificationReceipt{}, fmt.Errorf("%w: %s", ErrReplayPublicationIntegrity, decision.ReasonCode)
		case ReplayDecisionResourceStop:
			return ReplayVerificationReceipt{}, fmt.Errorf("%w: %s", ErrReplayPublicationResource, decision.ReasonCode)
		case ReplayDecisionExecute:
			if len(decision.Actions) == 0 {
				return ReplayVerificationReceipt{}, fmt.Errorf("execute decision has no approved action")
			}
			// Execute exactly one action from this observation. Even when the pure
			// plan contains several absent objects, the old observation is discarded.
			action := decision.Actions[0]
			for _, kind := range []ReplayEventKind{ReplayEventActionPlanned, ReplayEventActionStarted} {
				_ = p.appendEvent(ctx, bundle, ReplayPublicationEvent{
					Kind: kind, BundleDigest: bundle.Digest, ObservationDigest: observationDigest,
					ActionPlanDigest: decision.ActionPlanDigest, ActionKind: action.Kind,
					ObjectID: action.ObjectID, ErrorClass: ReplayActionErrorNone,
				})
			}
			result, err := p.executor.Execute(ctx, bundle, action)
			if err != nil {
				return ReplayVerificationReceipt{}, err
			}
			if p.hooks.afterExecute != nil {
				if err := p.hooks.afterExecute(round, result); err != nil {
					return ReplayVerificationReceipt{}, err
				}
			}
			_ = p.appendEvent(ctx, bundle, ReplayPublicationEvent{
				Kind: ReplayEventActionFinished, BundleDigest: bundle.Digest,
				ObservationDigest: observationDigest, ActionPlanDigest: decision.ActionPlanDigest,
				ActionKind: action.Kind, ObjectID: action.ObjectID,
				ResultClass: result.Class, ErrorClass: result.ErrorClass,
			})
			switch result.Class {
			case ReplayActionCompleted:
				continue
			case ReplayActionDifferent:
				return ReplayVerificationReceipt{}, fmt.Errorf("%w: executor %s", ErrReplayPublicationIntegrity, result.ErrorClass)
			case ReplayActionUnavailable:
				return ReplayVerificationReceipt{}, fmt.Errorf("%w: executor %s", ErrReplayPublicationRetry, result.ErrorClass)
			default:
				return ReplayVerificationReceipt{}, fmt.Errorf("unknown replay executor result class %q", result.Class)
			}
		default:
			return ReplayVerificationReceipt{}, fmt.Errorf("unknown replay decision %q", decision.Kind)
		}
	}
	return ReplayVerificationReceipt{}, ErrReplayPublicationRounds
}

func verifyReplayLocalSources(bundle ReplayPublicationBundle) error {
	actions := make([]ReplayAction, 0, len(bundle.Contract.ParquetObjects)+len(bundle.Contract.PartManifests)+1)
	for _, object := range bundle.Contract.ParquetObjects {
		actions = append(actions, ReplayAction{Kind: ReplayActionUploadParquet, ObjectID: ReplayObjectID(object.ObjectID)})
	}
	for _, object := range bundle.Contract.PartManifests {
		actions = append(actions, ReplayAction{Kind: ReplayActionUploadPartManifest, ObjectID: ReplayObjectID(object.ObjectID)})
	}
	actions = append(actions, ReplayAction{Kind: ReplayActionUploadReplayManifest, ObjectID: replayManifestObjectID(bundle.Contract)})
	for _, action := range actions {
		resolved, err := resolveReplayAction(bundle, action)
		if err != nil {
			return err
		}
		_, cleanup, err := materializeReplayActionSource(resolved)
		cleanup()
		if err != nil {
			return err
		}
	}
	return nil
}

type publisherObservationGuard struct {
	lock         *PublicationLock
	bundleDigest [32]byte
	released     bool
}

func (g *publisherObservationGuard) AssertHeld(bundle ReplayPublicationBundle) error {
	if g == nil || g.released || g.lock == nil || !g.lock.Held() || bundle.Digest != g.bundleDigest {
		return fmt.Errorf("replay publication lock is not held")
	}
	return nil
}

func (p *ReplayPublisher) appendEvent(ctx context.Context, bundle ReplayPublicationBundle, event ReplayPublicationEvent) error {
	if p.events == nil {
		return nil
	}
	sealed, err := NewReplayPublicationEvent(event)
	if err != nil {
		return err
	}
	return p.events.Append(ctx, bundle, sealed)
}

func replayObservationDiagnosticDigest(observation ReplayRemoteObservation) ([32]byte, error) {
	objectValues := func(objects []ReplayObjectObservation) []any {
		copyObjects := append([]ReplayObjectObservation(nil), objects...)
		sort.Slice(copyObjects, func(i, j int) bool { return copyObjects[i].ObjectID < copyObjects[j].ObjectID })
		values := make([]any, len(copyObjects))
		for index, object := range copyObjects {
			values[index] = map[string]any{"class": string(object.Class), "object_id": string(object.ObjectID)}
		}
		return values
	}
	canonical, err := protocol.CanonicalJSON(map[string]any{
		"bundle_digest": hex.EncodeToString(observation.BundleDigest[:]), "claim": string(observation.Claim),
		"complete": observation.Complete, "observation_bytes": observation.ObservationBytes,
		"observation_requests": observation.RequestCount, "parquet_objects": objectValues(observation.ParquetObjects),
		"part_chain": string(observation.PartChain), "part_manifests": objectValues(observation.PartManifests),
		"raw_manifest": string(observation.RawManifest), "raw_objects": objectValues(observation.RawObjects),
		"replay_graph": string(observation.ReplayGraph), "replay_manifest": string(observation.ReplayManifest),
	})
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte("tick-data-platform/replay-observation-diagnostic/v1\x00"), canonical...)), nil
}

func cloneStringMap(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func cloneByteSlices(input [][]byte) [][]byte {
	result := make([][]byte, len(input))
	for index := range input {
		result[index] = append([]byte(nil), input[index]...)
	}
	return result
}
