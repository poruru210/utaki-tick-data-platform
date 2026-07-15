package r2

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"tick-data-platform/internal/protocol"
)

const replayActionPlanDomain = "tick-data-platform/replay-action-plan/v1\x00"

type ObservationClass string

const (
	ObservationAbsent      ObservationClass = "Absent"
	ObservationExact       ObservationClass = "Exact"
	ObservationDifferent   ObservationClass = "Different"
	ObservationAmbiguous   ObservationClass = "Ambiguous"
	ObservationOversized   ObservationClass = "Oversized"
	ObservationUnavailable ObservationClass = "Unavailable"
)

type ReplayObjectObservation struct {
	ObjectID ReplayObjectID
	Class    ObservationClass
}

// ReplayRemoteObservation is the pure reconciler input. R3's observer will
// populate it from bounded remote reads; it intentionally has no local paths,
// backend handles, journal state, events, timestamps, or credentials.
type ReplayRemoteObservation struct {
	BundleDigest     [32]byte
	Claim            ObservationClass
	RawManifest      ObservationClass
	RawObjects       []ReplayObjectObservation
	ParquetObjects   []ReplayObjectObservation
	PartManifests    []ReplayObjectObservation
	PartChain        ObservationClass
	ReplayManifest   ObservationClass
	ReplayGraph      ObservationClass
	RequestCount     uint64
	ObservationBytes uint64
	Complete         bool
	FinalObservation *protocol.ReplayFinalObservation
	FinalDigest      [32]byte
}

type ReplayDecisionKind string

const (
	ReplayDecisionExecute         ReplayDecisionKind = "Execute"
	ReplayDecisionRetry           ReplayDecisionKind = "Retry"
	ReplayDecisionIntegrityStop   ReplayDecisionKind = "IntegrityStop"
	ReplayDecisionResourceStop    ReplayDecisionKind = "ResourceStop"
	ReplayDecisionReadyForReceipt ReplayDecisionKind = "ReadyForReceipt"
)

type ReplayActionKind string

const (
	ReplayActionUploadParquet        ReplayActionKind = "UploadParquet"
	ReplayActionUploadPartManifest   ReplayActionKind = "UploadPartManifest"
	ReplayActionUploadReplayManifest ReplayActionKind = "UploadReplayManifest"
)

type ReplayAction struct {
	Kind     ReplayActionKind
	ObjectID ReplayObjectID
}

type ReplayReconcileDecision struct {
	Kind             ReplayDecisionKind
	Actions          []ReplayAction
	ActionPlanDigest [32]byte
	StopClass        ObservationClass
	ReasonCode       string
}

// ReconcileReplayPublication is a pure policy function. It validates the
// sealed contract and then stops at the first unsatisfied publication barrier.
func ReconcileReplayPublication(bundle ReplayPublicationBundle, observation ReplayRemoteObservation) (ReplayReconcileDecision, error) {
	canonical, err := protocol.ReplayPublicationBundleCanonicalJSON(bundle.Contract)
	if err != nil {
		return ReplayReconcileDecision{}, err
	}
	digest, err := protocol.ReplayPublicationBundleDigest(bundle.Contract)
	if err != nil {
		return ReplayReconcileDecision{}, err
	}
	if !bytes.Equal(canonical, bundle.CanonicalBytes) || digest != bundle.Digest {
		return ReplayReconcileDecision{}, fmt.Errorf("sealed replay bundle identity is inconsistent")
	}
	if observation.BundleDigest != bundle.Digest {
		return makeReplayDecision(bundle.Digest, ReplayDecisionIntegrityStop, ObservationDifferent, "bundle_digest_mismatch", nil)
	}
	if observation.RequestCount > bundle.Contract.Limits.MaxObservationRequests || observation.ObservationBytes > bundle.Contract.Limits.MaxObservationBytes {
		return makeReplayDecision(bundle.Digest, ReplayDecisionResourceStop, ObservationOversized, "observation_budget_exhausted", nil)
	}

	if decision, done, err := evaluateSingleBarrier(bundle.Digest, "claim", observation.Claim, true, "", ""); done || err != nil {
		return decision, err
	}
	if decision, done, err := evaluateSingleBarrier(bundle.Digest, "raw_manifest", observation.RawManifest, true, "", ""); done || err != nil {
		return decision, err
	}
	rawIDs := make([]ReplayObjectID, len(bundle.Contract.RawObjects))
	for index, object := range bundle.Contract.RawObjects {
		rawIDs[index] = replayRawObjectID(object)
	}
	if decision, done, err := evaluateObjectBarrier(bundle.Digest, "raw_object", rawIDs, observation.RawObjects, true, ""); done || err != nil {
		return decision, err
	}

	parquetIDs := make([]ReplayObjectID, len(bundle.Contract.ParquetObjects))
	for index, object := range bundle.Contract.ParquetObjects {
		parquetIDs[index] = ReplayObjectID(object.ObjectID)
	}
	if decision, done, err := evaluateObjectBarrier(bundle.Digest, "parquet", parquetIDs, observation.ParquetObjects, false, ReplayActionUploadParquet); done || err != nil {
		return decision, err
	}
	partIDs := make([]ReplayObjectID, len(bundle.Contract.PartManifests))
	for index, object := range bundle.Contract.PartManifests {
		partIDs[index] = ReplayObjectID(object.ObjectID)
	}
	if decision, done, err := evaluateObjectBarrier(bundle.Digest, "part_manifest", partIDs, observation.PartManifests, false, ReplayActionUploadPartManifest); done || err != nil {
		return decision, err
	}
	if decision, done, err := evaluateSingleBarrier(bundle.Digest, "part_chain", observation.PartChain, true, "", ""); done || err != nil {
		return decision, err
	}
	if decision, done, err := evaluateSingleBarrier(
		bundle.Digest, "replay_manifest", observation.ReplayManifest, false,
		ReplayActionUploadReplayManifest, replayManifestObjectID(bundle.Contract),
	); done || err != nil {
		return decision, err
	}
	if decision, done, err := evaluateSingleBarrier(bundle.Digest, "replay_graph", observation.ReplayGraph, true, "", ""); done || err != nil {
		return decision, err
	}
	if !observation.Complete {
		return makeReplayDecision(bundle.Digest, ReplayDecisionIntegrityStop, ObservationAmbiguous, "final_observation_incomplete", nil)
	}
	return makeReplayDecision(bundle.Digest, ReplayDecisionReadyForReceipt, ObservationExact, "ready_for_receipt", nil)
}

func evaluateSingleBarrier(
	bundleDigest [32]byte,
	barrier string,
	class ObservationClass,
	dependency bool,
	actionKind ReplayActionKind,
	objectID ReplayObjectID,
) (ReplayReconcileDecision, bool, error) {
	if err := validateObservationClass(class); err != nil {
		return ReplayReconcileDecision{}, false, fmt.Errorf("%s: %w", barrier, err)
	}
	switch class {
	case ObservationExact:
		return ReplayReconcileDecision{}, false, nil
	case ObservationUnavailable:
		decision, err := makeReplayDecision(bundleDigest, ReplayDecisionRetry, class, barrier+"_unavailable", nil)
		return decision, true, err
	case ObservationOversized:
		decision, err := makeReplayDecision(bundleDigest, ReplayDecisionResourceStop, class, barrier+"_oversized", nil)
		return decision, true, err
	case ObservationDifferent, ObservationAmbiguous:
		decision, err := makeReplayDecision(bundleDigest, ReplayDecisionIntegrityStop, class, barrier+"_not_exact", nil)
		return decision, true, err
	case ObservationAbsent:
		if dependency || actionKind == "" || objectID == "" {
			decision, err := makeReplayDecision(bundleDigest, ReplayDecisionIntegrityStop, ObservationAbsent, barrier+"_absent", nil)
			return decision, true, err
		}
		decision, err := makeReplayDecision(bundleDigest, ReplayDecisionExecute, ObservationAbsent, barrier+"_upload", []ReplayAction{{Kind: actionKind, ObjectID: objectID}})
		return decision, true, err
	default:
		panic("validated observation class was not handled")
	}
}

func evaluateObjectBarrier(
	bundleDigest [32]byte,
	barrier string,
	expected []ReplayObjectID,
	observed []ReplayObjectObservation,
	dependency bool,
	actionKind ReplayActionKind,
) (ReplayReconcileDecision, bool, error) {
	want := make(map[ReplayObjectID]struct{}, len(expected))
	for _, objectID := range expected {
		if objectID == "" {
			return ReplayReconcileDecision{}, false, fmt.Errorf("%s bundle object ID is empty", barrier)
		}
		want[objectID] = struct{}{}
	}
	classes := make(map[ReplayObjectID]ObservationClass, len(observed))
	for _, item := range observed {
		if err := validateObservationClass(item.Class); err != nil {
			return ReplayReconcileDecision{}, false, fmt.Errorf("%s/%s: %w", barrier, item.ObjectID, err)
		}
		if _, ok := want[item.ObjectID]; !ok {
			decision, err := makeReplayDecision(bundleDigest, ReplayDecisionIntegrityStop, ObservationAmbiguous, barrier+"_candidate_or_orphan", nil)
			return decision, true, err
		}
		if _, duplicate := classes[item.ObjectID]; duplicate {
			decision, err := makeReplayDecision(bundleDigest, ReplayDecisionIntegrityStop, ObservationAmbiguous, barrier+"_duplicate_observation", nil)
			return decision, true, err
		}
		classes[item.ObjectID] = item.Class
	}
	if len(classes) != len(want) {
		decision, err := makeReplayDecision(bundleDigest, ReplayDecisionIntegrityStop, ObservationAmbiguous, barrier+"_observation_incomplete", nil)
		return decision, true, err
	}

	ordered := append([]ReplayObjectID(nil), expected...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, class := range []ObservationClass{ObservationOversized, ObservationDifferent, ObservationAmbiguous, ObservationUnavailable} {
		for _, objectID := range ordered {
			if classes[objectID] != class {
				continue
			}
			kind := ReplayDecisionIntegrityStop
			if class == ObservationOversized {
				kind = ReplayDecisionResourceStop
			} else if class == ObservationUnavailable {
				kind = ReplayDecisionRetry
			}
			decision, err := makeReplayDecision(bundleDigest, kind, class, barrier+"_"+string(class), nil)
			return decision, true, err
		}
	}
	actions := make([]ReplayAction, 0)
	for _, objectID := range ordered {
		if classes[objectID] != ObservationAbsent {
			continue
		}
		if dependency || actionKind == "" {
			decision, err := makeReplayDecision(bundleDigest, ReplayDecisionIntegrityStop, ObservationAbsent, barrier+"_absent", nil)
			return decision, true, err
		}
		actions = append(actions, ReplayAction{Kind: actionKind, ObjectID: objectID})
	}
	if len(actions) != 0 {
		decision, err := makeReplayDecision(bundleDigest, ReplayDecisionExecute, ObservationAbsent, barrier+"_upload", actions)
		return decision, true, err
	}
	return ReplayReconcileDecision{}, false, nil
}

func validateObservationClass(class ObservationClass) error {
	switch class {
	case ObservationAbsent, ObservationExact, ObservationDifferent, ObservationAmbiguous, ObservationOversized, ObservationUnavailable:
		return nil
	default:
		return fmt.Errorf("unknown observation class %q", class)
	}
}

func makeReplayDecision(
	bundleDigest [32]byte,
	kind ReplayDecisionKind,
	stopClass ObservationClass,
	reason string,
	actions []ReplayAction,
) (ReplayReconcileDecision, error) {
	values := make([]any, len(actions))
	for index, action := range actions {
		if action.ObjectID == "" {
			return ReplayReconcileDecision{}, fmt.Errorf("replay action object ID is empty")
		}
		values[index] = map[string]any{"kind": string(action.Kind), "object_id": string(action.ObjectID)}
	}
	canonical, err := protocol.CanonicalJSON(map[string]any{
		"actions": values, "bundle_digest": hex.EncodeToString(bundleDigest[:]),
		"decision_kind": string(kind), "reason_code": reason, "stop_class": string(stopClass),
	})
	if err != nil {
		return ReplayReconcileDecision{}, err
	}
	digest := sha256.Sum256(append([]byte(replayActionPlanDomain), canonical...))
	return ReplayReconcileDecision{
		Kind: kind, Actions: append([]ReplayAction(nil), actions...), ActionPlanDigest: digest,
		StopClass: stopClass, ReasonCode: reason,
	}, nil
}
