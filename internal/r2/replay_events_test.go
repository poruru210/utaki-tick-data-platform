package r2

import (
	"bytes"
	"context"
	"testing"
)

func replayEventDigest(seed byte) [32]byte {
	var digest [32]byte
	for index := range digest {
		digest[index] = seed
	}
	return digest
}

func sealedReplayEvent(t *testing.T, event ReplayPublicationEvent) ReplayPublicationEvent {
	t.Helper()
	sealed, err := NewReplayPublicationEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

func replayActionEventFixture(t *testing.T) (ReplayPublicationBundle, ReplayAction, [32]byte, [32]byte) {
	t.Helper()
	bundle := sealedReplayExecutorBundle(t)
	action := ReplayAction{Kind: ReplayActionUploadParquet, ObjectID: ReplayObjectID(bundle.Contract.ParquetObjects[0].ObjectID)}
	return bundle, action, replayEventDigest(0x31), replayEventDigest(0x42)
}

func TestReplayEventMissingDoesNotAuthorizeOrBlockApprovedExecutor(t *testing.T) {
	bundle, action, _, _ := replayActionEventFixture(t)
	store := NewReplayDiagnosticEventStore()
	events, err := store.Load(context.Background(), bundle)
	if err != nil || len(events) != 0 {
		t.Fatalf("empty diagnostic store: events=%v err=%v", events, err)
	}
	tool := &narrowReplayTool{}
	executor, _ := NewNarrowReplayActionExecutor(tool)
	result, err := executor.Execute(context.Background(), bundle, action)
	if err != nil || result.Class != ReplayActionCompleted {
		t.Fatalf("approved action depended on missing event: result=%+v err=%v", result, err)
	}
}

func TestReplayEventSameDuplicateIsIdempotent(t *testing.T) {
	bundle, _, observation, _ := replayActionEventFixture(t)
	event := sealedReplayEvent(t, ReplayPublicationEvent{Kind: ReplayEventObservationCompleted, BundleDigest: bundle.Digest, ObservationDigest: observation, ErrorClass: ReplayActionErrorNone})
	store := NewReplayDiagnosticEventStore()
	if err := store.Append(context.Background(), bundle, event); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(context.Background(), bundle, event); err != nil {
		t.Fatal(err)
	}
	events, err := store.Load(context.Background(), bundle)
	if err != nil || len(events) != 1 {
		t.Fatalf("duplicate append: events=%d err=%v", len(events), err)
	}
}

func TestReplayEventConflictingDuplicateFailsClosed(t *testing.T) {
	bundle, _, observation, _ := replayActionEventFixture(t)
	event := sealedReplayEvent(t, ReplayPublicationEvent{Kind: ReplayEventObservationCompleted, BundleDigest: bundle.Digest, ObservationDigest: observation, ErrorClass: ReplayActionErrorNone})
	store := NewReplayDiagnosticEventStore()
	if err := store.Append(context.Background(), bundle, event); err != nil {
		t.Fatal(err)
	}
	conflict := event
	conflict.ObservationDigest[0] ^= 0xff
	if err := store.Append(context.Background(), bundle, conflict); err == nil {
		t.Fatal("same event ID with different content was accepted")
	}
}

func TestReplayEventRejectsUnknownKindActionAndObject(t *testing.T) {
	bundle, action, observation, plan := replayActionEventFixture(t)
	tests := []ReplayPublicationEvent{
		{Kind: ReplayEventKind("StageAdvanced"), BundleDigest: bundle.Digest, ErrorClass: ReplayActionErrorNone},
		{Kind: ReplayEventActionPlanned, BundleDigest: bundle.Digest, ObservationDigest: observation, ActionPlanDigest: plan, ActionKind: ReplayActionKind("Delete"), ObjectID: action.ObjectID, ErrorClass: ReplayActionErrorNone},
		{Kind: ReplayEventActionPlanned, BundleDigest: bundle.Digest, ObservationDigest: observation, ActionPlanDigest: plan, ActionKind: action.Kind, ObjectID: ReplayObjectID("unknown"), ErrorClass: ReplayActionErrorNone},
	}
	store := NewReplayDiagnosticEventStore()
	for _, value := range tests {
		event, err := NewReplayPublicationEvent(value)
		if err == nil {
			err = store.Append(context.Background(), bundle, event)
		}
		if err == nil {
			t.Fatalf("unknown event was accepted: %+v", value)
		}
	}
}

func TestReplayEventRejectsBundleObservationAndPlanMismatch(t *testing.T) {
	bundle, action, observation, plan := replayActionEventFixture(t)
	store := NewReplayDiagnosticEventStore()
	planned := sealedReplayEvent(t, ReplayPublicationEvent{Kind: ReplayEventActionPlanned, BundleDigest: bundle.Digest, ObservationDigest: observation, ActionPlanDigest: plan, ActionKind: action.Kind, ObjectID: action.ObjectID, ErrorClass: ReplayActionErrorNone})
	if err := store.Append(context.Background(), bundle, planned); err != nil {
		t.Fatal(err)
	}

	wrongBundle := planned
	wrongBundle.BundleDigest[0] ^= 0xff
	wrongBundle = sealedReplayEvent(t, ReplayPublicationEvent{Kind: wrongBundle.Kind, BundleDigest: wrongBundle.BundleDigest, ObservationDigest: wrongBundle.ObservationDigest, ActionPlanDigest: wrongBundle.ActionPlanDigest, ActionKind: wrongBundle.ActionKind, ObjectID: wrongBundle.ObjectID, ErrorClass: ReplayActionErrorNone})
	if err := store.Append(context.Background(), bundle, wrongBundle); err == nil {
		t.Fatal("bundle mismatch was accepted")
	}

	conflictingObservation := observation
	conflictingObservation[0] ^= 0xff
	started := sealedReplayEvent(t, ReplayPublicationEvent{Kind: ReplayEventActionStarted, BundleDigest: bundle.Digest, ObservationDigest: conflictingObservation, ActionPlanDigest: plan, ActionKind: action.Kind, ObjectID: action.ObjectID, ErrorClass: ReplayActionErrorNone})
	if err := store.Append(context.Background(), bundle, started); err == nil {
		t.Fatal("observation lineage mismatch was accepted")
	}

	conflictingPlan := plan
	conflictingPlan[0] ^= 0xff
	started = sealedReplayEvent(t, ReplayPublicationEvent{Kind: ReplayEventActionStarted, BundleDigest: bundle.Digest, ObservationDigest: observation, ActionPlanDigest: conflictingPlan, ActionKind: action.Kind, ObjectID: action.ObjectID, ErrorClass: ReplayActionErrorNone})
	if err := store.Append(context.Background(), bundle, started); err == nil {
		t.Fatal("action plan lineage mismatch was accepted")
	}
}

func TestReplayEventCanonicalBytesExcludeSecretEndpointAndLocalPath(t *testing.T) {
	bundle, action, observation, plan := replayActionEventFixture(t)
	event := sealedReplayEvent(t, ReplayPublicationEvent{Kind: ReplayEventActionFinished, BundleDigest: bundle.Digest, ObservationDigest: observation, ActionPlanDigest: plan, ActionKind: action.Kind, ObjectID: action.ObjectID, ResultClass: ReplayActionUnavailable, ErrorClass: ReplayActionErrorUnknownOutcome})
	canonical, err := replayPublicationEventCanonical(event)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{[]byte("secret"), []byte("endpoint"), []byte(bundle.LocalSources.Artifacts[action.ObjectID].Path)} {
		if len(forbidden) != 0 && bytes.Contains(canonical, forbidden) {
			t.Fatalf("event leaked forbidden value %q", forbidden)
		}
	}
}

func TestReplayEventAllKindsHaveStableCanonicalIDs(t *testing.T) {
	bundle, action, observation, plan := replayActionEventFixture(t)
	events := []ReplayPublicationEvent{
		{Kind: ReplayEventBundleRegistered, BundleDigest: bundle.Digest, ErrorClass: ReplayActionErrorNone},
		{Kind: ReplayEventObservationCompleted, BundleDigest: bundle.Digest, ObservationDigest: observation, ErrorClass: ReplayActionErrorNone},
		{Kind: ReplayEventActionPlanned, BundleDigest: bundle.Digest, ObservationDigest: observation, ActionPlanDigest: plan, ActionKind: action.Kind, ObjectID: action.ObjectID, ErrorClass: ReplayActionErrorNone},
		{Kind: ReplayEventActionStarted, BundleDigest: bundle.Digest, ObservationDigest: observation, ActionPlanDigest: plan, ActionKind: action.Kind, ObjectID: action.ObjectID, ErrorClass: ReplayActionErrorNone},
		{Kind: ReplayEventActionFinished, BundleDigest: bundle.Digest, ObservationDigest: observation, ActionPlanDigest: plan, ActionKind: action.Kind, ObjectID: action.ObjectID, ResultClass: ReplayActionCompleted, ErrorClass: ReplayActionErrorNone},
		{Kind: ReplayEventReceiptSaved, BundleDigest: bundle.Digest, ObservationDigest: observation, ActionPlanDigest: plan, ErrorClass: ReplayActionErrorNone},
	}
	store := NewReplayDiagnosticEventStore()
	for _, value := range events {
		first := sealedReplayEvent(t, value)
		second := sealedReplayEvent(t, value)
		if first.EventID != second.EventID || first.EventID == ([32]byte{}) {
			t.Fatalf("unstable event ID for %s", value.Kind)
		}
		if err := store.Append(context.Background(), bundle, first); err != nil {
			t.Fatalf("append %s: %v", value.Kind, err)
		}
	}
}
