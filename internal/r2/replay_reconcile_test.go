package r2

import (
	"slices"
	"strings"
	"testing"
)

func exactReplayObservation(bundle ReplayPublicationBundle) ReplayRemoteObservation {
	observation := ReplayRemoteObservation{
		BundleDigest: bundle.Digest, Claim: ObservationExact, RawManifest: ObservationExact,
		PartChain: ObservationExact, ReplayManifest: ObservationExact, ReplayGraph: ObservationExact,
		RequestCount: 1, ObservationBytes: 1, Complete: true,
	}
	for _, object := range bundle.Contract.RawObjects {
		observation.RawObjects = append(observation.RawObjects, ReplayObjectObservation{ObjectID: replayRawObjectID(object), Class: ObservationExact})
	}
	for _, object := range bundle.Contract.ParquetObjects {
		observation.ParquetObjects = append(observation.ParquetObjects, ReplayObjectObservation{ObjectID: ReplayObjectID(object.ObjectID), Class: ObservationExact})
	}
	for _, object := range bundle.Contract.PartManifests {
		observation.PartManifests = append(observation.PartManifests, ReplayObjectObservation{ObjectID: ReplayObjectID(object.ObjectID), Class: ObservationExact})
	}
	return observation
}

func sealedReplayBundleForReconcile(t *testing.T) ReplayPublicationBundle {
	t.Helper()
	fixture := newReplayPublicationFixture(t, false)
	bundle, err := SealReplayPublicationBundle(replayBundleInputFromFixture(t, fixture))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestReconcileReplayPublicationTruthTable(t *testing.T) {
	bundle := sealedReplayBundleForReconcile(t)
	tests := []struct {
		name       string
		mutate     func(*ReplayRemoteObservation)
		wantKind   ReplayDecisionKind
		wantClass  ObservationClass
		wantAction ReplayActionKind
		wantReason string
	}{
		{"claim_absent", func(o *ReplayRemoteObservation) { o.Claim = ObservationAbsent }, ReplayDecisionIntegrityStop, ObservationAbsent, "", "claim_absent"},
		{"claim_unavailable", func(o *ReplayRemoteObservation) { o.Claim = ObservationUnavailable }, ReplayDecisionRetry, ObservationUnavailable, "", "claim_unavailable"},
		{"claim_different", func(o *ReplayRemoteObservation) { o.Claim = ObservationDifferent }, ReplayDecisionIntegrityStop, ObservationDifferent, "", "claim_not_exact"},
		{"claim_ambiguous", func(o *ReplayRemoteObservation) { o.Claim = ObservationAmbiguous }, ReplayDecisionIntegrityStop, ObservationAmbiguous, "", "claim_not_exact"},
		{"claim_oversized", func(o *ReplayRemoteObservation) { o.Claim = ObservationOversized }, ReplayDecisionResourceStop, ObservationOversized, "", "claim_oversized"},
		{"raw_manifest_absent", func(o *ReplayRemoteObservation) { o.RawManifest = ObservationAbsent }, ReplayDecisionIntegrityStop, ObservationAbsent, "", "raw_manifest_absent"},
		{"raw_object_absent", func(o *ReplayRemoteObservation) { o.RawObjects[0].Class = ObservationAbsent }, ReplayDecisionIntegrityStop, ObservationAbsent, "", "raw_object_absent"},
		{"raw_object_unavailable", func(o *ReplayRemoteObservation) { o.RawObjects[0].Class = ObservationUnavailable }, ReplayDecisionRetry, ObservationUnavailable, "", "raw_object_Unavailable"},
		{"parquet_absent", func(o *ReplayRemoteObservation) { o.ParquetObjects[0].Class = ObservationAbsent }, ReplayDecisionExecute, ObservationAbsent, ReplayActionUploadParquet, "parquet_upload"},
		{"parquet_unavailable", func(o *ReplayRemoteObservation) { o.ParquetObjects[0].Class = ObservationUnavailable }, ReplayDecisionRetry, ObservationUnavailable, "", "parquet_Unavailable"},
		{"parquet_different", func(o *ReplayRemoteObservation) { o.ParquetObjects[0].Class = ObservationDifferent }, ReplayDecisionIntegrityStop, ObservationDifferent, "", "parquet_Different"},
		{"parquet_ambiguous", func(o *ReplayRemoteObservation) { o.ParquetObjects[0].Class = ObservationAmbiguous }, ReplayDecisionIntegrityStop, ObservationAmbiguous, "", "parquet_Ambiguous"},
		{"parquet_oversized", func(o *ReplayRemoteObservation) { o.ParquetObjects[0].Class = ObservationOversized }, ReplayDecisionResourceStop, ObservationOversized, "", "parquet_Oversized"},
		{"part_manifest_absent", func(o *ReplayRemoteObservation) { o.PartManifests[0].Class = ObservationAbsent }, ReplayDecisionExecute, ObservationAbsent, ReplayActionUploadPartManifest, "part_manifest_upload"},
		{"part_predecessor_ambiguous", func(o *ReplayRemoteObservation) { o.PartChain = ObservationAmbiguous }, ReplayDecisionIntegrityStop, ObservationAmbiguous, "", "part_chain_not_exact"},
		{"part_chain_absent", func(o *ReplayRemoteObservation) { o.PartChain = ObservationAbsent }, ReplayDecisionIntegrityStop, ObservationAbsent, "", "part_chain_absent"},
		{"replay_manifest_absent", func(o *ReplayRemoteObservation) { o.ReplayManifest = ObservationAbsent }, ReplayDecisionExecute, ObservationAbsent, ReplayActionUploadReplayManifest, "replay_manifest_upload"},
		{"replay_predecessor_ambiguous", func(o *ReplayRemoteObservation) { o.ReplayGraph = ObservationAmbiguous }, ReplayDecisionIntegrityStop, ObservationAmbiguous, "", "replay_graph_not_exact"},
		{"replay_graph_absent", func(o *ReplayRemoteObservation) { o.ReplayGraph = ObservationAbsent }, ReplayDecisionIntegrityStop, ObservationAbsent, "", "replay_graph_absent"},
		{"final_receipt_barrier", func(o *ReplayRemoteObservation) { o.Complete = false }, ReplayDecisionIntegrityStop, ObservationAmbiguous, "", "final_observation_incomplete"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			observation := exactReplayObservation(bundle)
			testCase.mutate(&observation)
			decision, err := ReconcileReplayPublication(bundle, observation)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Kind != testCase.wantKind || decision.StopClass != testCase.wantClass || decision.ReasonCode != testCase.wantReason {
				t.Fatalf("decision = %+v", decision)
			}
			if testCase.wantAction == "" {
				if len(decision.Actions) != 0 {
					t.Fatalf("zero-action outcome returned %+v", decision.Actions)
				}
			} else if len(decision.Actions) != 1 || decision.Actions[0].Kind != testCase.wantAction || decision.Actions[0].ObjectID == "" {
				t.Fatalf("action = %+v, want %s", decision.Actions, testCase.wantAction)
			}
			if decision.ActionPlanDigest == ([32]byte{}) {
				t.Fatal("decision omitted action-plan digest")
			}
		})
	}
}

func TestReconcileReplayPublicationStopsAtFirstBarrier(t *testing.T) {
	bundle := sealedReplayBundleForReconcile(t)
	observation := exactReplayObservation(bundle)
	observation.Claim = ObservationAbsent
	for index := range observation.ParquetObjects {
		observation.ParquetObjects[index].Class = ObservationAbsent
	}
	decision, err := ReconcileReplayPublication(bundle, observation)
	if err != nil {
		t.Fatal(err)
	}
	if decision.ReasonCode != "claim_absent" || len(decision.Actions) != 0 {
		t.Fatalf("first barrier was skipped: %+v", decision)
	}
}

func TestReconcileReplayPublicationRejectsCandidateOrphanDuplicateAndMissing(t *testing.T) {
	bundle := sealedReplayBundleForReconcile(t)
	tests := []struct {
		name   string
		mutate func(*ReplayRemoteObservation)
		reason string
	}{
		{"orphan", func(o *ReplayRemoteObservation) {
			o.ParquetObjects = append(o.ParquetObjects, ReplayObjectObservation{ObjectID: ReplayObjectID(strings.Repeat("f", 64)), Class: ObservationExact})
		}, "parquet_candidate_or_orphan"},
		{"duplicate", func(o *ReplayRemoteObservation) {
			o.ParquetObjects = append(o.ParquetObjects, o.ParquetObjects[0])
		}, "parquet_duplicate_observation"},
		{"missing", func(o *ReplayRemoteObservation) {
			o.ParquetObjects = o.ParquetObjects[:len(o.ParquetObjects)-1]
		}, "parquet_observation_incomplete"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			observation := exactReplayObservation(bundle)
			testCase.mutate(&observation)
			decision, err := ReconcileReplayPublication(bundle, observation)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Kind != ReplayDecisionIntegrityStop || decision.StopClass != ObservationAmbiguous || decision.ReasonCode != testCase.reason || len(decision.Actions) != 0 {
				t.Fatalf("decision = %+v", decision)
			}
		})
	}
}

func TestReconcileReplayPublicationResourceUnknownAndReadyOutcomes(t *testing.T) {
	bundle := sealedReplayBundleForReconcile(t)

	overBudget := exactReplayObservation(bundle)
	overBudget.RequestCount = bundle.Contract.Limits.MaxObservationRequests + 1
	decision, err := ReconcileReplayPublication(bundle, overBudget)
	if err != nil || decision.Kind != ReplayDecisionResourceStop || decision.ReasonCode != "observation_budget_exhausted" {
		t.Fatalf("resource decision = %+v, err=%v", decision, err)
	}

	unknown := exactReplayObservation(bundle)
	unknown.ReplayGraph = ObservationClass("Unknown")
	if _, err := ReconcileReplayPublication(bundle, unknown); err == nil {
		t.Fatal("unknown observation class was accepted")
	}

	ready := exactReplayObservation(bundle)
	decision, err = ReconcileReplayPublication(bundle, ready)
	if err != nil || decision.Kind != ReplayDecisionReadyForReceipt || decision.StopClass != ObservationExact || len(decision.Actions) != 0 {
		t.Fatalf("ready decision = %+v, err=%v", decision, err)
	}
}

func TestReconcileReplayPublicationActionOrderAndDigestAreDeterministic(t *testing.T) {
	bundle := sealedReplayBundleForReconcile(t)
	firstObservation := exactReplayObservation(bundle)
	for index := range firstObservation.ParquetObjects {
		firstObservation.ParquetObjects[index].Class = ObservationAbsent
	}
	secondObservation := exactReplayObservation(bundle)
	for index := range secondObservation.ParquetObjects {
		secondObservation.ParquetObjects[index].Class = ObservationAbsent
	}
	slices.Reverse(secondObservation.ParquetObjects)
	first, err := ReconcileReplayPublication(bundle, firstObservation)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ReconcileReplayPublication(bundle, secondObservation)
	if err != nil {
		t.Fatal(err)
	}
	if first.ActionPlanDigest != second.ActionPlanDigest || !slices.Equal(first.Actions, second.Actions) {
		t.Fatalf("reordered observation changed plan: first=%+v second=%+v", first, second)
	}
	for index, action := range first.Actions {
		if index > 0 && first.Actions[index-1].ObjectID > action.ObjectID {
			t.Fatal("actions are not ordered by stable object ID")
		}
		if strings.Contains(string(action.ObjectID), "/") {
			t.Fatalf("action leaks a key or path: %+v", action)
		}
	}
}
