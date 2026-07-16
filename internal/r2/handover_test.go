package r2

import (
	"bytes"
	"context"
	"io"
	"sort"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/operations"
)

type handoverBackend struct {
	objects          map[string][]byte
	openErrors       map[string]error
	putError         error
	putAfterWriteErr error
}

func (b *handoverBackend) PutIfAbsent(_ context.Context, key string, body []byte) error {
	if _, exists := b.objects[key]; exists {
		return ErrObjectExists
	}
	b.objects[key] = append([]byte(nil), body...)
	if b.putAfterWriteErr != nil {
		err := b.putAfterWriteErr
		b.putAfterWriteErr = nil
		return err
	}
	if b.putError != nil {
		return b.putError
	}
	return nil
}

func (b *handoverBackend) ListLimited(_ context.Context, prefix string, max uint64) (ReplayRemoteObjectList, error) {
	keys := make([]string, 0)
	for key := range b.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if uint64(len(keys)) > max {
		return ReplayRemoteObjectList{}, ErrResourceLimit
	}
	objects := make([]RemoteObject, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, RemoteObject{Key: key, Size: int64(len(b.objects[key]))})
	}
	return ReplayRemoteObjectList{Objects: objects, Complete: true}, nil
}

func (b *handoverBackend) OpenLimited(_ context.Context, key string, max uint64) (io.ReadCloser, int64, error) {
	if err := b.openErrors[key]; err != nil {
		return nil, 0, err
	}
	body, ok := b.objects[key]
	if !ok {
		return nil, 0, ErrObjectNotFound
	}
	if uint64(len(body)) > max {
		return nil, int64(len(body)), ErrResourceLimit
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func handoverEvidenceFor(t *testing.T, scope archive.ScopeConfig) HandoverOperatorEvidence {
	t.Helper()
	scopeKey, err := archive.ScopePathKey(scope)
	if err != nil {
		t.Fatal(err)
	}
	configDigest, err := scope.ConfigHash()
	if err != nil {
		t.Fatal(err)
	}
	return HandoverOperatorEvidence{
		EvidenceVersion: "operator-handover-evidence-v1", ScopeKey: scopeKey, PriorEpoch: scope.PublisherEpoch,
		Process: ProcessStopEvidence{
			EvidenceVersion: "process-stop-evidence-v1", ScopeKey: scopeKey, PriorPublisherEpoch: scope.PublisherEpoch,
			RuntimeIdentityDigest: [32]byte{1}, ObservedAtUnixMS: 100, Stopped: true,
		},
		Credential: CredentialRevocationEvidence{
			EvidenceVersion: "credential-revocation-evidence-v1", ScopeKey: scopeKey,
			CredentialIDDigest: [32]byte{2}, ScopeDigest: configDigest, RevokedAtUnixMS: 101, Revoked: true,
		},
	}
}

func handoverConfirmationFor(t *testing.T, seal HandoverSeal) HandoverConfirmationRecord {
	t.Helper()
	return HandoverConfirmationRecord{
		ConfirmationVersion: "operator-handover-confirmation-v1",
		ScopeKey:            seal.ScopeKey,
		PriorEpoch:          seal.Artifact.PriorPublisherEpoch,
		SealDigest:          seal.Digest,
		OperatorIDDigest:    [32]byte{3},
		ConfirmedAtUnixMS:   102,
		Confirmed:           true,
	}
}

func TestHandoverObserveReconcileAndConditionalCreate(t *testing.T) {
	scope := layoutTestScope()
	layout, err := NewLayout("immutable-root", scope)
	if err != nil {
		t.Fatal(err)
	}
	evidence := handoverEvidenceFor(t, scope)
	seal, err := SealHandover(layout, scope.PublisherEpoch+1, evidence)
	if err != nil {
		t.Fatal(err)
	}
	confirmation := handoverConfirmationFor(t, seal)
	backend := &handoverBackend{objects: map[string][]byte{seal.Artifact.PriorClaimKey: seal.PriorClaimBytes}}
	limits := operations.DefaultResourceLimits
	observation, err := ObserveHandover(context.Background(), backend, layout, seal, limits)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := ReconcileHandover(seal, observation, evidence, confirmation)
	if err != nil || decision.Kind != HandoverExecute || len(decision.Actions) != 1 || decision.Actions[0].Kind != HandoverCreateArtifact {
		t.Fatalf("initial handover decision=%+v err=%v", decision, err)
	}
	if err := ExecuteHandoverActions(context.Background(), backend, backend, layout, seal, limits, evidence, confirmation, decision); err != nil {
		t.Fatal(err)
	}

	observation, err = ObserveHandover(context.Background(), backend, layout, seal, limits)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = ReconcileHandover(seal, observation, evidence, confirmation)
	if err != nil || decision.Kind != HandoverExecute || len(decision.Actions) != 1 || decision.Actions[0].Kind != HandoverCreateTransition {
		t.Fatalf("transition handover decision=%+v err=%v", decision, err)
	}
	if err := ExecuteHandoverActions(context.Background(), backend, backend, layout, seal, limits, evidence, confirmation, decision); err != nil {
		t.Fatal(err)
	}

	observation, err = ObserveHandover(context.Background(), backend, layout, seal, limits)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = ReconcileHandover(seal, observation, evidence, confirmation)
	if err != nil || decision.Kind != HandoverExecute || len(decision.Actions) != 1 || decision.Actions[0].Kind != HandoverCreateNextClaim {
		t.Fatalf("next-claim handover decision=%+v err=%v", decision, err)
	}
	if err := ExecuteHandoverActions(context.Background(), backend, backend, layout, seal, limits, evidence, confirmation, decision); err != nil {
		t.Fatal(err)
	}

	observation, err = ObserveHandover(context.Background(), backend, layout, seal, limits)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = ReconcileHandover(seal, observation, evidence, confirmation)
	if err != nil || decision.Kind != HandoverReady || len(decision.Actions) != 0 {
		t.Fatalf("completed handover decision=%+v err=%v", decision, err)
	}
	if err := ExecuteHandoverActions(context.Background(), backend, backend, layout, seal, limits, evidence, confirmation, decision); err == nil {
		t.Fatal("ready handover was unexpectedly executable")
	}
}

func TestHandoverReconcilerStopsUnsafeStates(t *testing.T) {
	scope := layoutTestScope()
	layout, err := NewLayout("immutable-root", scope)
	if err != nil {
		t.Fatal(err)
	}
	evidence := handoverEvidenceFor(t, scope)
	seal, err := SealHandover(layout, scope.PublisherEpoch+1, evidence)
	if err != nil {
		t.Fatal(err)
	}
	confirmation := handoverConfirmationFor(t, seal)
	limits := operations.DefaultResourceLimits
	base := HandoverRemoteObservation{
		SealDigest: seal.Digest, PriorClaim: ObservationExact, Artifact: ObservationExact, Transition: ObservationAbsent,
		NextClaim: ObservationAbsent, CandidateNamespace: ObservationAbsent,
	}
	decision, err := ReconcileHandover(seal, base, evidence, confirmation)
	if err != nil || decision.Kind != HandoverExecute {
		t.Fatalf("base decision=%+v err=%v", decision, err)
	}
	bypassBackend := &handoverBackend{objects: map[string][]byte{seal.Artifact.PriorClaimKey: seal.PriorClaimBytes}}
	if err := ExecuteHandoverActions(context.Background(), bypassBackend, bypassBackend, layout, seal, limits, evidence, confirmation, HandoverDecision{
		Kind: HandoverExecute, SealDigest: seal.Digest,
		Actions: []HandoverAction{{Kind: HandoverCreateNextClaim}},
	}); err == nil {
		t.Fatal("executor accepted a hand-built next-claim bypass")
	}
	if _, exists := bypassBackend.objects[seal.Artifact.ExpectedNextClaimKey]; exists {
		t.Fatal("executor performed a next-claim bypass mutation")
	}
	unsafeEvidence := evidence
	unsafeEvidence.Process.Stopped = false
	decision, err = ReconcileHandover(seal, base, unsafeEvidence, confirmation)
	if err != nil || decision.Kind != HandoverIntegrityStop || len(decision.Actions) != 0 {
		t.Fatalf("old process active decision=%+v err=%v", decision, err)
	}
	badConfirmation := confirmation
	badConfirmation.SealDigest[0] ^= 1
	decision, err = ReconcileHandover(seal, base, evidence, badConfirmation)
	if err != nil || decision.Kind != HandoverIntegrityStop || len(decision.Actions) != 0 {
		t.Fatalf("confirmation for a different seal was accepted: %+v err=%v", decision, err)
	}
	for _, test := range []struct {
		name     string
		wantKind HandoverDecisionKind
		mutate   func(*HandoverRemoteObservation)
	}{
		{name: "transition-different", wantKind: HandoverIntegrityStop, mutate: func(o *HandoverRemoteObservation) { o.Transition = ObservationDifferent }},
		{name: "candidate-ambiguous", wantKind: HandoverIntegrityStop, mutate: func(o *HandoverRemoteObservation) { o.CandidateNamespace = ObservationAmbiguous }},
		{name: "next-without-transition", wantKind: HandoverIntegrityStop, mutate: func(o *HandoverRemoteObservation) { o.NextClaim = ObservationExact }},
		{name: "prior-claim-different", wantKind: HandoverIntegrityStop, mutate: func(o *HandoverRemoteObservation) { o.PriorClaim = ObservationDifferent }},
		{name: "next-claim-different", wantKind: HandoverIntegrityStop, mutate: func(o *HandoverRemoteObservation) {
			o.Transition = ObservationExact
			o.NextClaim = ObservationDifferent
		}},
		{name: "next-claim-collision", wantKind: HandoverIntegrityStop, mutate: func(o *HandoverRemoteObservation) {
			o.Transition = ObservationExact
			o.CandidateNamespace = ObservationAmbiguous
		}},
		{name: "artifact-different", wantKind: HandoverIntegrityStop, mutate: func(o *HandoverRemoteObservation) { o.Artifact = ObservationDifferent }},
		{name: "transition-unavailable", wantKind: HandoverRetry, mutate: func(o *HandoverRemoteObservation) { o.Transition = ObservationUnavailable }},
		{name: "transition-oversized", wantKind: HandoverResourceStop, mutate: func(o *HandoverRemoteObservation) { o.Transition = ObservationOversized }},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation := base
			test.mutate(&observation)
			decision, err := ReconcileHandover(seal, observation, evidence, confirmation)
			if err != nil || decision.Kind != test.wantKind || len(decision.Actions) != 0 {
				t.Fatalf("decision=%+v err=%v", decision, err)
			}
		})
	}
	mutated := seal
	mutated.NextClaimBytes = append([]byte(nil), seal.NextClaimBytes...)
	mutated.NextClaimBytes[0] ^= 1
	if err := mutated.Validate(); err == nil {
		t.Fatal("seal accepted claim bytes detached from artifact")
	}
}

func TestHandoverRevocationAndUnknownOutcomeAreFailClosed(t *testing.T) {
	scope := layoutTestScope()
	layout, err := NewLayout("immutable-root", scope)
	if err != nil {
		t.Fatal(err)
	}
	evidence := handoverEvidenceFor(t, scope)
	seal, err := SealHandover(layout, scope.PublisherEpoch+1, evidence)
	if err != nil {
		t.Fatal(err)
	}
	confirmation := handoverConfirmationFor(t, seal)
	base := HandoverRemoteObservation{
		SealDigest: seal.Digest, PriorClaim: ObservationExact, Artifact: ObservationExact,
		Transition: ObservationExact, NextClaim: ObservationAbsent, CandidateNamespace: ObservationAbsent,
	}
	for _, mutate := range []func(*HandoverOperatorEvidence){
		func(e *HandoverOperatorEvidence) { e.Credential.Revoked = false },
		func(e *HandoverOperatorEvidence) { e.Process.Stopped = false },
	} {
		unsafe := evidence
		mutate(&unsafe)
		decision, err := ReconcileHandover(seal, base, unsafe, confirmation)
		if err != nil || decision.Kind != HandoverIntegrityStop || len(decision.Actions) != 0 {
			t.Fatalf("unsafe operator evidence decision=%+v err=%v", decision, err)
		}
	}

	backend := &handoverBackend{
		objects:    map[string][]byte{seal.ArtifactKey: seal.CanonicalBytes, seal.Artifact.PriorClaimKey: seal.PriorClaimBytes},
		openErrors: map[string]error{seal.TransitionKey: context.DeadlineExceeded},
	}
	observation, err := ObserveHandover(context.Background(), backend, layout, seal, operations.DefaultResourceLimits)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Transition != ObservationUnavailable {
		t.Fatalf("timeout was not classified as unavailable: %+v", observation)
	}
	decision, err := ReconcileHandover(seal, observation, evidence, confirmation)
	if err != nil || decision.Kind != HandoverRetry || len(decision.Actions) != 0 {
		t.Fatalf("unknown transition outcome was not retryable: %+v err=%v", decision, err)
	}

	backend = &handoverBackend{objects: map[string][]byte{seal.ArtifactKey: append([]byte("different"), seal.CanonicalBytes...), seal.Artifact.PriorClaimKey: seal.PriorClaimBytes}}
	observation, err = ObserveHandover(context.Background(), backend, layout, seal, operations.DefaultResourceLimits)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = ReconcileHandover(seal, observation, evidence, confirmation)
	if err != nil || decision.Kind != HandoverIntegrityStop || len(decision.Actions) != 0 {
		t.Fatalf("same-key different artifact was not stopped: %+v err=%v", decision, err)
	}
}

func TestHandoverRestartAfterEachStepAndConditionalFailure(t *testing.T) {
	scope := layoutTestScope()
	layout, err := NewLayout("immutable-root", scope)
	if err != nil {
		t.Fatal(err)
	}
	evidence := handoverEvidenceFor(t, scope)
	seal, err := SealHandover(layout, scope.PublisherEpoch+1, evidence)
	if err != nil {
		t.Fatal(err)
	}
	confirmation := handoverConfirmationFor(t, seal)
	backend := &handoverBackend{objects: map[string][]byte{seal.Artifact.PriorClaimKey: seal.PriorClaimBytes}}
	limits := operations.DefaultResourceLimits
	for step, want := range []HandoverActionKind{HandoverCreateArtifact, HandoverCreateTransition, HandoverCreateNextClaim} {
		observation, err := ObserveHandover(context.Background(), backend, layout, seal, limits)
		if err != nil {
			t.Fatal(err)
		}
		decision, err := ReconcileHandover(seal, observation, evidence, confirmation)
		if err != nil || decision.Kind != HandoverExecute || len(decision.Actions) != 1 || decision.Actions[0].Kind != want {
			t.Fatalf("step %d decision=%+v err=%v", step, decision, err)
		}
		if err := ExecuteHandoverActions(context.Background(), backend, backend, layout, seal, limits, evidence, confirmation, decision); err != nil {
			t.Fatal(err)
		}
		cloned := make(map[string][]byte, len(backend.objects))
		for key, body := range backend.objects {
			cloned[key] = append([]byte(nil), body...)
		}
		backend = &handoverBackend{objects: cloned}
	}
	observation, err := ObserveHandover(context.Background(), backend, layout, seal, limits)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := ReconcileHandover(seal, observation, evidence, confirmation)
	if err != nil || decision.Kind != HandoverReady {
		t.Fatalf("restart recovery did not reach ready: %+v err=%v", decision, err)
	}

	fault := &handoverBackend{objects: map[string][]byte{seal.Artifact.PriorClaimKey: seal.PriorClaimBytes}, putAfterWriteErr: context.DeadlineExceeded}
	observation, err = ObserveHandover(context.Background(), fault, layout, seal, limits)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = ReconcileHandover(seal, observation, evidence, confirmation)
	if err != nil || decision.Kind != HandoverExecute || decision.Actions[0].Kind != HandoverCreateArtifact {
		t.Fatalf("fault setup decision=%+v err=%v", decision, err)
	}
	if err := ExecuteHandoverActions(context.Background(), fault, fault, layout, seal, limits, evidence, confirmation, decision); err == nil {
		t.Fatal("unknown write outcome was unexpectedly successful")
	}
	observation, err = ObserveHandover(context.Background(), fault, layout, seal, limits)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Artifact != ObservationExact {
		t.Fatalf("unknown outcome did not preserve artifact: %+v", observation)
	}
	if err := ExecuteHandoverActions(context.Background(), fault, fault, layout, seal, limits, evidence, confirmation, HandoverDecision{Kind: HandoverExecute, SealDigest: seal.Digest, Actions: []HandoverAction{{Kind: HandoverCreateArtifact}}}); err != nil {
		t.Fatalf("conditional same-key retry was not idempotent: %v", err)
	}
}
