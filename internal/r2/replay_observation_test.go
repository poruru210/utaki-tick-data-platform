package r2

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"testing"

	"tick-data-platform/internal/archive"
	"tick-data-platform/internal/protocol"
)

type replayObserverBackend struct {
	objects    map[string][]byte
	list       ReplayRemoteObjectList
	openErrors map[string]error
	openSizes  map[string]int64
	openSteps  map[string][]replayObserverOpenStep
	listError  error
	calls      int
}

type replayObserverOpenStep struct {
	body           []byte
	err            error
	readErr        error
	advertisedSize *int64
}

func (b *replayObserverBackend) ListLimited(_ context.Context, _ string, max uint64) (ReplayRemoteObjectList, error) {
	b.calls++
	if b.listError != nil {
		return ReplayRemoteObjectList{}, b.listError
	}
	if uint64(len(b.list.Objects)) > max {
		return ReplayRemoteObjectList{}, ErrResourceLimit
	}
	return b.list, nil
}

func (b *replayObserverBackend) OpenLimited(_ context.Context, key string, max uint64) (io.ReadCloser, int64, error) {
	b.calls++
	if steps := b.openSteps[key]; len(steps) != 0 {
		step := steps[0]
		b.openSteps[key] = steps[1:]
		if step.err != nil {
			return nil, 0, step.err
		}
		size := int64(len(step.body))
		if step.advertisedSize != nil {
			size = *step.advertisedSize
		}
		var reader io.Reader = bytes.NewReader(step.body)
		if step.readErr != nil {
			reader = &replayErrorAfterReader{reader: reader, err: step.readErr}
		}
		return &boundedReplayReadCloser{Reader: io.LimitReader(reader, int64(max)+1), Closer: io.NopCloser(bytes.NewReader(nil))}, size, nil
	}
	if err := b.openErrors[key]; err != nil {
		return nil, 0, err
	}
	body, ok := b.objects[key]
	if !ok {
		return nil, 0, ErrObjectNotFound
	}
	size := int64(len(body))
	if override, ok := b.openSizes[key]; ok {
		size = override
	}
	return &boundedReplayReadCloser{Reader: io.LimitReader(bytes.NewReader(body), int64(max)+1), Closer: io.NopCloser(bytes.NewReader(nil))}, size, nil
}

type replayErrorAfterReader struct {
	reader io.Reader
	err    error
}

func (r *replayErrorAfterReader) Read(buffer []byte) (int, error) {
	read, err := r.reader.Read(buffer)
	if err == io.EOF {
		return 0, r.err
	}
	return read, err
}

type replayObserverLock struct{ err error }

func (l replayObserverLock) AssertHeld(ReplayPublicationBundle) error { return l.err }

func replayObserverFixture(t *testing.T) (ReplayPublicationBundle, *replayObserverBackend) {
	t.Helper()
	fixture := newReplayPublicationFixture(t, false)
	bundle, err := SealReplayPublicationBundle(replayBundleInputFromFixture(t, fixture))
	if err != nil {
		t.Fatal(err)
	}
	backend := &replayObserverBackend{objects: map[string][]byte{}, openErrors: map[string]error{}, openSizes: map[string]int64{}, openSteps: map[string][]replayObserverOpenStep{}}
	backend.objects[bundle.Contract.Claim.FullKey] = []byte(bundle.Contract.Claim.CanonicalJSON)
	backend.objects[bundle.Contract.RawManifest.FullKey] = append([]byte(nil), fixture.input.RawManifestBytes...)
	for _, object := range bundle.Contract.RawObjects {
		data, err := os.ReadFile(bundle.LocalSources.Artifacts[replayRawObjectID(object)].Path)
		if err != nil {
			t.Fatal(err)
		}
		backend.objects[object.FullKey] = data
	}
	for _, object := range bundle.Contract.PartManifests {
		backend.objects[object.FullKey] = append([]byte(nil), bundle.LocalSources.Artifacts[ReplayObjectID(object.ObjectID)].CanonicalBytes...)
	}
	backend.objects[bundle.Contract.ReplayManifest.FullKey] = append([]byte(nil), bundle.LocalSources.Artifacts[replayManifestObjectID(bundle.Contract)].CanonicalBytes...)
	for _, object := range bundle.Contract.ParquetObjects {
		data, err := os.ReadFile(bundle.LocalSources.Artifacts[ReplayObjectID(object.ObjectID)].Path)
		if err != nil {
			t.Fatal(err)
		}
		backend.objects[object.FullKey] = data
		backend.list.Objects = append(backend.list.Objects, RemoteObject{Key: object.FullKey, Size: int64(object.Bytes)})
	}
	for _, object := range bundle.Contract.PartManifests {
		backend.list.Objects = append(backend.list.Objects, RemoteObject{Key: object.FullKey, Size: int64(object.Bytes)})
	}
	backend.list.Objects = append(backend.list.Objects, RemoteObject{Key: bundle.Contract.ReplayManifest.FullKey, Size: int64(bundle.Contract.ReplayManifest.Bytes)})
	sort.Slice(backend.list.Objects, func(i, j int) bool { return backend.list.Objects[i].Key < backend.list.Objects[j].Key })
	backend.list.Complete = true
	return bundle, backend
}

func TestReplayObservationProducesProtocolFinalOnlyWhenExact(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	observer, err := NewReplayBoundedObserver(backend, replayObserverLock{})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := observer.Observe(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if !observation.Complete || observation.FinalObservation == nil || observation.FinalDigest == ([32]byte{}) {
		t.Fatalf("final observation was not produced: %+v", observation)
	}
	if _, err := protocol.ReplayFinalObservationDigest(*observation.FinalObservation, bundle.Contract); err != nil {
		t.Fatal(err)
	}
	required, err := protocol.ReplayFinalObservationRequiredBytes(bundle.Contract, observation.FinalObservation.ReplayEdges)
	if err != nil {
		t.Fatal(err)
	}
	if observation.FinalObservation.ObservationBytes < required {
		t.Fatalf("final observation bytes=%d required=%d", observation.FinalObservation.ObservationBytes, required)
	}
}

func TestReplayObservationFinalAdapterSatisfiesG1EContract(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	bundle, err := SealReplayPublicationBundle(replayBundleInputFromFixture(t, fixture))
	if err != nil {
		t.Fatal(err)
	}
	graph, err := VerifyReplayRevisionGraph([]protocol.ReplayDayManifest{fixture.replay}, bundle.Contract.Limits.MaxGraphNodes)
	if err != nil {
		t.Fatal(err)
	}
	edges, err := replayObservedRevisionEdges(bundle.Layout, graph.Edges, graph.Revisions)
	if err != nil {
		t.Fatal(err)
	}
	required, err := protocol.ReplayFinalObservationRequiredBytes(bundle.Contract, edges)
	if err != nil {
		t.Fatal(err)
	}
	final, err := makeProtocolFinalObservation(bundle, ReplayObservationBudgetSnapshot{Requests: 100, ObservationBytes: required}, edges)
	if err != nil {
		t.Fatal(err)
	}
	if err := final.Validate(bundle.Contract); err != nil {
		t.Fatalf("G1E final observation adapter: %v (counted=%d required=%d limit=%d)", err, final.ObservationBytes, required, bundle.Contract.Limits.MaxObservationBytes)
	}
}

func replayFinalUpliftFixture(t *testing.T) (ReplayPublicationBundle, []protocol.ReplayObservedRevisionEdge, uint64) {
	t.Helper()
	fixture := newReplayPublicationFixture(t, false)
	bundle := fixture.sealedBundle(t)
	graph, err := VerifyReplayRevisionGraph([]protocol.ReplayDayManifest{fixture.replay}, bundle.Contract.Limits.MaxGraphNodes)
	if err != nil {
		t.Fatal(err)
	}
	edges, err := replayObservedRevisionEdges(bundle.Layout, graph.Edges, graph.Revisions)
	if err != nil {
		t.Fatal(err)
	}
	required, err := protocol.ReplayFinalObservationRequiredBytes(bundle.Contract, edges)
	if err != nil {
		t.Fatal(err)
	}
	return bundle, edges, required
}

func TestReplayFinalObservationUpliftChargesOnceAndPreservesPastRounds(t *testing.T) {
	bundle, edges, required := replayFinalUpliftFixture(t)
	if required < 8 {
		t.Fatalf("unexpectedly small required bytes: %d", required)
	}
	t.Run("exact_fit_with_past_round", func(t *testing.T) {
		budget, _ := NewReplayObservationBudget(bundle.Contract.Limits)
		if err := budget.ChargeObservation(7); err != nil {
			t.Fatal(err)
		}
		start := budget.Snapshot()
		if err := budget.ChargeObservation(required - 3); err != nil {
			t.Fatal(err)
		}
		before := budget.Snapshot()
		pass, class, err := chargeReplayFinalObservationUplift(budget, start, bundle, edges)
		if err != nil || class != ObservationExact {
			t.Fatalf("exact-fit uplift: class=%s err=%v", class, err)
		}
		after := budget.Snapshot()
		if pass.ObservationBytes != required || after.ObservationBytes != 7+required || after.ObservationBytes-before.ObservationBytes != 3 {
			t.Fatalf("uplift snapshots: start=%+v before=%+v pass=%+v after=%+v", start, before, pass, after)
		}
	})
	t.Run("actual_already_sufficient", func(t *testing.T) {
		budget, _ := NewReplayObservationBudget(bundle.Contract.Limits)
		if err := budget.ChargeObservation(required + 5); err != nil {
			t.Fatal(err)
		}
		before := budget.Snapshot()
		pass, class, err := chargeReplayFinalObservationUplift(budget, ReplayObservationBudgetSnapshot{}, bundle, edges)
		if err != nil || class != ObservationExact || pass.ObservationBytes != required+5 || budget.Snapshot() != before {
			t.Fatalf("zero uplift changed budget: before=%+v pass=%+v after=%+v class=%s err=%v", before, pass, budget.Snapshot(), class, err)
		}
	})
}

func TestReplayFinalObservationUpliftExhaustionIsOversizedWithoutFinal(t *testing.T) {
	bundle, edges, required := replayFinalUpliftFixture(t)
	budget, _ := NewReplayObservationBudget(bundle.Contract.Limits)
	prior := bundle.Contract.Limits.MaxObservationBytes - required + 1
	if err := budget.ChargeObservation(prior); err != nil {
		t.Fatal(err)
	}
	start := budget.Snapshot()
	if err := budget.ChargeObservation(required - 2); err != nil {
		t.Fatal(err)
	}
	pass, class, err := chargeReplayFinalObservationUplift(budget, start, bundle, edges)
	if err != nil || class != ObservationOversized {
		t.Fatalf("uplift exhaustion: pass=%+v class=%s err=%v", pass, class, err)
	}
	observation := exactReplayObservation(bundle)
	observation.ReplayGraph = class
	observation.Complete = false
	decision, err := ReconcileReplayPublication(bundle, observation)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Kind != ReplayDecisionResourceStop || len(decision.Actions) != 0 || observation.FinalObservation != nil || observation.FinalDigest != ([32]byte{}) {
		t.Fatalf("uplift exhaustion authorized final/action: observation=%+v decision=%+v", observation, decision)
	}
	if _, err := BuildReplayVerificationReceipt(bundle, observation); err == nil {
		t.Fatal("uplift exhaustion produced a receipt")
	}
}

func TestReplayObservationExactRemoteReadsFailOnlyAtFinalUplift(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	actual := replayFixtureActualPassBytes(bundle)
	graph, err := VerifyReplayRevisionGraph([]protocol.ReplayDayManifest{mustReplayManifest(t, backend.objects[bundle.Contract.ReplayManifest.FullKey])}, bundle.Contract.Limits.MaxGraphNodes)
	if err != nil {
		t.Fatal(err)
	}
	edges, err := replayObservedRevisionEdges(bundle.Layout, graph.Edges, graph.Revisions)
	if err != nil {
		t.Fatal(err)
	}
	required, err := protocol.ReplayFinalObservationRequiredBytes(bundle.Contract, edges)
	if err != nil {
		t.Fatal(err)
	}
	if actual >= required {
		t.Fatalf("fixture does not exercise uplift: actual=%d required=%d", actual, required)
	}
	uplift := required - actual
	budget, _ := NewReplayObservationBudget(bundle.Contract.Limits)
	prior := bundle.Contract.Limits.MaxObservationBytes - actual - uplift + 1
	if err := budget.ChargeObservation(prior); err != nil {
		t.Fatal(err)
	}
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	observation, err := observer.ObserveWithBudget(context.Background(), bundle, budget)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ReplayGraph != ObservationOversized || observation.Complete || observation.FinalObservation != nil || observation.FinalDigest != ([32]byte{}) {
		t.Fatalf("uplift-only exhaustion = %+v", observation)
	}
	decision, err := ReconcileReplayPublication(bundle, observation)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Kind != ReplayDecisionResourceStop || len(decision.Actions) != 0 {
		t.Fatalf("uplift-only exhaustion authorized action: %+v", decision)
	}
	if _, err := BuildReplayVerificationReceipt(bundle, observation); err == nil {
		t.Fatal("uplift-only exhaustion produced receipt")
	}
}

func TestReplayObservationFinalUpliftExactFitCompletes(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	graph, err := VerifyReplayRevisionGraph([]protocol.ReplayDayManifest{mustReplayManifest(t, backend.objects[bundle.Contract.ReplayManifest.FullKey])}, bundle.Contract.Limits.MaxGraphNodes)
	if err != nil {
		t.Fatal(err)
	}
	edges, err := replayObservedRevisionEdges(bundle.Layout, graph.Edges, graph.Revisions)
	if err != nil {
		t.Fatal(err)
	}
	required, err := protocol.ReplayFinalObservationRequiredBytes(bundle.Contract, edges)
	if err != nil {
		t.Fatal(err)
	}
	budget, _ := NewReplayObservationBudget(bundle.Contract.Limits)
	if err := budget.ChargeObservation(bundle.Contract.Limits.MaxObservationBytes - required); err != nil {
		t.Fatal(err)
	}
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	observation, err := observer.ObserveWithBudget(context.Background(), bundle, budget)
	if err != nil {
		t.Fatal(err)
	}
	if !observation.Complete || observation.ReplayGraph != ObservationExact || observation.FinalObservation == nil {
		t.Fatalf("exact-fit uplift did not complete: %+v", observation)
	}
	if observation.FinalObservation.ObservationBytes != required || observation.ObservationBytes != bundle.Contract.Limits.MaxObservationBytes {
		t.Fatalf("exact-fit counters: final=%d aggregate=%d required=%d", observation.FinalObservation.ObservationBytes, observation.ObservationBytes, required)
	}
}

func replayFixtureActualPassBytes(bundle ReplayPublicationBundle) uint64 {
	actual := uint64(len(bundle.Contract.Claim.CanonicalJSON)) + bundle.Contract.RawManifest.Bytes
	for _, object := range bundle.Contract.RawObjects {
		actual += object.Bytes
	}
	for _, object := range bundle.Contract.ParquetObjects {
		actual += uint64(len(object.FullKey)) + 8 + object.Bytes
	}
	for _, object := range bundle.Contract.PartManifests {
		actual += uint64(len(object.FullKey)) + 8 + object.Bytes
	}
	actual += uint64(len(bundle.Contract.ReplayManifest.FullKey)) + 8 + 2*bundle.Contract.ReplayManifest.Bytes
	return actual
}

func mustReplayManifest(t *testing.T, body []byte) protocol.ReplayDayManifest {
	t.Helper()
	manifest, err := protocol.VerifyReplayDayManifest(body)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := protocol.ReplayDayManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ManifestSHA256 = digest
	return manifest
}

func TestReplayObservationSharedBudgetAccumulatesAcrossFreshPasses(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	budget, err := NewReplayObservationBudget(bundle.Contract.Limits)
	if err != nil {
		t.Fatal(err)
	}
	first, err := observer.ObserveWithBudget(context.Background(), bundle, budget)
	if err != nil || !first.Complete {
		t.Fatalf("first observation: complete=%v err=%v", first.Complete, err)
	}
	second, err := observer.ObserveWithBudget(context.Background(), bundle, budget)
	if err != nil || !second.Complete {
		t.Fatalf("second observation: complete=%v err=%v", second.Complete, err)
	}
	if second.RequestCount <= first.RequestCount || second.ObservationBytes <= first.ObservationBytes {
		t.Fatalf("shared budget did not accumulate: first=%+v second=%+v", first, second)
	}
}

func TestReplayObservationIncompletePaginationIsAmbiguous(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	backend.list.Complete = false
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	observation, err := observer.Observe(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ParquetObjects[0].Class != ObservationAmbiguous || observation.Complete {
		t.Fatalf("incomplete list = %+v", observation)
	}
}

func TestReplayObservationListFailureIsUnavailableNotAbsent(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	backend.listError = context.DeadlineExceeded
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	observation, err := observer.Observe(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ReplayManifest != ObservationUnavailable || observation.ReplayGraph != ObservationUnavailable {
		t.Fatalf("list failure = manifest:%s graph:%s", observation.ReplayManifest, observation.ReplayGraph)
	}
}

func TestReplayObservationMissingCandidateIsAbsentOnlyAfterCompleteList(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	missing := bundle.Contract.ParquetObjects[0].FullKey
	filtered := backend.list.Objects[:0]
	for _, object := range backend.list.Objects {
		if object.Key != missing {
			filtered = append(filtered, object)
		}
	}
	backend.list.Objects = filtered
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	observation, err := observer.Observe(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ParquetObjects[0].Class != ObservationAbsent {
		t.Fatalf("missing candidate = %s", observation.ParquetObjects[0].Class)
	}
}

func TestReplayObservationTimeoutAndStaleListNeverBecomeAbsent(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		bundle, backend := replayObserverFixture(t)
		backend.openErrors[bundle.Contract.RawObjects[0].FullKey] = context.DeadlineExceeded
		observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
		observation, err := observer.Observe(context.Background(), bundle)
		if err != nil {
			t.Fatal(err)
		}
		if observation.RawObjects[0].Class != ObservationUnavailable {
			t.Fatalf("timeout = %s", observation.RawObjects[0].Class)
		}
	})
	t.Run("stale_list", func(t *testing.T) {
		bundle, backend := replayObserverFixture(t)
		backend.openErrors[bundle.Contract.PartManifests[0].FullKey] = ErrObjectNotFound
		observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
		observation, err := observer.Observe(context.Background(), bundle)
		if err != nil {
			t.Fatal(err)
		}
		if observation.PartManifests[0].Class != ObservationAmbiguous {
			t.Fatalf("stale list = %s", observation.PartManifests[0].Class)
		}
	})
}

func TestReplayObservationShortReadIsUnavailable(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	key := bundle.Contract.RawObjects[0].FullKey
	backend.openSizes[key] = int64(len(backend.objects[key]) + 1)
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	observation, err := observer.Observe(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if observation.RawObjects[0].Class != ObservationUnavailable {
		t.Fatalf("short read = %s", observation.RawObjects[0].Class)
	}
}

func TestReplayObservationInvalidInventoryIsAmbiguous(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ReplayPublicationBundle, *replayObserverBackend)
	}{
		{"duplicate", func(_ *ReplayPublicationBundle, backend *replayObserverBackend) {
			backend.list.Objects = append(backend.list.Objects, backend.list.Objects[0])
		}},
		{"negative_size", func(_ *ReplayPublicationBundle, backend *replayObserverBackend) {
			backend.list.Objects[0].Size = -1
		}},
		{"unknown_key", func(bundle *ReplayPublicationBundle, backend *replayObserverBackend) {
			scope, _, _ := replayVerificationInputs(bundle.Contract)
			base, _ := protocol.ReplayDerivativeBaseKey(scope)
			backend.list.Objects = append(backend.list.Objects, RemoteObject{Key: bundle.Contract.Scope.ImmutablePrefix + "/" + base + "/unknown.bin", Size: 1})
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			bundle, backend := replayObserverFixture(t)
			testCase.mutate(&bundle, backend)
			observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
			observation, err := observer.Observe(context.Background(), bundle)
			if err != nil {
				t.Fatal(err)
			}
			if observation.ParquetObjects[0].Class != ObservationAmbiguous || observation.Complete {
				t.Fatalf("invalid inventory = %+v", observation)
			}
		})
	}
}

func TestReplayObservationParquetMutationIsDifferent(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	key := bundle.Contract.ParquetObjects[0].FullKey
	backend.objects[key] = append([]byte(nil), backend.objects[key]...)
	backend.objects[key][0] ^= 1
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	observation, err := observer.Observe(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ParquetObjects[0].Class != ObservationDifferent {
		t.Fatalf("Parquet mutation = %s", observation.ParquetObjects[0].Class)
	}
}

func TestReplayObservationParquetListSizeMismatchIsAmbiguous(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	target := bundle.Contract.ParquetObjects[0]
	for index := range backend.list.Objects {
		if backend.list.Objects[index].Key == target.FullKey {
			backend.list.Objects[index].Size++
		}
	}
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	observation, err := observer.Observe(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ParquetObjects[0].Class != ObservationAmbiguous || observation.Complete || observation.FinalObservation != nil || observation.FinalDigest != ([32]byte{}) {
		t.Fatalf("wrong Parquet list size = %+v", observation)
	}
	decision, err := ReconcileReplayPublication(bundle, observation)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Kind != ReplayDecisionIntegrityStop || len(decision.Actions) != 0 {
		t.Fatalf("wrong Parquet list size authorized action: %+v", decision)
	}
}

func TestReplayObservationTerminalFreshReadPreservesClassification(t *testing.T) {
	wrongFixture := newReplayPublicationFixture(t, true)
	for _, testCase := range []struct {
		name  string
		step  replayObserverOpenStep
		class ObservationClass
	}{
		{name: "timeout", step: replayObserverOpenStep{err: context.DeadlineExceeded}, class: ObservationUnavailable},
		{name: "metadata_limit", step: replayObserverOpenStep{err: ErrResourceLimit}, class: ObservationOversized},
		{name: "readable_invalid", step: replayObserverOpenStep{body: []byte("{")}, class: ObservationAmbiguous},
		{name: "valid_wrong_identity", step: replayObserverOpenStep{body: wrongFixture.input.ReplayManifestBytes}, class: ObservationDifferent},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			bundle, backend := replayObserverFixture(t)
			key := bundle.Contract.ReplayManifest.FullKey
			backend.openSteps[key] = []replayObserverOpenStep{{body: append([]byte(nil), backend.objects[key]...)}, testCase.step}
			observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
			observation, err := observer.Observe(context.Background(), bundle)
			if err != nil {
				t.Fatal(err)
			}
			if observation.ReplayManifest != testCase.class || observation.Complete || observation.FinalObservation != nil || observation.FinalDigest != ([32]byte{}) {
				t.Fatalf("terminal second read = %+v", observation)
			}
		})
	}
}

func TestReplayObservationTerminalFreshReadBudgetExhaustionIsOversized(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	parts := make([]protocol.PartManifest, 0, len(bundle.Contract.PartManifests))
	for _, object := range bundle.Contract.PartManifests {
		part, err := archive.VerifyPartManifestObject(backend.objects[object.FullKey], object.RelativeKey, mustHash(object.DomainDigest))
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, part)
	}
	scope, conversion, err := replayVerificationInputs(bundle.Contract)
	if err != nil {
		t.Fatal(err)
	}
	partGraph, err := VerifyReplayPartGraph(parts, scope, conversion, bundle.Contract.Limits.MaxGraphNodes)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := NewReplayObservationBudget(bundle.Contract.Limits)
	if err != nil {
		t.Fatal(err)
	}
	remaining := bundle.Contract.ReplayManifest.Bytes - 1
	if err := budget.ChargeMetadata(bundle.Contract.Limits.MaxTotalMetadataBytes - remaining); err != nil {
		t.Fatal(err)
	}
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	_, class := observer.observeTerminalReplayCandidate(context.Background(), budget, bundle, scope, conversion, partGraph, nil)
	if class != ObservationOversized {
		t.Fatalf("terminal budget exhaustion = %s", class)
	}
	if snapshot := budget.Snapshot(); snapshot.MetadataBytes != bundle.Contract.Limits.MaxTotalMetadataBytes {
		t.Fatalf("terminal actual bytes did not saturate metadata budget: %+v", snapshot)
	}
}

func TestReplayObservationChargesActualBytesForUnexpectedBodies(t *testing.T) {
	t.Run("metadata", func(t *testing.T) {
		bundle, backend := replayObserverFixture(t)
		key := bundle.Contract.Claim.FullKey
		backend.objects[key] = append(backend.objects[key], ' ')
		budget, _ := NewReplayObservationBudget(bundle.Contract.Limits)
		observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
		class := observer.observeMetadata(context.Background(), budget, key, uint64(len(bundle.Contract.Claim.CanonicalJSON)), []byte(bundle.Contract.Claim.CanonicalJSON), nil)
		if class != ObservationAmbiguous {
			t.Fatalf("larger metadata = %s", class)
		}
		if got := budget.Snapshot().MetadataBytes; got != uint64(len(backend.objects[key])) {
			t.Fatalf("metadata charged=%d actual=%d", got, len(backend.objects[key]))
		}
	})
	t.Run("raw_reader_error", func(t *testing.T) {
		bundle, backend := replayObserverFixture(t)
		object := bundle.Contract.RawObjects[0]
		body := append([]byte(nil), backend.objects[object.FullKey]...)
		backend.openSteps[object.FullKey] = []replayObserverOpenStep{{body: body, readErr: errors.New("injected reader failure")}}
		budget, _ := NewReplayObservationBudget(bundle.Contract.Limits)
		observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
		class := observer.observeHashedObject(context.Background(), budget, object.FullKey, object.Bytes, object.SHA256, replayBudgetObservation, bundle.Contract.Limits.MaxObservationBytes)
		if class != ObservationUnavailable {
			t.Fatalf("raw reader error = %s", class)
		}
		if got := budget.Snapshot().ObservationBytes; got != uint64(len(body)) {
			t.Fatalf("raw charged=%d actual=%d", got, len(body))
		}
	})
	t.Run("raw_larger_than_expected", func(t *testing.T) {
		bundle, backend := replayObserverFixture(t)
		object := bundle.Contract.RawObjects[0]
		backend.objects[object.FullKey] = append(backend.objects[object.FullKey], 'x')
		budget, _ := NewReplayObservationBudget(bundle.Contract.Limits)
		observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
		class := observer.observeHashedObject(context.Background(), budget, object.FullKey, object.Bytes, object.SHA256, replayBudgetObservation, bundle.Contract.Limits.MaxObservationBytes)
		if class != ObservationDifferent {
			t.Fatalf("larger raw body = %s", class)
		}
		if got := budget.Snapshot().ObservationBytes; got != uint64(len(backend.objects[object.FullKey])) {
			t.Fatalf("larger raw charged=%d actual=%d", got, len(backend.objects[object.FullKey]))
		}
	})
}

func TestReplayObservationMultipleObjectsCannotBypassAggregateBytes(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	backend.objects["first"] = []byte("123456")
	backend.objects["second"] = []byte("12345")
	limits := bundle.Contract.Limits
	limits.MaxObservationBytes = 10
	budget, err := NewReplayObservationBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	if _, class := observer.consumeRemoteObject(context.Background(), budget, "first", replayBudgetObservation, 10, io.Discard); class != ObservationExact {
		t.Fatalf("first object = %s", class)
	}
	if _, class := observer.consumeRemoteObject(context.Background(), budget, "second", replayBudgetObservation, 10, io.Discard); class != ObservationOversized {
		t.Fatalf("second object = %s", class)
	}
	calls := backend.calls
	if _, class := observer.consumeRemoteObject(context.Background(), budget, "third", replayBudgetObservation, 10, io.Discard); class != ObservationOversized {
		t.Fatalf("post-exhaustion object = %s", class)
	}
	if backend.calls != calls {
		t.Fatal("aggregate exhaustion allowed another remote read")
	}
}

func TestReplayObservationMultipleMetadataObjectsCannotBypassCategoryBudget(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	backend.objects["first-metadata"] = []byte("123456")
	backend.objects["second-metadata"] = []byte("12345")
	limits := bundle.Contract.Limits
	limits.MaxTotalMetadataBytes = 10
	budget, err := NewReplayObservationBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
	if _, class := observer.consumeRemoteObject(context.Background(), budget, "first-metadata", replayBudgetMetadata, 10, io.Discard); class != ObservationExact {
		t.Fatalf("first metadata object = %s", class)
	}
	if _, class := observer.consumeRemoteObject(context.Background(), budget, "second-metadata", replayBudgetMetadata, 10, io.Discard); class != ObservationOversized {
		t.Fatalf("second metadata object = %s", class)
	}
	snapshot := budget.Snapshot()
	if snapshot.MetadataBytes != 10 || snapshot.ObservationBytes != 11 {
		t.Fatalf("metadata aggregate charge = %+v", snapshot)
	}
}

func TestReplayObservationParquetOversizeAndShortReadCannotBypassBudget(t *testing.T) {
	t.Run("oversize", func(t *testing.T) {
		bundle, backend := replayObserverFixture(t)
		object := bundle.Contract.ParquetObjects[0]
		budget, _ := NewReplayObservationBudget(bundle.Contract.Limits)
		observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
		class := observer.observeHashedObject(context.Background(), budget, object.FullKey, object.Bytes, object.SHA256, replayBudgetParquet, object.Bytes-1)
		if class != ObservationOversized {
			t.Fatalf("Parquet oversize = %s", class)
		}
		if got := budget.Snapshot().ParquetBytes; got != object.Bytes {
			t.Fatalf("Parquet oversize charged=%d actual=%d", got, object.Bytes)
		}
	})
	t.Run("short_read", func(t *testing.T) {
		bundle, backend := replayObserverFixture(t)
		object := bundle.Contract.ParquetObjects[0]
		backend.openSizes[object.FullKey] = int64(object.Bytes + 1)
		observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{})
		observation, err := observer.Observe(context.Background(), bundle)
		if err != nil {
			t.Fatal(err)
		}
		if observation.ParquetObjects[0].Class != ObservationUnavailable || observation.Complete || observation.FinalDigest != ([32]byte{}) {
			t.Fatalf("Parquet short read = %+v", observation)
		}
	})
}

func TestReplayObservationLockFailurePreventsRemoteCalls(t *testing.T) {
	bundle, backend := replayObserverFixture(t)
	observer, _ := NewReplayBoundedObserver(backend, replayObserverLock{err: errors.New("not held")})
	if _, err := observer.Observe(context.Background(), bundle); err == nil {
		t.Fatal("lock failure was accepted")
	}
	if backend.calls != 0 {
		t.Fatalf("remote calls after lock failure: backend=%d", backend.calls)
	}
}
