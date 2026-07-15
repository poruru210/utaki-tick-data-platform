package r2

import (
	"testing"

	"tick-data-platform/internal/protocol"
)

func TestReplayPartGraphAcceptsOnlyCompleteOrderedChain(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	sealed, err := SealReplayPublicationBundle(replayBundleInputFromFixture(t, fixture))
	if err != nil {
		t.Fatal(err)
	}
	scope, conversion, err := replayVerificationInputs(sealed.Contract)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := VerifyReplayPartGraph(fixture.parts, scope, conversion, sealed.Contract.Limits.MaxGraphNodes)
	if err != nil {
		t.Fatal(err)
	}
	if graph.PartSetRoot != fixture.replay.PartSetRoot || graph.CanonicalRowChainRoot != fixture.replay.CanonicalStreamRowChainRoot {
		t.Fatalf("part graph roots = %+v", graph)
	}

	branched := append([]protocol.PartManifest(nil), fixture.parts...)
	branched = append(branched, branched[len(branched)-1])
	if _, err := VerifyReplayPartGraph(branched, scope, conversion, sealed.Contract.Limits.MaxGraphNodes); err == nil {
		t.Fatal("duplicate branch was accepted")
	}
	missing := append([]protocol.PartManifest(nil), fixture.parts[1:]...)
	if _, err := VerifyReplayPartGraph(missing, scope, conversion, sealed.Contract.Limits.MaxGraphNodes); err == nil {
		t.Fatal("missing predecessor was accepted")
	}
}

func TestReplayRevisionGraphRejectsBranchAndMissingPredecessor(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	graph, err := VerifyReplayRevisionGraph([]protocol.ReplayDayManifest{fixture.replay}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Edges) != 1 || graph.Edges[0].Revision != 1 {
		t.Fatalf("revision graph = %+v", graph)
	}
	branched := []protocol.ReplayDayManifest{fixture.replay, fixture.replay}
	if _, err := VerifyReplayRevisionGraph(branched, 2); err == nil {
		t.Fatal("duplicate revision winner selection was accepted")
	}
	missing := fixture.replay
	missing.Revision = 2
	missing.ManifestSHA256 = [32]byte{}
	if _, err := VerifyReplayRevisionGraph([]protocol.ReplayDayManifest{missing}, 2); err == nil {
		t.Fatal("missing genesis predecessor was accepted")
	}
}

func TestReplayObservedRevisionEdgeDerivesFullKeyFromTrustedLayout(t *testing.T) {
	fixture := newReplayPublicationFixture(t, false)
	input := replayBundleInputFromFixture(t, fixture)
	graph, err := VerifyReplayRevisionGraph([]protocol.ReplayDayManifest{fixture.replay}, 2)
	if err != nil {
		t.Fatal(err)
	}
	graph.Edges[0].FullKey = "remote-observed/untrusted.json"
	edges, err := replayObservedRevisionEdges(input.Layout, graph.Edges, graph.Revisions)
	if err != nil {
		t.Fatal(err)
	}
	want, err := input.Layout.ReplayDayManifestKey(fixture.replay)
	if err != nil {
		t.Fatal(err)
	}
	if edges[0].FullKey != want || edges[0].FullKey == "remote-observed/untrusted.json" {
		t.Fatalf("derived full key = %q, want %q", edges[0].FullKey, want)
	}
}
