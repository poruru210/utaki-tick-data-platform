package r2

import (
	"errors"
	"testing"

	"tick-data-platform/internal/archive"
)

func TestValidateRevisionGraphRequiresPredecessorAndRejectsBranch(t *testing.T) {
	scope := layoutTestScope()
	first := emptyRevisionManifest(t, scope, 1, nil, "provisional")
	firstBytes, err := archive.ManifestCanonicalJSON(first)
	if err != nil {
		t.Fatal(err)
	}
	second := emptyRevisionManifest(t, scope, 2, &first, "settled_snapshot")
	secondBytes, err := archive.ManifestCanonicalJSON(second)
	if err != nil {
		t.Fatal(err)
	}
	if same, err := ValidateRevisionGraph(second, secondBytes, nil); same || err == nil {
		t.Fatalf("missing predecessor result same=%v err=%v", same, err)
	}
	if same, err := ValidateRevisionGraph(second, secondBytes, []ManifestRecord{{Manifest: first, Bytes: firstBytes}}); same || err != nil {
		t.Fatalf("valid successor result same=%v err=%v", same, err)
	}
	branch := emptyRevisionManifest(t, scope, 2, &first, "incomplete_sync")
	branchBytes, err := archive.ManifestCanonicalJSON(branch)
	if err != nil {
		t.Fatal(err)
	}
	if same, err := ValidateRevisionGraph(branch, branchBytes, []ManifestRecord{
		{Manifest: first, Bytes: firstBytes},
		{Manifest: second, Bytes: secondBytes},
	}); same || !errors.Is(err, ErrPublisherConflict) {
		t.Fatalf("branch result same=%v err=%v, want ErrPublisherConflict", same, err)
	}
}

func TestValidateRevisionGraphAcceptsExactResume(t *testing.T) {
	scope := layoutTestScope()
	first := emptyRevisionManifest(t, scope, 1, nil, "provisional")
	firstBytes, err := archive.ManifestCanonicalJSON(first)
	if err != nil {
		t.Fatal(err)
	}
	if same, err := ValidateRevisionGraph(first, firstBytes, []ManifestRecord{{Manifest: first, Bytes: firstBytes}}); !same || err != nil {
		t.Fatalf("exact resume result same=%v err=%v", same, err)
	}
}

func TestValidateRevisionSuccessorRequiresPrefixAndAllowsForwardExtension(t *testing.T) {
	key := archive.RawWALObjectKey([32]byte{1})
	object := archive.RawObjectRange{Key: key, SHA256: [32]byte{1}, Bytes: 10, StartIngestSequence: 1, EndIngestSequence: 1, FirstRecordOrdinal: 0, LastRecordOrdinal: 0}
	chain := archive.RawChainObject{Key: key, SHA256: [32]byte{1}, Bytes: 10, StartIngestSequence: 1, EndIngestSequence: 1}
	previous := archive.RawDayManifest{
		DatasetID: "dataset", CampaignID: "campaign", DayDefinitionID: "day", Date: "2024-03-09", PublisherID: "publisher", PublisherEpoch: 1, SettlePolicy: "settle", ConfigHash: [32]byte{1},
		Objects: []archive.RawObjectRange{object}, ChainObjects: []archive.RawChainObject{chain}, ChainSliceStartSequence: 1, ChainSliceStartRoot: [32]byte{2}, ChainSliceEndSequence: 1, ChainSliceEndRoot: [32]byte{3}, AcceptedRecordCount: 1, ObservedThroughSourceMSC: 1, ObservedThroughCaptureSeq: 1,
	}
	current := previous
	current.Objects = append([]archive.RawObjectRange(nil), previous.Objects...)
	current.ChainObjects = append([]archive.RawChainObject(nil), previous.ChainObjects...)
	current.ChainSliceEndSequence = 2
	current.ChainSliceEndRoot = [32]byte{4}
	current.AcceptedRecordCount = 2
	current.ObservedThroughSourceMSC = 2
	current.ObservedThroughCaptureSeq = 2
	if err := validateRevisionSuccessor(previous, current); err != nil {
		t.Fatalf("forward extension rejected: %v", err)
	}
	current.Objects[0].FirstRecordOrdinal = 1
	if err := validateRevisionSuccessor(previous, current); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("changed object prefix error = %v, want ErrIntegrity", err)
	}
}

func emptyRevisionManifest(t *testing.T, scope archive.ScopeConfig, revision uint64, previous *archive.RawDayManifest, completeness string) archive.RawDayManifest {
	t.Helper()
	manifest, err := archive.BuildRawDayManifest(archive.RawDayManifestInput{
		Scope:              scope,
		Date:               "2024-03-09",
		Revision:           revision,
		Previous:           previous,
		TerminalSyncStatus: "complete",
		CompletenessStatus: completeness,
		LogicalCloseTimeS:  1710028800,
	})
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}
