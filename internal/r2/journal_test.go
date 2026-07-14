package r2

import (
	"errors"
	"path/filepath"
	"testing"

	"tick-data-platform/internal/archive"
)

func TestPublicationJournalAdvanceStageIsIdempotentAndMonotonic(t *testing.T) {
	journal, err := OpenPublicationJournal(filepath.Join(t.TempDir(), "publication.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	intent := testPublicationIntent(t)
	if _, err := journal.CreateOrGetIntent(intent); err != nil {
		t.Fatal(err)
	}
	stages := []string{StageClaimed, StageObjectsCopied, StageObjectsVerified, StageManifestCopied, StageManifestVerified, StageReceiptSaved}
	for _, stage := range stages {
		if err := journal.AdvanceStage(intent.ManifestKey, stage); err != nil {
			t.Fatalf("advance to %s: %v", stage, err)
		}
		if err := journal.AdvanceStage(intent.ManifestKey, stage); err != nil {
			t.Fatalf("repeat advance to %s: %v", stage, err)
		}
		if err := journal.AdvanceStage(intent.ManifestKey, StageClaimed); err != nil {
			t.Fatalf("lower advance from %s: %v", stage, err)
		}
	}
	record, found, err := journal.Record(intent.ManifestKey)
	if err != nil || !found {
		t.Fatalf("record = %+v, found=%v, err=%v", record, found, err)
	}
	if record.Stage != StageReceiptSaved {
		t.Fatalf("final stage = %q, want %q", record.Stage, StageReceiptSaved)
	}
	if err := journal.SetStage(intent.ManifestKey, StageIntent); !errors.Is(err, archive.ErrIntegrity) {
		t.Fatalf("backward SetStage error = %v, want ErrIntegrity", err)
	}
}

func testPublicationIntent(t *testing.T) PublicationIntent {
	t.Helper()
	scope := layoutTestScope()
	claim, err := NewPublisherClaim(scope)
	if err != nil {
		t.Fatal(err)
	}
	claimHash, err := claim.Digest()
	if err != nil {
		t.Fatal(err)
	}
	manifest := emptyRevisionManifest(t, scope, 1, nil, "settled_snapshot")
	manifestBytes, err := archive.ManifestCanonicalJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	return PublicationIntent{
		Scope:                    scope,
		Claim:                    claim,
		ClaimKey:                 "claim",
		ClaimHash:                claimHash,
		ScopeDescriptorKey:       "descriptor",
		ScopeDescriptorRcloneKey: "r2:descriptor",
		ScopeDescriptorPath:      "descriptor.json",
		ManifestKey:              "manifest",
		Manifest:                 manifest,
		ManifestBytes:            manifestBytes,
		ManifestPath:             "manifest.json",
		ReceiptPath:              "receipt.json",
	}
}
