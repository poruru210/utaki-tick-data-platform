package r2

import (
	"context"
	"crypto/sha256"
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

func TestPublicationJournalListsUnfinishedIntentForRecovery(t *testing.T) {
	journal, err := OpenPublicationJournal(filepath.Join(t.TempDir(), "publication.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	intent := testPublicationIntent(t)
	if _, err := journal.CreateOrGetIntent(intent); err != nil {
		t.Fatal(err)
	}
	unfinished, err := journal.ListUnfinished(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(unfinished) != 1 || unfinished[0].Identity != intent.ManifestKey || unfinished[0].Stage != StageIntent {
		t.Fatalf("unfinished intents = %+v", unfinished)
	}
	input := unfinished[0].Input
	if input.Manifest.ManifestSHA256 != intent.Manifest.ManifestSHA256 || string(input.ManifestBytes) != string(intent.ManifestBytes) || input.ManifestPath != intent.ManifestPath || input.ReceiptPath != intent.ReceiptPath {
		t.Fatalf("recovered input = %+v", input)
	}
	if err := journal.AdvanceStage(intent.ManifestKey, StageReceiptSaved); err != nil {
		t.Fatal(err)
	}
	unfinished, err = journal.ListUnfinished(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(unfinished) != 0 {
		t.Fatalf("terminal intent remained unfinished: %+v", unfinished)
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
	descriptorBytes := []byte(`{"scope":"test"}`)
	descriptorHash := sha256.Sum256(descriptorBytes)
	return PublicationIntent{
		Scope:                 scope,
		Claim:                 claim,
		ClaimKey:              "claim",
		ClaimHash:             claimHash,
		ScopeDescriptorKey:    "descriptor",
		ScopeDescriptorPath:   "descriptor.json",
		ScopeDescriptorSHA256: descriptorHash,
		ScopeDescriptorBytes:  uint64(len(descriptorBytes)),
		ManifestKey:           "manifest",
		Manifest:              manifest,
		ManifestBytes:         manifestBytes,
		ManifestPath:          "manifest.json",
		ReceiptPath:           "receipt.json",
	}
}
