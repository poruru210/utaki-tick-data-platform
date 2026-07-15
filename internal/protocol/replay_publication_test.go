package protocol

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type replayPublicationFixture struct {
	Bundle                  ReplayPublicationBundle `json:"bundle"`
	PublicationBundleDigest string                  `json:"publication_bundle_digest"`
	FinalObservation        ReplayFinalObservation  `json:"final_observation"`
	FinalObservationDigest  string                  `json:"final_observation_digest"`
	NegativeCases           []struct {
		CaseID        string `json:"case_id"`
		Target        string `json:"target"`
		ExpectedError string `json:"expected_error"`
		RawHex        string `json:"raw_hex"`
	} `json:"negative_cases"`
}

func loadReplayPublicationFixture(t *testing.T) replayPublicationFixture {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "tickdata", "golden", "replay-publication-v1-conformance.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixture replayPublicationFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestReplayPublicationContractGolden(t *testing.T) {
	fixture := loadReplayPublicationFixture(t)
	bundleCanonical, err := ReplayPublicationBundleCanonicalJSON(fixture.Bundle)
	if err != nil {
		t.Fatal(err)
	}
	_, bundleDigest, err := VerifyReplayPublicationBundle(bundleCanonical)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(bundleDigest[:]); got != fixture.PublicationBundleDigest {
		t.Fatalf("bundle digest = %s, want %s", got, fixture.PublicationBundleDigest)
	}
	observationCanonical, err := ReplayFinalObservationCanonicalJSON(fixture.FinalObservation, fixture.Bundle)
	if err != nil {
		t.Fatal(err)
	}
	_, observationDigest, err := VerifyReplayFinalObservation(observationCanonical, fixture.Bundle)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(observationDigest[:]); got != fixture.FinalObservationDigest {
		t.Fatalf("final observation digest = %s, want %s", got, fixture.FinalObservationDigest)
	}
}

func TestReplayPublicationContractNegativeGolden(t *testing.T) {
	fixture := loadReplayPublicationFixture(t)
	bundleCanonical, err := ReplayPublicationBundleCanonicalJSON(fixture.Bundle)
	if err != nil {
		t.Fatal(err)
	}
	observationCanonical, err := ReplayFinalObservationCanonicalJSON(fixture.FinalObservation, fixture.Bundle)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range fixture.NegativeCases {
		t.Run(testCase.CaseID, func(t *testing.T) {
			var verifyErr error
			switch testCase.Target {
			case "raw":
				raw, decodeErr := hex.DecodeString(testCase.RawHex)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				_, _, verifyErr = VerifyReplayPublicationBundle(raw)
			case "bundle":
				raw := mutateReplayPublicationBundle(t, bundleCanonical, testCase.CaseID)
				_, _, verifyErr = VerifyReplayPublicationBundle(raw)
			case "observation":
				raw := mutateReplayFinalObservation(t, observationCanonical, testCase.CaseID)
				_, _, verifyErr = VerifyReplayFinalObservation(raw, fixture.Bundle)
			default:
				t.Fatalf("unknown target %q", testCase.Target)
			}
			if got := string(ErrorCodeOf(verifyErr)); got != testCase.ExpectedError {
				t.Fatalf("error code = %q, want %q (error: %v)", got, testCase.ExpectedError, verifyErr)
			}
		})
	}
}

func TestRequiredReplayPublicationRoundsScalesWithPartCount(t *testing.T) {
	for _, testCase := range []struct {
		parts uint64
		want  uint64
	}{
		{parts: 0, want: 2},
		{parts: 1, want: 4},
		{parts: 4, want: 10},
		{parts: 10_000, want: 20_002},
	} {
		got, err := RequiredReplayPublicationRounds(testCase.parts)
		if err != nil || got != testCase.want {
			t.Fatalf("parts=%d: rounds=%d err=%v, want %d", testCase.parts, got, err, testCase.want)
		}
	}
	if _, err := RequiredReplayPublicationRounds(^uint64(0)); ErrorCodeOf(err) != ErrResourceLimit {
		t.Fatalf("overflow rounds error = %v", err)
	}
}

func TestReplayFinalObservationAcceptsProvenEmptyTerminalAndEmptyPredecessor(t *testing.T) {
	fixture := loadReplayPublicationFixture(t)
	original := fixture.FinalObservation.ReplayEdges[0]
	manifest, err := VerifyReplayDayManifest([]byte(original.CanonicalJSON))
	if err != nil {
		t.Fatal(err)
	}
	empty := manifest
	empty.PartManifestKeys = []string{}
	empty.PartSetRoot = [32]byte{}
	empty.CanonicalStreamRowChainRoot = [32]byte{}
	emptyCanonical, err := ReplayDayManifestCanonicalJSON(empty)
	if err != nil {
		t.Fatal(err)
	}
	emptyDigest, _ := ReplayDayManifestDigest(empty)
	emptyRelative, _ := ReplayDayManifestKey(empty)
	emptyEdge := replayTestEdge(empty, fixture.Bundle.Scope.ImmutablePrefix+"/"+emptyRelative, emptyCanonical)

	emptyBundle := fixture.Bundle
	emptyBundle.ParquetObjects = []ReplayPublicationParquetObject{}
	emptyBundle.PartManifests = []ReplayPublicationPartManifest{}
	emptyBundle.PartSetRoot = strings.Repeat("0", 64)
	emptyBundle.CanonicalStreamRowChainRoot = strings.Repeat("0", 64)
	emptyBundle.ReplayManifest = ReplayPublicationReplayManifest{Bytes: uint64(len(emptyCanonical)), DomainDigest: hex.EncodeToString(emptyDigest[:]), FullKey: emptyEdge.FullKey, RelativeKey: emptyRelative, RcloneKey: emptyBundle.Scope.RclonePrefix + "/" + emptyRelative, Revision: 1}
	emptyObservation := replayTestObservation(t, emptyBundle, []ReplayObservedRevisionEdge{emptyEdge})
	if _, err := ReplayFinalObservationCanonicalJSON(emptyObservation, emptyBundle); err != nil {
		t.Fatalf("proven empty terminal rejected: %v", err)
	}

	predecessor := empty
	predecessor.RawDayManifestKey = "old.json"
	predecessor.RawDayManifestSHA256 = [32]byte{0xaa}
	predecessorCanonical, _ := ReplayDayManifestCanonicalJSON(predecessor)
	predecessorDigest, _ := ReplayDayManifestDigest(predecessor)
	predecessorRelative, _ := ReplayDayManifestKey(predecessor)
	predecessorEdge := replayTestEdge(predecessor, fixture.Bundle.Scope.ImmutablePrefix+"/"+predecessorRelative, predecessorCanonical)
	successor := manifest
	successor.Revision = 2
	successor.PreviousManifestSHA256 = &predecessorDigest
	successor.ManifestID = predecessor.ManifestID
	successorCanonical, _ := ReplayDayManifestCanonicalJSON(successor)
	successorDigest, _ := ReplayDayManifestDigest(successor)
	successorRelative, _ := ReplayDayManifestKey(successor)
	successorEdge := replayTestEdge(successor, fixture.Bundle.Scope.ImmutablePrefix+"/"+successorRelative, successorCanonical)
	successorBundle := fixture.Bundle
	successorBundle.ReplayManifest = ReplayPublicationReplayManifest{Bytes: uint64(len(successorCanonical)), DomainDigest: hex.EncodeToString(successorDigest[:]), FullKey: successorEdge.FullKey, RelativeKey: successorRelative, RcloneKey: successorBundle.Scope.RclonePrefix + "/" + successorRelative, Revision: 2}
	successorObservation := replayTestObservation(t, successorBundle, []ReplayObservedRevisionEdge{predecessorEdge, successorEdge})
	if _, err := ReplayFinalObservationCanonicalJSON(successorObservation, successorBundle); err != nil {
		t.Fatalf("proven empty predecessor rejected: %v", err)
	}
	unproven := successorObservation
	unproven.ReplayEdges = append([]ReplayObservedRevisionEdge(nil), successorObservation.ReplayEdges...)
	unproven.ReplayEdges[0].CanonicalJSON = ""
	if _, err := ReplayFinalObservationCanonicalJSON(unproven, successorBundle); err == nil {
		t.Fatal("unproven earlier empty revision accepted")
	}
}

func TestReplayFinalObservationBudgetExactAggregateAndExhaustion(t *testing.T) {
	fixture := loadReplayPublicationFixture(t)
	required, err := ReplayFinalObservationRequiredBytes(fixture.Bundle, fixture.FinalObservation.ReplayEdges)
	if err != nil {
		t.Fatal(err)
	}
	exact := fixture.Bundle
	exact.Limits.MaxObservationBytes = required
	exact.Limits.MaxParquetObjectBytes = exact.ParquetObjects[0].Bytes
	exact.Limits.MaxTotalParquetBytes = exact.ParquetObjects[0].Bytes
	if err := ReplayFinalObservationBudgetFeasible(exact, fixture.FinalObservation.ReplayEdges); err != nil {
		t.Fatalf("exact budget rejected: %v", err)
	}
	exhausted := exact
	exhausted.Limits.MaxObservationBytes = required - 1
	if err := ReplayFinalObservationBudgetFeasible(exhausted, fixture.FinalObservation.ReplayEdges); ErrorCodeOf(err) != ErrResourceLimit {
		t.Fatalf("exhausted aggregate budget error = %v", err)
	}
	overflow := fixture.FinalObservation.ReplayEdges
	overflow = append([]ReplayObservedRevisionEdge(nil), overflow...)
	overflow[0].CanonicalJSON += strings.Repeat("x", 1024)
	if err := ReplayFinalObservationBudgetFeasible(fixture.Bundle, overflow); err == nil {
		t.Fatal("invalid aggregate edge bytes accepted")
	}
}

func replayTestEdge(manifest ReplayDayManifest, fullKey string, canonical []byte) ReplayObservedRevisionEdge {
	digest, _ := ReplayDayManifestDigest(manifest)
	edge := ReplayObservedRevisionEdge{CanonicalJSON: string(canonical), CanonicalStreamRowChainRoot: hex.EncodeToString(manifest.CanonicalStreamRowChainRoot[:]), FullKey: fullKey, ManifestDigest: hex.EncodeToString(digest[:]), PartCount: uint64(len(manifest.PartManifestKeys)), PartSetRoot: hex.EncodeToString(manifest.PartSetRoot[:]), Revision: manifest.Revision}
	if manifest.PreviousManifestSHA256 != nil {
		previous := hex.EncodeToString(manifest.PreviousManifestSHA256[:])
		edge.PreviousManifestDigest = &previous
	}
	return edge
}

func replayTestObservation(t *testing.T, bundle ReplayPublicationBundle, edges []ReplayObservedRevisionEdge) ReplayFinalObservation {
	t.Helper()
	digest, err := ReplayPublicationBundleDigest(bundle)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]ReplayObservedRawObject, len(bundle.RawObjects))
	for index, object := range bundle.RawObjects {
		raw[index] = ReplayObservedRawObject{Bytes: object.Bytes, FullKey: object.FullKey, SHA256: object.SHA256}
	}
	return ReplayFinalObservation{ObservationVersion: ReplayFinalObservationVersion, BundleDigest: hex.EncodeToString(digest[:]), Claim: bundle.Claim, Complete: true, DerivativeObjects: bundle.expectedDerivatives(), ObservationBytes: bundle.Limits.MaxObservationBytes, ObservationRequests: bundle.Limits.MaxObservationRequests, RawManifest: ReplayObservedRawManifest{Bytes: bundle.RawManifest.Bytes, DomainDigest: bundle.RawManifest.DomainDigest, FullKey: bundle.RawManifest.FullKey}, RawObjects: raw, ReplayEdges: edges}
}

func mutateReplayPublicationBundle(t *testing.T, canonical []byte, caseID string) []byte {
	t.Helper()
	if caseID == "duplicate_key" {
		return bytes.Replace(canonical, []byte("{"), []byte(`{"bundle_version":"replay-publication-bundle-v1",`), 1)
	}
	if caseID == "noncanonical_bytes" {
		return bytes.Replace(canonical, []byte("{"), []byte("{ "), 1)
	}
	decoded, err := decodePublicationCanonicalJSON(canonical)
	if err != nil {
		t.Fatal(err)
	}
	value := decoded.(map[string]any)
	switch caseID {
	case "unknown_key":
		value["unknown"] = uint64(1)
	case "zero_digest":
		value["part_set_root"] = string(bytes.Repeat([]byte("0"), 64))
	case "wrong_domain":
		value["claim"].(map[string]any)["domain_digest"] = string(bytes.Repeat([]byte("c"), 64))
	case "wrong_key":
		value["claim"].(map[string]any)["full_key"] = "i/publisher-claims/epoch=8.json"
	case "scope_collision":
		claim := value["claim"].(map[string]any)
		decodedClaim, err := decodePublicationCanonicalJSON([]byte(claim["canonical_json"].(string)))
		if err != nil {
			t.Fatal(err)
		}
		claimValue := decodedClaim.(map[string]any)
		claimValue["dataset_id"] = "x"
		claimCanonical, err := CanonicalJSON(claimValue)
		if err != nil {
			t.Fatal(err)
		}
		claimDigest := PublisherClaimDomainDigest(claimCanonical)
		claim["canonical_json"] = string(claimCanonical)
		claim["domain_digest"] = hex.EncodeToString(claimDigest[:])
	case "raw_claim_missing":
		value["claim"] = map[string]any{"canonical_json": "", "domain_digest": "", "full_key": ""}
	case "oversized_aggregate":
		limits := value["limits"].(map[string]any)
		limits["max_metadata_object_bytes"] = uint64(1000)
		limits["max_total_metadata_bytes"] = uint64(1200)
	default:
		t.Fatalf("unknown bundle mutation %q", caseID)
	}
	raw, err := CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mutateReplayFinalObservation(t *testing.T, canonical []byte, caseID string) []byte {
	t.Helper()
	decoded, err := decodePublicationCanonicalJSON(canonical)
	if err != nil {
		t.Fatal(err)
	}
	value := decoded.(map[string]any)
	edge := func() map[string]any { return value["replay_edges"].([]any)[0].(map[string]any) }
	switch caseID {
	case "incomplete_observation":
		value["complete"] = false
	case "missing_derivative":
		value["derivative_objects"] = []any{}
	case "missing_edge_manifest":
		edge()["canonical_json"] = ""
	case "invalid_edge_manifest":
		edge()["canonical_json"] = "{}"
	case "noncanonical_edge_manifest":
		edge()["canonical_json"] = " " + edge()["canonical_json"].(string)
	case "edge_key_mismatch":
		edge()["full_key"] = "i/wrong.json"
	case "edge_digest_mismatch":
		edge()["manifest_digest"] = strings.Repeat("a", 64)
	case "edge_revision_mismatch":
		edge()["revision"] = uint64(2)
	case "edge_root_mismatch":
		edge()["part_set_root"] = strings.Repeat("a", 64)
	case "terminal_shape_mismatch":
		edge()["part_count"] = edge()["part_count"].(uint64) + 1
	case "nonempty_zero_roots", "mixed_root":
		manifest, decodeErr := decodePublicationCanonicalJSON([]byte(edge()["canonical_json"].(string)))
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		manifestObject := manifest.(map[string]any)
		manifestObject["part_set_root"] = strings.Repeat("0", 64)
		if caseID == "nonempty_zero_roots" {
			manifestObject["canonical_stream_row_chain_root"] = strings.Repeat("0", 64)
		}
		manifestCanonical, canonicalErr := CanonicalJSON(manifestObject)
		if canonicalErr != nil {
			t.Fatal(canonicalErr)
		}
		edge()["canonical_json"] = string(manifestCanonical)
	default:
		t.Fatalf("unknown observation mutation %q", caseID)
	}
	raw, err := CanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
