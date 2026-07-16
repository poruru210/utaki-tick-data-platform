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

func testHandoverArtifact() HandoverArtifact {
	return HandoverArtifact{
		HandoverVersion:               HandoverArtifactVersion,
		DatasetID:                     "fixture-dataset",
		CampaignID:                    "fixture-campaign",
		ScopeKey:                      strings.Repeat("11", 32),
		PriorPublisherEpoch:           7,
		NextPublisherEpoch:            8,
		PriorClaimKey:                 "immutable-root/dataset=fixture/publisher-claims/epoch=7.json",
		PriorClaimDomainDigest:        mustHash("22"),
		ExpectedNextClaimKey:          "immutable-root/dataset=fixture/publisher-claims/epoch=8.json",
		ExpectedNextClaimDomainDigest: mustHash("33"),
		TransitionKey:                 "immutable-root/dataset=fixture/handover-transitions/next-epoch=8.json",
		OperatorEvidenceDigest:        mustHash("44"),
	}
}

func mustHash(prefix string) [32]byte {
	value, err := hex.DecodeString(strings.Repeat(prefix, 32))
	if err != nil {
		panic(err)
	}
	var result [32]byte
	copy(result[:], value)
	return result
}

func TestHandoverArtifactCanonicalRoundTrip(t *testing.T) {
	artifact := testHandoverArtifact()
	canonical, err := artifact.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, digest, err := VerifyHandoverArtifact(canonical)
	if err != nil {
		t.Fatal(err)
	}
	want, err := decoded.CanonicalJSON()
	if err != nil || !bytes.Equal(canonical, want) {
		t.Fatalf("canonical round trip changed bytes: %q != %q; err=%v", canonical, want, err)
	}
	wantDigest, err := artifact.Digest()
	if err != nil || digest != wantDigest {
		t.Fatalf("digest = %x, want %x; err=%v", digest, wantDigest, err)
	}
	key, err := HandoverArtifactKey("immutable-root/dataset=fixture", artifact.NextPublisherEpoch)
	if err != nil || key != "immutable-root/dataset=fixture/handover/next-epoch=8.json" {
		t.Fatalf("handover key = %q; err=%v", key, err)
	}
}

func TestHandoverArtifactGolden(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "tickdata", "golden", "publisher-handover-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		ImmutablePrefix string `json:"immutable_prefix"`
		ArtifactKey     string `json:"artifact_key"`
		CanonicalJSON   string `json:"canonical_json"`
		ArtifactDigest  string `json:"artifact_digest"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	artifact, digest, err := VerifyHandoverArtifactBinding([]byte(fixture.CanonicalJSON), fixture.ImmutablePrefix, strings.Repeat("11", 32), fixture.ArtifactKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := EncodeHashHex(digest); got != fixture.ArtifactDigest {
		t.Fatalf("golden digest = %s, want %s", got, fixture.ArtifactDigest)
	}
	key, err := HandoverArtifactKey(fixture.ImmutablePrefix, artifact.NextPublisherEpoch)
	if err != nil || key != fixture.ArtifactKey {
		t.Fatalf("golden key = %q, want %q; err=%v", key, fixture.ArtifactKey, err)
	}
}

func TestHandoverArtifactBindingRejectsWrongScope(t *testing.T) {
	artifact := testHandoverArtifact()
	canonical, err := CanonicalJSON(artifact.Value())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := VerifyHandoverArtifactBinding(canonical, "immutable-root/dataset=fixture", strings.Repeat("aa", 32), "immutable-root/dataset=fixture/handover/next-epoch=8.json"); ErrorCodeOf(err) != ErrScopeCollision {
		t.Fatalf("wrong scope error = %v", err)
	}
	artifact.PriorClaimKey = strings.Replace(artifact.PriorClaimKey, "immutable-root", "other-root", 1)
	canonical, err = CanonicalJSON(artifact.Value())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := VerifyHandoverArtifactBinding(canonical, "immutable-root/dataset=fixture", strings.Repeat("11", 32), "immutable-root/dataset=fixture/handover/next-epoch=8.json"); ErrorCodeOf(err) != ErrWrongKey {
		t.Fatalf("wrong prefix error = %v", err)
	}
}

func TestHandoverArtifactRejectsStrictNegativeCases(t *testing.T) {
	base, err := testHandoverArtifact().CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(HandoverArtifact) HandoverArtifact
		want   ErrorCode
	}{
		{"same_epoch", func(value HandoverArtifact) HandoverArtifact {
			value.NextPublisherEpoch = value.PriorPublisherEpoch
			return value
		}, ErrInvalidField},
		{"regression", func(value HandoverArtifact) HandoverArtifact {
			value.NextPublisherEpoch = value.PriorPublisherEpoch - 1
			return value
		}, ErrInvalidField},
		{"wrong_key", func(value HandoverArtifact) HandoverArtifact {
			value.ExpectedNextClaimKey = strings.Replace(value.ExpectedNextClaimKey, "epoch=8", "epoch=7", 1)
			return value
		}, ErrWrongKey},
		{"zero_digest", func(value HandoverArtifact) HandoverArtifact { value.OperatorEvidenceDigest = [32]byte{}; return value }, ErrZeroDigest},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			value := testCase.mutate(testHandoverArtifact())
			canonical, err := CanonicalJSON(value.Value())
			if err != nil {
				t.Fatal(err)
			}
			_, _, verifyErr := VerifyHandoverArtifact(canonical)
			if got := ErrorCodeOf(verifyErr); got != testCase.want {
				t.Fatalf("error code = %q, want %q; err=%v", got, testCase.want, verifyErr)
			}
		})
	}
	unknownValue := testHandoverArtifact().Value()
	unknownValue["unknown"] = uint64(1)
	unknown, err := CanonicalJSON(unknownValue)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := VerifyHandoverArtifact(unknown); ErrorCodeOf(err) != ErrUnknownField {
		t.Fatalf("unknown field error = %v", err)
	}
	duplicate := bytes.Replace(base, []byte(`"campaign_id":"fixture-campaign"`), []byte(`"campaign_id":"fixture-campaign","campaign_id":"fixture-campaign"`), 1)
	if _, _, err := VerifyHandoverArtifact(duplicate); ErrorCodeOf(err) != ErrDuplicateField {
		t.Fatalf("duplicate field error = %v", err)
	}
}
