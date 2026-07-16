package protocol

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
)

const (
	HandoverArtifactVersion = "publisher-handover-v1"
	HandoverArtifactDomain  = "tick-data-platform/publisher-handover/v1\x00"
)

var handoverArtifactKeys = map[string]bool{
	"campaign_id":                       true,
	"dataset_id":                        true,
	"expected_next_claim_domain_digest": true,
	"expected_next_claim_key":           true,
	"handover_version":                  true,
	"next_publisher_epoch":              true,
	"operator_evidence_digest":          true,
	"prior_claim_domain_digest":         true,
	"prior_claim_key":                   true,
	"prior_publisher_epoch":             true,
	"scope_key":                         true,
	"transition_key":                    true,
}

// HandoverArtifact is the immutable, secret-free remote record that binds one
// publisher epoch to the expected next epoch. Credential values, endpoints,
// and local filesystem paths are intentionally absent from this type.
type HandoverArtifact struct {
	HandoverVersion               string
	DatasetID                     string
	CampaignID                    string
	ScopeKey                      string
	PriorPublisherEpoch           uint64
	NextPublisherEpoch            uint64
	PriorClaimKey                 string
	PriorClaimDomainDigest        [32]byte
	ExpectedNextClaimKey          string
	ExpectedNextClaimDomainDigest [32]byte
	TransitionKey                 string
	OperatorEvidenceDigest        [32]byte
}

func (h HandoverArtifact) Value() map[string]any {
	return map[string]any{
		"campaign_id":                       h.CampaignID,
		"dataset_id":                        h.DatasetID,
		"expected_next_claim_domain_digest": EncodeHashHex(h.ExpectedNextClaimDomainDigest),
		"expected_next_claim_key":           h.ExpectedNextClaimKey,
		"handover_version":                  h.HandoverVersion,
		"next_publisher_epoch":              h.NextPublisherEpoch,
		"operator_evidence_digest":          EncodeHashHex(h.OperatorEvidenceDigest),
		"prior_claim_domain_digest":         EncodeHashHex(h.PriorClaimDomainDigest),
		"prior_claim_key":                   h.PriorClaimKey,
		"prior_publisher_epoch":             h.PriorPublisherEpoch,
		"scope_key":                         h.ScopeKey,
		"transition_key":                    h.TransitionKey,
	}
}

func (h HandoverArtifact) Validate() error {
	if h.HandoverVersion != HandoverArtifactVersion {
		return newError(ErrInvalidField, "invalid handover version")
	}
	for name, value := range map[string]string{
		"dataset_id":  h.DatasetID,
		"campaign_id": h.CampaignID,
	} {
		if err := validatePublicationString(name, value); err != nil {
			return err
		}
	}
	if h.PriorPublisherEpoch == 0 || h.NextPublisherEpoch == 0 || h.NextPublisherEpoch <= h.PriorPublisherEpoch {
		return newError(ErrInvalidField, "handover epochs must increase strictly")
	}
	if err := validateNonzeroHash("scope_key", h.ScopeKey); err != nil {
		return err
	}
	if err := validateHandoverFullKey("prior_claim_key", h.PriorClaimKey); err != nil {
		return err
	}
	if err := validateHandoverFullKey("expected_next_claim_key", h.ExpectedNextClaimKey); err != nil {
		return err
	}
	if err := validateHandoverFullKey("transition_key", h.TransitionKey); err != nil {
		return err
	}
	if !strings.HasSuffix(h.PriorClaimKey, "/publisher-claims/epoch="+strconv.FormatUint(h.PriorPublisherEpoch, 10)+".json") {
		return newError(ErrWrongKey, "prior claim key does not bind prior epoch")
	}
	if !strings.HasSuffix(h.ExpectedNextClaimKey, "/publisher-claims/epoch="+strconv.FormatUint(h.NextPublisherEpoch, 10)+".json") {
		return newError(ErrWrongKey, "expected next claim key does not bind next epoch")
	}
	if !strings.HasSuffix(h.TransitionKey, "/handover-transitions/next-epoch="+strconv.FormatUint(h.NextPublisherEpoch, 10)+".json") {
		return newError(ErrWrongKey, "transition key does not bind next epoch")
	}
	if err := validateNonzeroHash("prior_claim_domain_digest", EncodeHashHex(h.PriorClaimDomainDigest)); err != nil {
		return err
	}
	if err := validateNonzeroHash("expected_next_claim_domain_digest", EncodeHashHex(h.ExpectedNextClaimDomainDigest)); err != nil {
		return err
	}
	if err := validateNonzeroHash("operator_evidence_digest", EncodeHashHex(h.OperatorEvidenceDigest)); err != nil {
		return err
	}
	return nil
}

func validateHandoverFullKey(name, value string) error {
	if value == "" || len(value) > MaxPathBytes || strings.HasPrefix(value, "/") || strings.ContainsAny(value, "\\\r\n") || strings.Contains(value, "//") {
		return newError(ErrWrongKey, "%s is not a canonical full key", name)
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || component == "." || component == ".." {
			return newError(ErrWrongKey, "%s contains a forbidden key component", name)
		}
	}
	return nil
}

func (h HandoverArtifact) CanonicalJSON() ([]byte, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}
	return CanonicalJSON(h.Value())
}

func HandoverArtifactDigest(canonical []byte) [32]byte {
	return sha256.Sum256(append([]byte(HandoverArtifactDomain), canonical...))
}

func (h HandoverArtifact) Digest() ([32]byte, error) {
	canonical, err := h.CanonicalJSON()
	if err != nil {
		return [32]byte{}, err
	}
	return HandoverArtifactDigest(canonical), nil
}

// HandoverArtifactKey returns the only canonical relative layout for a
// handover artifact under one already-trusted immutable campaign prefix.
func HandoverArtifactKey(immutablePrefix string, nextEpoch uint64) (string, error) {
	if err := validatePrefix("immutable_prefix", immutablePrefix); err != nil {
		return "", err
	}
	if nextEpoch == 0 {
		return "", newError(ErrInvalidField, "next publisher epoch is zero")
	}
	return immutablePrefix + "/handover/next-epoch=" + strconv.FormatUint(nextEpoch, 10) + ".json", nil
}

func HandoverTransitionKey(immutablePrefix string, nextEpoch uint64) (string, error) {
	if err := validatePrefix("immutable_prefix", immutablePrefix); err != nil {
		return "", err
	}
	if nextEpoch == 0 {
		return "", newError(ErrInvalidField, "next publisher epoch is zero")
	}
	return immutablePrefix + "/handover-transitions/next-epoch=" + strconv.FormatUint(nextEpoch, 10) + ".json", nil
}

// VerifyHandoverArtifact strictly decodes one canonical handover object and
// returns its domain-separated digest.
func VerifyHandoverArtifact(data []byte) (HandoverArtifact, [32]byte, error) {
	value, err := decodePublicationCanonicalJSON(data)
	if err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if rawObject, ok := value.(map[string]any); ok {
		for key := range rawObject {
			if !handoverArtifactKeys[key] {
				return HandoverArtifact{}, [32]byte{}, newError(ErrUnknownField, "unknown handover field %q", key)
			}
		}
	}
	object, err := exactObject(value, handoverArtifactKeys)
	if err != nil {
		if strings.Contains(err.Error(), "unknown") {
			return HandoverArtifact{}, [32]byte{}, newError(ErrUnknownField, "%v", err)
		}
		return HandoverArtifact{}, [32]byte{}, newError(ErrInvalidField, "%v", err)
	}
	readString := func(name string) (string, error) {
		value, ok := object[name].(string)
		if !ok {
			return "", newError(ErrInvalidField, "%s must be a string", name)
		}
		return value, nil
	}
	readUint := func(name string) (uint64, error) {
		value, ok := object[name].(uint64)
		if !ok {
			return 0, newError(ErrInvalidField, "%s must be a non-negative integer", name)
		}
		return value, nil
	}
	readHash := func(name string) ([32]byte, error) {
		value, err := readString(name)
		if err != nil {
			return [32]byte{}, err
		}
		hash, err := ParseHashHex(value)
		if err != nil {
			return [32]byte{}, newError(ErrInvalidField, "%s is not lowercase SHA-256", name)
		}
		if hash == ([32]byte{}) {
			return [32]byte{}, newError(ErrZeroDigest, "%s is zero", name)
		}
		return hash, nil
	}
	var result HandoverArtifact
	if result.HandoverVersion, err = readString("handover_version"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.DatasetID, err = readString("dataset_id"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.CampaignID, err = readString("campaign_id"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.ScopeKey, err = readString("scope_key"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.PriorPublisherEpoch, err = readUint("prior_publisher_epoch"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.NextPublisherEpoch, err = readUint("next_publisher_epoch"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.PriorClaimKey, err = readString("prior_claim_key"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.ExpectedNextClaimKey, err = readString("expected_next_claim_key"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.TransitionKey, err = readString("transition_key"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.PriorClaimDomainDigest, err = readHash("prior_claim_domain_digest"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.ExpectedNextClaimDomainDigest, err = readHash("expected_next_claim_domain_digest"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if result.OperatorEvidenceDigest, err = readHash("operator_evidence_digest"); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if err := result.Validate(); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	canonical, err := result.CanonicalJSON()
	if err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if string(canonical) != string(data) {
		return HandoverArtifact{}, [32]byte{}, newError(ErrInvalidCanonicalJSON, "handover bytes are not canonical")
	}
	return result, HandoverArtifactDigest(canonical), nil
}

// VerifyHandoverArtifactBinding verifies the artifact against the trusted
// immutable campaign prefix and scope selected by the caller. The generic
// decoder cannot infer this external trust context, so publication and
// reconciliation must use this binding check before accepting remote state.
func VerifyHandoverArtifactBinding(data []byte, immutablePrefix, expectedScopeKey, expectedArtifactKey string) (HandoverArtifact, [32]byte, error) {
	artifact, digest, err := VerifyHandoverArtifact(data)
	if err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if err := validatePrefix("immutable_prefix", immutablePrefix); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if err := validateNonzeroHash("expected scope_key", expectedScopeKey); err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if artifact.ScopeKey != expectedScopeKey {
		return HandoverArtifact{}, [32]byte{}, newError(ErrScopeCollision, "handover scope key differs from trusted scope")
	}
	wantArtifactKey, err := HandoverArtifactKey(immutablePrefix, artifact.NextPublisherEpoch)
	if err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if expectedArtifactKey != wantArtifactKey {
		return HandoverArtifact{}, [32]byte{}, newError(ErrWrongKey, "expected handover artifact key is not the trusted derivation")
	}
	wantPriorClaim := immutablePrefix + "/publisher-claims/epoch=" + strconv.FormatUint(artifact.PriorPublisherEpoch, 10) + ".json"
	wantNextClaim := immutablePrefix + "/publisher-claims/epoch=" + strconv.FormatUint(artifact.NextPublisherEpoch, 10) + ".json"
	wantTransition, err := HandoverTransitionKey(immutablePrefix, artifact.NextPublisherEpoch)
	if err != nil {
		return HandoverArtifact{}, [32]byte{}, err
	}
	if artifact.PriorClaimKey != wantPriorClaim || artifact.ExpectedNextClaimKey != wantNextClaim || artifact.TransitionKey != wantTransition {
		return HandoverArtifact{}, [32]byte{}, newError(ErrWrongKey, "handover keys are outside the trusted campaign prefix")
	}
	return artifact, digest, nil
}

// EncodeHashHex is shared by canonical operational contracts.
func EncodeHashHex(value [32]byte) string {
	return fmt.Sprintf("%064x", value)
}
