package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sort"

	"tick-data-platform/internal/operations"
	"tick-data-platform/internal/protocol"
)

const handoverEvidenceDomain = "tick-data-platform/operator-handover-evidence/v1\x00"
const handoverConfirmationDomain = "tick-data-platform/operator-handover-confirmation/v1\x00"

// ProcessStopEvidence and CredentialRevocationEvidence are typed, secret-free
// operator assertions. They contain stable digests and timestamps, never a
// process path, endpoint, credential value, or token.
type ProcessStopEvidence struct {
	EvidenceVersion       string
	ScopeKey              string
	PriorPublisherEpoch   uint64
	RuntimeIdentityDigest [32]byte
	ObservedAtUnixMS      uint64
	Stopped               bool
}

type CredentialRevocationEvidence struct {
	EvidenceVersion    string
	ScopeKey           string
	CredentialIDDigest [32]byte
	ScopeDigest        [32]byte
	RevokedAtUnixMS    uint64
	Revoked            bool
}

type HandoverOperatorEvidence struct {
	EvidenceVersion string
	ScopeKey        string
	PriorEpoch      uint64
	Process         ProcessStopEvidence
	Credential      CredentialRevocationEvidence
}

// HandoverConfirmationRecord is a separate operator acknowledgement. Typed
// process/credential evidence says what adapters observed; this record says an
// operator approved this exact sealed handover. It contains only digests and
// timestamps, never a token, endpoint, or local path.
type HandoverConfirmationRecord struct {
	ConfirmationVersion string
	ScopeKey            string
	PriorEpoch          uint64
	SealDigest          [32]byte
	OperatorIDDigest    [32]byte
	ConfirmedAtUnixMS   uint64
	Confirmed           bool
}

func (r HandoverConfirmationRecord) Value() map[string]any {
	return map[string]any{
		"confirmation_version": r.ConfirmationVersion,
		"confirmed":            r.Confirmed,
		"confirmed_at_unix_ms": r.ConfirmedAtUnixMS,
		"operator_id_digest":   protocol.EncodeHashHex(r.OperatorIDDigest),
		"prior_epoch":          r.PriorEpoch,
		"scope_key":            r.ScopeKey,
		"seal_digest":          protocol.EncodeHashHex(r.SealDigest),
	}
}

func (r HandoverConfirmationRecord) Validate(scopeKey string, priorEpoch uint64, sealDigest [32]byte) error {
	if r.ConfirmationVersion != "operator-handover-confirmation-v1" || r.ScopeKey != scopeKey || r.PriorEpoch != priorEpoch || priorEpoch == 0 || r.SealDigest != sealDigest || r.SealDigest == ([32]byte{}) || r.OperatorIDDigest == ([32]byte{}) || r.ConfirmedAtUnixMS == 0 || !r.Confirmed {
		return fmt.Errorf("handover operator confirmation is incomplete or for a different seal")
	}
	if _, err := protocol.ParseHashHex(scopeKey); err != nil {
		return fmt.Errorf("handover confirmation scope key is invalid")
	}
	return nil
}

func (r HandoverConfirmationRecord) CanonicalJSON(scopeKey string, priorEpoch uint64, sealDigest [32]byte) ([]byte, error) {
	if err := r.Validate(scopeKey, priorEpoch, sealDigest); err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(r.Value())
}

func (r HandoverConfirmationRecord) Digest(scopeKey string, priorEpoch uint64, sealDigest [32]byte) ([32]byte, error) {
	canonical, err := r.CanonicalJSON(scopeKey, priorEpoch, sealDigest)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(handoverConfirmationDomain), canonical...)), nil
}

func (e HandoverOperatorEvidence) Value() map[string]any {
	return map[string]any{
		"credential": map[string]any{
			"credential_id_digest": protocol.EncodeHashHex(e.Credential.CredentialIDDigest),
			"evidence_version":     e.Credential.EvidenceVersion,
			"revoked":              e.Credential.Revoked,
			"revoked_at_unix_ms":   e.Credential.RevokedAtUnixMS,
			"scope_digest":         protocol.EncodeHashHex(e.Credential.ScopeDigest),
			"scope_key":            e.Credential.ScopeKey,
		},
		"evidence_version": e.EvidenceVersion,
		"prior_epoch":      e.PriorEpoch,
		"process": map[string]any{
			"evidence_version":        e.Process.EvidenceVersion,
			"observed_at_unix_ms":     e.Process.ObservedAtUnixMS,
			"prior_publisher_epoch":   e.Process.PriorPublisherEpoch,
			"runtime_identity_digest": protocol.EncodeHashHex(e.Process.RuntimeIdentityDigest),
			"scope_key":               e.Process.ScopeKey,
			"stopped":                 e.Process.Stopped,
		},
		"scope_key": e.ScopeKey,
	}
}

func (e HandoverOperatorEvidence) Validate(scopeKey string, priorEpoch uint64) error {
	if e.EvidenceVersion != "operator-handover-evidence-v1" || e.ScopeKey != scopeKey || e.PriorEpoch != priorEpoch || priorEpoch == 0 {
		return fmt.Errorf("handover operator evidence identity is invalid")
	}
	if _, err := protocol.ParseHashHex(scopeKey); err != nil {
		return fmt.Errorf("handover operator scope key is invalid")
	}
	if e.Process.EvidenceVersion != "process-stop-evidence-v1" || e.Process.ScopeKey != scopeKey || e.Process.PriorPublisherEpoch != priorEpoch || e.Process.RuntimeIdentityDigest == ([32]byte{}) || e.Process.ObservedAtUnixMS == 0 || !e.Process.Stopped {
		return fmt.Errorf("old process stop evidence is incomplete")
	}
	if e.Credential.EvidenceVersion != "credential-revocation-evidence-v1" || e.Credential.ScopeKey != scopeKey || e.Credential.CredentialIDDigest == ([32]byte{}) || e.Credential.ScopeDigest == ([32]byte{}) || e.Credential.RevokedAtUnixMS == 0 || !e.Credential.Revoked {
		return fmt.Errorf("old credential revocation evidence is incomplete")
	}
	return nil
}

func (e HandoverOperatorEvidence) CanonicalJSON() ([]byte, error) {
	if err := e.Validate(e.ScopeKey, e.PriorEpoch); err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(e.Value())
}

func (e HandoverOperatorEvidence) Digest() ([32]byte, error) {
	canonical, err := e.CanonicalJSON()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte(handoverEvidenceDomain), canonical...)), nil
}

type HandoverSeal struct {
	ImmutablePrefix    string
	ScopeKey           string
	ArtifactKey        string
	TransitionKey      string
	Artifact           protocol.HandoverArtifact
	CanonicalBytes     []byte
	Digest             [32]byte
	PriorClaimBytes    []byte
	NextClaimBytes     []byte
	NextClaimDomainSHA [32]byte
}

// SealHandover derives every remote identity from the trusted Layout. The
// caller supplies only the next epoch and typed operator evidence; no remote
// key is accepted as authority.
func SealHandover(layout Layout, nextEpoch uint64, evidence HandoverOperatorEvidence) (HandoverSeal, error) {
	priorClaim, err := NewPublisherClaim(layout.Scope)
	if err != nil {
		return HandoverSeal{}, err
	}
	priorBytes, err := priorClaim.CanonicalJSON()
	if err != nil {
		return HandoverSeal{}, err
	}
	priorDigest, err := priorClaim.Digest()
	if err != nil {
		return HandoverSeal{}, err
	}
	if nextEpoch <= layout.Scope.PublisherEpoch {
		return HandoverSeal{}, fmt.Errorf("next publisher epoch must increase strictly")
	}
	if err := evidence.Validate(priorClaim.ScopeKey, layout.Scope.PublisherEpoch); err != nil {
		return HandoverSeal{}, err
	}
	nextScope := layout.Scope
	nextScope.PublisherEpoch = nextEpoch
	nextClaim, err := NewPublisherClaim(nextScope)
	if err != nil {
		return HandoverSeal{}, err
	}
	nextBytes, err := nextClaim.CanonicalJSON()
	if err != nil {
		return HandoverSeal{}, err
	}
	nextDigest, err := nextClaim.Digest()
	if err != nil {
		return HandoverSeal{}, err
	}
	evidenceDigest, err := evidence.Digest()
	if err != nil {
		return HandoverSeal{}, err
	}
	configHash, err := layout.Scope.ConfigHash()
	if err != nil || evidence.Credential.ScopeDigest != configHash {
		return HandoverSeal{}, fmt.Errorf("credential revocation evidence is for a different scope")
	}
	priorClaimKey, err := layout.ClaimKey(layout.Scope.PublisherEpoch)
	if err != nil {
		return HandoverSeal{}, err
	}
	nextClaimKey, err := layout.ClaimKey(nextEpoch)
	if err != nil {
		return HandoverSeal{}, err
	}
	artifactKey, err := layout.HandoverArtifactKey(nextEpoch)
	if err != nil {
		return HandoverSeal{}, err
	}
	transitionKey, err := layout.HandoverTransitionKey(nextEpoch)
	if err != nil {
		return HandoverSeal{}, err
	}
	artifact := protocol.HandoverArtifact{
		HandoverVersion:               protocol.HandoverArtifactVersion,
		DatasetID:                     layout.Scope.DatasetID,
		CampaignID:                    layout.Scope.CampaignID,
		ScopeKey:                      priorClaim.ScopeKey,
		PriorPublisherEpoch:           layout.Scope.PublisherEpoch,
		NextPublisherEpoch:            nextEpoch,
		PriorClaimKey:                 priorClaimKey,
		PriorClaimDomainDigest:        priorDigest,
		ExpectedNextClaimKey:          nextClaimKey,
		ExpectedNextClaimDomainDigest: nextDigest,
		TransitionKey:                 transitionKey,
		OperatorEvidenceDigest:        evidenceDigest,
	}
	canonical, err := artifact.CanonicalJSON()
	if err != nil {
		return HandoverSeal{}, err
	}
	digest := protocol.HandoverArtifactDigest(canonical)
	if _, boundDigest, err := protocol.VerifyHandoverArtifactBinding(canonical, layout.ImmutableCampaignPrefix(), priorClaim.ScopeKey, artifactKey); err != nil || boundDigest != digest {
		if err != nil {
			return HandoverSeal{}, fmt.Errorf("handover artifact binding: %w", err)
		}
		return HandoverSeal{}, fmt.Errorf("handover artifact digest changed during binding")
	}
	return HandoverSeal{
		ImmutablePrefix:    layout.ImmutableCampaignPrefix(),
		ScopeKey:           priorClaim.ScopeKey,
		ArtifactKey:        artifactKey,
		TransitionKey:      transitionKey,
		Artifact:           artifact,
		CanonicalBytes:     append([]byte(nil), canonical...),
		Digest:             digest,
		PriorClaimBytes:    append([]byte(nil), priorBytes...),
		NextClaimBytes:     append([]byte(nil), nextBytes...),
		NextClaimDomainSHA: nextDigest,
	}, nil
}

func (s HandoverSeal) Validate() error {
	if s.ImmutablePrefix == "" || s.ScopeKey == "" || s.ArtifactKey == "" || s.TransitionKey == "" || len(s.CanonicalBytes) == 0 || len(s.PriorClaimBytes) == 0 || len(s.NextClaimBytes) == 0 {
		return fmt.Errorf("handover seal is incomplete")
	}
	if err := s.Artifact.Validate(); err != nil {
		return err
	}
	canonical, err := s.Artifact.CanonicalJSON()
	if err != nil || !bytes.Equal(canonical, s.CanonicalBytes) {
		return fmt.Errorf("handover seal bytes are not canonical")
	}
	if protocol.HandoverArtifactDigest(canonical) != s.Digest {
		return fmt.Errorf("handover seal digest differs")
	}
	if s.TransitionKey != s.Artifact.TransitionKey || protocol.PublisherClaimDomainDigest(s.PriorClaimBytes) != s.Artifact.PriorClaimDomainDigest || protocol.PublisherClaimDomainDigest(s.NextClaimBytes) != s.Artifact.ExpectedNextClaimDomainDigest || s.NextClaimDomainSHA != s.Artifact.ExpectedNextClaimDomainDigest {
		return fmt.Errorf("handover claim bytes are not bound to artifact")
	}
	if _, _, err := protocol.VerifyHandoverArtifactBinding(s.CanonicalBytes, s.ImmutablePrefix, s.ScopeKey, s.ArtifactKey); err != nil {
		return err
	}
	return nil
}

type HandoverRemoteObservation struct {
	SealDigest         [32]byte
	PriorClaim         ObservationClass
	Artifact           ObservationClass
	Transition         ObservationClass
	NextClaim          ObservationClass
	CandidateNamespace ObservationClass
	RequestCount       uint64
	ObservationBytes   uint64
}

type handoverObservationBudget struct {
	maxRequests uint64
	maxBytes    uint64
	requests    uint64
	bytes       uint64
}

func newHandoverObservationBudget(limits operations.ResourceLimits) (*handoverObservationBudget, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &handoverObservationBudget{maxRequests: limits.MaxHandoverObservationRequests, maxBytes: limits.MaxHandoverObservationBytes}, nil
}

func (b *handoverObservationBudget) request() bool {
	if b.requests >= b.maxRequests {
		return false
	}
	b.requests++
	return true
}

func (b *handoverObservationBudget) consume(count uint64) bool {
	if count > b.maxBytes-b.bytes {
		b.bytes = b.maxBytes
		return false
	}
	b.bytes += count
	return true
}

// ObserveHandover performs bounded fresh reads only. It does not expose a
// write capability and distinguishes absence, collision, ambiguity, outage,
// and resource exhaustion for the pure reconciler.
func ObserveHandover(ctx context.Context, remote ReplayRemoteReadBackend, layout Layout, seal HandoverSeal, limits operations.ResourceLimits) (HandoverRemoteObservation, error) {
	if remote == nil {
		return HandoverRemoteObservation{}, fmt.Errorf("handover remote reader is nil")
	}
	if err := seal.Validate(); err != nil {
		return HandoverRemoteObservation{}, err
	}
	budget, err := newHandoverObservationBudget(limits)
	if err != nil {
		return HandoverRemoteObservation{}, err
	}
	priorKey, err := layout.ClaimKey(seal.Artifact.PriorPublisherEpoch)
	if err != nil || priorKey != seal.Artifact.PriorClaimKey {
		return HandoverRemoteObservation{}, fmt.Errorf("handover prior claim key is not trusted")
	}
	nextKey, err := layout.ClaimKey(seal.Artifact.NextPublisherEpoch)
	if err != nil || nextKey != seal.Artifact.ExpectedNextClaimKey {
		return HandoverRemoteObservation{}, fmt.Errorf("handover next claim key is not trusted")
	}
	transitionKey, err := layout.HandoverTransitionKey(seal.Artifact.NextPublisherEpoch)
	if err != nil || transitionKey != seal.Artifact.TransitionKey {
		return HandoverRemoteObservation{}, fmt.Errorf("handover transition key is not trusted")
	}
	observation := HandoverRemoteObservation{SealDigest: seal.Digest}
	observation.PriorClaim = observeHandoverObject(ctx, remote, budget, priorKey, seal.PriorClaimBytes, true)
	observation.Artifact = observeHandoverObject(ctx, remote, budget, seal.ArtifactKey, seal.CanonicalBytes, true)
	observation.Transition = observeHandoverObject(ctx, remote, budget, transitionKey, seal.CanonicalBytes, true)
	observation.NextClaim = observeHandoverObject(ctx, remote, budget, nextKey, seal.NextClaimBytes, true)
	observation.CandidateNamespace = observeHandoverCandidateNamespace(ctx, remote, budget, layout, seal.Artifact.NextPublisherEpoch, nextKey)
	observation.RequestCount = budget.requests
	observation.ObservationBytes = budget.bytes
	return observation, nil
}

func observeHandoverObject(ctx context.Context, remote ReplayRemoteReadBackend, budget *handoverObservationBudget, key string, expected []byte, absentIsValid bool) ObservationClass {
	if !budget.request() {
		return ObservationOversized
	}
	capBytes := budget.maxBytes - budget.bytes
	if capBytes == 0 || capBytes > uint64(^uint64(0)>>1)-1 {
		return ObservationOversized
	}
	body, advertised, err := remote.OpenLimited(ctx, key, capBytes)
	if err != nil {
		return classifyReplayRemoteError(err, absentIsValid)
	}
	if body == nil {
		return ObservationUnavailable
	}
	data, readErr := io.ReadAll(io.LimitReader(body, int64(capBytes)+1))
	closeErr := body.Close()
	if !budget.consume(uint64(len(data))) {
		return ObservationOversized
	}
	if readErr != nil || closeErr != nil || advertised < 0 || uint64(len(data)) != uint64(advertised) {
		return ObservationAmbiguous
	}
	if !bytes.Equal(data, expected) {
		return ObservationDifferent
	}
	return ObservationExact
}

func observeHandoverCandidateNamespace(ctx context.Context, remote ReplayRemoteReadBackend, budget *handoverObservationBudget, layout Layout, nextEpoch uint64, expectedKey string) ObservationClass {
	if !budget.request() {
		return ObservationOversized
	}
	prefix, err := layout.HandoverCandidatePrefix(nextEpoch)
	if err != nil {
		return ObservationAmbiguous
	}
	listed, err := remote.ListLimited(ctx, prefix, 2)
	if err != nil {
		return classifyReplayRemoteError(err, true)
	}
	if !listed.Complete {
		return ObservationAmbiguous
	}
	sort.Slice(listed.Objects, func(i, j int) bool { return listed.Objects[i].Key < listed.Objects[j].Key })
	if len(listed.Objects) == 0 {
		return ObservationAbsent
	}
	if len(listed.Objects) != 1 || listed.Objects[0].Key != expectedKey {
		return ObservationAmbiguous
	}
	return ObservationExact
}

type HandoverDecisionKind string

const (
	HandoverExecute       HandoverDecisionKind = "Execute"
	HandoverRetry         HandoverDecisionKind = "Retry"
	HandoverIntegrityStop HandoverDecisionKind = "IntegrityStop"
	HandoverResourceStop  HandoverDecisionKind = "ResourceStop"
	HandoverReady         HandoverDecisionKind = "Ready"
)

type HandoverActionKind string

const (
	HandoverCreateArtifact   HandoverActionKind = "CreateArtifact"
	HandoverCreateTransition HandoverActionKind = "CreateTransition"
	HandoverCreateNextClaim  HandoverActionKind = "CreateNextClaim"
)

type HandoverAction struct {
	Kind HandoverActionKind
}

type HandoverDecision struct {
	Kind       HandoverDecisionKind
	Actions    []HandoverAction
	StopClass  ObservationClass
	ReasonCode string
	SealDigest [32]byte
}

// ReconcileHandover is pure: no clock, filesystem, backend, credential, or
// process mutation is consulted. Actions contain no caller-selected key.
func ReconcileHandover(seal HandoverSeal, observation HandoverRemoteObservation, evidence HandoverOperatorEvidence, confirmation HandoverConfirmationRecord) (HandoverDecision, error) {
	if err := seal.Validate(); err != nil {
		return HandoverDecision{}, err
	}
	decision := HandoverDecision{SealDigest: seal.Digest}
	if observation.SealDigest != seal.Digest {
		return handoverDecision(decision, HandoverIntegrityStop, ObservationDifferent, "seal_digest_mismatch"), nil
	}
	if err := evidence.Validate(seal.ScopeKey, seal.Artifact.PriorPublisherEpoch); err != nil {
		return handoverDecision(decision, HandoverIntegrityStop, ObservationDifferent, "operator_evidence_incomplete"), nil
	}
	evidenceDigest, err := evidence.Digest()
	if err != nil || evidenceDigest != seal.Artifact.OperatorEvidenceDigest {
		return handoverDecision(decision, HandoverIntegrityStop, ObservationDifferent, "operator_evidence_mismatch"), nil
	}
	if err := confirmation.Validate(seal.ScopeKey, seal.Artifact.PriorPublisherEpoch, seal.Digest); err != nil {
		return handoverDecision(decision, HandoverIntegrityStop, ObservationDifferent, "operator_confirmation_incomplete"), nil
	}
	if classDecision, done := handoverClassStop(decision, observation.PriorClaim, "prior_claim"); done {
		return classDecision, nil
	}
	if classDecision, done := handoverClassStop(decision, observation.CandidateNamespace, "candidate_namespace"); done {
		return classDecision, nil
	}
	if classDecision, done := handoverClassStop(decision, observation.Artifact, "artifact"); done && observation.Artifact != ObservationAbsent {
		return classDecision, nil
	}
	if classDecision, done := handoverClassStop(decision, observation.Transition, "transition"); done && observation.Transition != ObservationAbsent {
		return classDecision, nil
	}
	if observation.Transition == ObservationAbsent {
		if classDecision, done := handoverClassStop(decision, observation.NextClaim, "next_claim"); done {
			return classDecision, nil
		}
		if observation.CandidateNamespace == ObservationExact {
			return handoverDecision(decision, HandoverIntegrityStop, ObservationAmbiguous, "candidate_namespace_inconsistent"), nil
		}
		if observation.Artifact == ObservationAbsent {
			if observation.NextClaim != ObservationAbsent {
				return handoverDecision(decision, HandoverIntegrityStop, ObservationDifferent, "next_claim_without_artifact"), nil
			}
			return handoverDecisionWithActions(decision, HandoverExecute, ObservationAbsent, "artifact_absent_create", []HandoverAction{{Kind: HandoverCreateArtifact}}), nil
		}
		if observation.NextClaim != ObservationAbsent {
			return handoverDecision(decision, HandoverIntegrityStop, ObservationDifferent, "next_claim_without_transition"), nil
		}
		return handoverDecisionWithActions(decision, HandoverExecute, ObservationAbsent, "transition_absent_create", []HandoverAction{{Kind: HandoverCreateTransition}}), nil
	}
	if observation.Artifact == ObservationAbsent {
		return handoverDecision(decision, HandoverIntegrityStop, ObservationDifferent, "transition_without_artifact"), nil
	}
	if classDecision, done := handoverClassStop(decision, observation.NextClaim, "next_claim"); done {
		return classDecision, nil
	}
	if observation.NextClaim == ObservationAbsent {
		return handoverDecisionWithActions(decision, HandoverExecute, ObservationAbsent, "next_claim_absent_create", []HandoverAction{{Kind: HandoverCreateNextClaim}}), nil
	}
	return handoverDecision(decision, HandoverReady, ObservationExact, "handover_exact"), nil
}

func handoverClassStop(decision HandoverDecision, class ObservationClass, name string) (HandoverDecision, bool) {
	switch class {
	case ObservationExact:
		return decision, false
	case ObservationAbsent:
		if name == "prior_claim" {
			return handoverDecision(decision, HandoverIntegrityStop, class, name+"_absent"), true
		}
		return decision, false
	case ObservationUnavailable:
		return handoverDecision(decision, HandoverRetry, class, name+"_unavailable"), true
	case ObservationOversized:
		return handoverDecision(decision, HandoverResourceStop, class, name+"_oversized"), true
	default:
		return handoverDecision(decision, HandoverIntegrityStop, class, name+"_not_exact"), true
	}
}

func handoverDecision(decision HandoverDecision, kind HandoverDecisionKind, class ObservationClass, reason string) HandoverDecision {
	decision.Kind = kind
	decision.StopClass = class
	decision.ReasonCode = reason
	return decision
}

func handoverDecisionWithActions(decision HandoverDecision, kind HandoverDecisionKind, class ObservationClass, reason string, actions []HandoverAction) HandoverDecision {
	decision = handoverDecision(decision, kind, class, reason)
	decision.Actions = actions
	return decision
}

type HandoverWriteBackend interface {
	PutIfAbsent(ctx context.Context, key string, body []byte) error
}

// ExecuteHandoverActions rechecks the pure reconciliation result immediately
// before writing. A caller cannot manufacture an executable decision that
// skips the artifact/transition/next-claim sequence. A conditional conflict
// is returned to the operator for a fresh observation; it never rolls back an
// earlier transition or reactivates the old epoch.
func ExecuteHandoverActions(ctx context.Context, remote ReplayRemoteReadBackend, writer HandoverWriteBackend, layout Layout, seal HandoverSeal, limits operations.ResourceLimits, evidence HandoverOperatorEvidence, confirmation HandoverConfirmationRecord, decision HandoverDecision) error {
	if remote == nil || writer == nil {
		return fmt.Errorf("handover writer is nil")
	}
	if err := seal.Validate(); err != nil {
		return err
	}
	observation, err := ObserveHandover(ctx, remote, layout, seal, limits)
	if err != nil {
		return err
	}
	expected, err := ReconcileHandover(seal, observation, evidence, confirmation)
	if err != nil {
		return err
	}
	if decision.SealDigest != seal.Digest || decision.Kind != HandoverExecute {
		return fmt.Errorf("handover decision is not executable")
	}
	if expected.Kind != HandoverExecute || decision.ReasonCode != expected.ReasonCode || decision.StopClass != expected.StopClass || !sameHandoverActions(decision.Actions, expected.Actions) {
		if !handoverDecisionAlreadySatisfied(decision, expected, observation) {
			return fmt.Errorf("handover decision is stale or not executable")
		}
		return nil
	}
	for _, action := range decision.Actions {
		var key string
		var body []byte
		switch action.Kind {
		case HandoverCreateArtifact:
			key, body = seal.ArtifactKey, seal.CanonicalBytes
		case HandoverCreateTransition:
			key, body = seal.TransitionKey, seal.CanonicalBytes
		case HandoverCreateNextClaim:
			key, body = seal.Artifact.ExpectedNextClaimKey, seal.NextClaimBytes
		default:
			return fmt.Errorf("unknown handover action %q", action.Kind)
		}
		if err := writer.PutIfAbsent(ctx, key, body); err != nil {
			if !errors.Is(err, ErrObjectExists) {
				return fmt.Errorf("handover conditional action %s: %w", action.Kind, err)
			}
			confirmedObservation, observeErr := ObserveHandover(ctx, remote, layout, seal, limits)
			if observeErr == nil {
				confirmedDecision, reconcileErr := ReconcileHandover(seal, confirmedObservation, evidence, confirmation)
				if reconcileErr == nil && (confirmedDecision.Kind == HandoverExecute || confirmedDecision.Kind == HandoverReady) && handoverActionSatisfied(action.Kind, confirmedObservation) {
					continue
				}
			}
			return fmt.Errorf("handover conditional action %s: %w", action.Kind, err)
		}
	}
	return nil
}

func handoverActionSatisfied(kind HandoverActionKind, observation HandoverRemoteObservation) bool {
	switch kind {
	case HandoverCreateArtifact:
		return observation.Artifact == ObservationExact
	case HandoverCreateTransition:
		return observation.Artifact == ObservationExact && observation.Transition == ObservationExact
	case HandoverCreateNextClaim:
		return observation.Artifact == ObservationExact && observation.Transition == ObservationExact && observation.NextClaim == ObservationExact
	default:
		return false
	}
}

func handoverDecisionAlreadySatisfied(decision, expected HandoverDecision, observation HandoverRemoteObservation) bool {
	if expected.Kind != HandoverExecute && expected.Kind != HandoverReady {
		return false
	}
	if len(decision.Actions) != 1 {
		return false
	}
	return handoverActionSatisfied(decision.Actions[0].Kind, observation)
}

func sameHandoverActions(left, right []HandoverAction) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Kind != right[index].Kind {
			return false
		}
	}
	return true
}
