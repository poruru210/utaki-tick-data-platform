package retention

import (
	"crypto/sha256"
	"fmt"
	"sort"

	"tick-data-platform/internal/protocol"
)

const (
	ArtifactWALSegment   = "wal_segment"
	ArtifactRawOutbox    = "raw_outbox"
	ArtifactReplayOutbox = "replay_outbox"
	ArtifactCache        = "cache"

	DiskNormal    DiskClass = "normal"
	DiskHigh      DiskClass = "high"
	DiskCritical  DiskClass = "critical"
	DiskEmergency DiskClass = "emergency"
)

type DiskClass string

// LocalArtifact is the reverified local identity used by the planner and
// later rederived by the executor. It never contains a caller-selected
// absolute path.
type LocalArtifact struct {
	Kind          string
	TrustedPath   string
	Bytes         uint64
	ContentSHA256 [32]byte
	WALRange      *WALRange
	Replay        *ReplayIdentity
	Active        bool
}

func (a LocalArtifact) StableID() (string, error) {
	if a.Kind != ArtifactWALSegment && a.Kind != ArtifactRawOutbox && a.Kind != ArtifactReplayOutbox && a.Kind != ArtifactCache {
		return "", fmt.Errorf("unknown artifact kind %q", a.Kind)
	}
	if err := validateRelativePath(a.TrustedPath); err != nil {
		return "", err
	}
	if a.Bytes == 0 || a.ContentSHA256 == ([32]byte{}) {
		return "", fmt.Errorf("artifact identity is incomplete")
	}
	value := map[string]any{
		"bytes":  a.Bytes,
		"kind":   a.Kind,
		"path":   a.TrustedPath,
		"sha256": protocol.EncodeHashHex(a.ContentSHA256),
	}
	canonical, err := protocol.CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("tick-data-platform/retention-artifact/v1\x00"), canonical...))
	return "artifact-" + protocol.EncodeHashHex(digest), nil
}

type CandidateFact struct {
	Artifact         LocalArtifact
	Proof            *RetentionProof
	FreshRemote      bool
	CoverageVerified bool
	RecoveryRequired bool
}

type PlannerInput struct {
	Candidates            []CandidateFact
	CurrentWallTimeUnixMS uint64
	DurableWallTimeUnixMS uint64
	ActiveRecoveryFloor   uint64
	WALExpectedStart      uint64
	GraceMS               uint64
	MaxCandidates         uint64
	ProofLimits           ProofLimits
	Disk                  DiskClass
}

type PruneAction struct {
	ArtifactID       string
	ArtifactKind     string
	TrustedPath      string
	Bytes            uint64
	ContentSHA256    string
	ProofDigest      string
	WALStartSequence uint64
	WALEndSequence   uint64
}

type BlockedCandidate struct {
	ArtifactID string
	Reason     string
}

type PrunePlan struct {
	PlanVersion           string
	Disk                  DiskClass
	CurrentWallTimeUnixMS uint64
	DurableWallTimeUnixMS uint64
	ActiveRecoveryFloor   uint64
	WALExpectedStart      uint64
	GraceMS               uint64
	Actions               []PruneAction
	Blocked               []BlockedCandidate
	PlanDigest            [32]byte
}

const PrunePlanVersion = "prune-plan-v1"

func validDiskClass(value DiskClass) bool {
	switch value {
	case DiskNormal, DiskHigh, DiskCritical, DiskEmergency:
		return true
	default:
		return false
	}
}

func (p PrunePlan) Value() map[string]any {
	actions := make([]any, len(p.Actions))
	for index, action := range p.Actions {
		actions[index] = map[string]any{
			"artifact_id":        action.ArtifactID,
			"artifact_kind":      action.ArtifactKind,
			"bytes":              action.Bytes,
			"content_sha256":     action.ContentSHA256,
			"proof_digest":       action.ProofDigest,
			"trusted_path":       action.TrustedPath,
			"wal_end_sequence":   action.WALEndSequence,
			"wal_start_sequence": action.WALStartSequence,
		}
	}
	blocked := make([]any, len(p.Blocked))
	for index, item := range p.Blocked {
		blocked[index] = map[string]any{"artifact_id": item.ArtifactID, "reason": item.Reason}
	}
	return map[string]any{
		"actions":                   actions,
		"active_recovery_floor":     p.ActiveRecoveryFloor,
		"blocked":                   blocked,
		"current_wall_time_unix_ms": p.CurrentWallTimeUnixMS,
		"disk_class":                string(p.Disk),
		"durable_wall_time_unix_ms": p.DurableWallTimeUnixMS,
		"grace_ms":                  p.GraceMS,
		"plan_version":              p.PlanVersion,
		"wal_expected_start":        p.WALExpectedStart,
	}
}

func (p PrunePlan) CanonicalJSON() ([]byte, error) {
	return protocol.CanonicalJSON(p.Value())
}

func PrunePlanDigest(canonical []byte) [32]byte {
	return sha256.Sum256(append([]byte(PrunePlanDomain), canonical...))
}

func (p PrunePlan) Digest() ([32]byte, error) {
	canonical, err := p.CanonicalJSON()
	if err != nil {
		return [32]byte{}, err
	}
	return PrunePlanDigest(canonical), nil
}

func proofMatchesArtifact(proof *RetentionProof, artifact LocalArtifact) bool {
	if proof == nil || proof.ArtifactKind != artifact.Kind || proof.TrustedRelativePath != artifact.TrustedPath || proof.Bytes != artifact.Bytes || proof.ContentSHA256 != artifact.ContentSHA256 {
		return false
	}
	if artifact.WALRange != nil && (proof.WALRange == nil || *artifact.WALRange != *proof.WALRange) {
		return false
	}
	if artifact.Replay != nil && (proof.Replay == nil || *artifact.Replay != *proof.Replay) {
		return false
	}
	return (artifact.WALRange == nil) == (proof.WALRange == nil) && (artifact.Replay == nil) == (proof.Replay == nil)
}

func candidateEligibility(candidate CandidateFact, input PlannerInput) (string, bool) {
	if candidate.Artifact.Active {
		return "active_artifact", false
	}
	if candidate.Artifact.Kind != ArtifactWALSegment && candidate.Artifact.Kind != ArtifactRawOutbox {
		return "artifact_policy_unimplemented", false
	}
	if candidate.RecoveryRequired {
		return "recovery_required", false
	}
	if !candidate.FreshRemote {
		return "fresh_observation_missing", false
	}
	if !candidate.CoverageVerified {
		return "manifest_coverage_missing", false
	}
	if candidate.Proof == nil {
		return "retention_proof_missing", false
	}
	if err := candidate.Proof.Validate(); err != nil {
		return "retention_proof_invalid", false
	}
	if candidate.Proof.Limits != input.ProofLimits {
		return "retention_proof_limits_mismatch", false
	}
	if !proofMatchesArtifact(candidate.Proof, candidate.Artifact) {
		return "retention_identity_mismatch", false
	}
	if candidate.Proof.ObservedWallTimeUnixMS > input.CurrentWallTimeUnixMS {
		return "observation_from_future", false
	}
	if candidate.Proof.GraceNotBeforeUnixMS > input.CurrentWallTimeUnixMS {
		return "grace_not_elapsed", false
	}
	if input.GraceMS > ^uint64(0)-candidate.Proof.ObservedWallTimeUnixMS {
		return "grace_not_elapsed", false
	}
	graceNotBefore := candidate.Proof.ObservedWallTimeUnixMS + input.GraceMS
	if candidate.Proof.GraceNotBeforeUnixMS < graceNotBefore {
		return "grace_not_elapsed", false
	}
	if candidate.Artifact.Kind == ArtifactWALSegment && candidate.Artifact.WALRange != nil && input.ActiveRecoveryFloor != 0 && candidate.Artifact.WALRange.EndSequence >= input.ActiveRecoveryFloor {
		return "active_recovery_floor", false
	}
	return "", true
}

func makePruneAction(candidate CandidateFact) (PruneAction, error) {
	id, err := candidate.Artifact.StableID()
	if err != nil {
		return PruneAction{}, err
	}
	proofDigest, err := candidate.Proof.Digest()
	if err != nil {
		return PruneAction{}, err
	}
	action := PruneAction{
		ArtifactID: id, ArtifactKind: candidate.Artifact.Kind, TrustedPath: candidate.Artifact.TrustedPath,
		Bytes: candidate.Artifact.Bytes, ContentSHA256: protocol.EncodeHashHex(candidate.Artifact.ContentSHA256),
		ProofDigest: protocol.EncodeHashHex(proofDigest),
	}
	if candidate.Artifact.WALRange != nil {
		action.WALStartSequence = candidate.Artifact.WALRange.StartSequence
		action.WALEndSequence = candidate.Artifact.WALRange.EndSequence
	}
	return action, nil
}

// BuildPrunePlan is pure: it reads no filesystem, network, clock, or journal
// state. Candidate facts must already have been freshly and independently
// verified by the observer.
func BuildPrunePlan(input PlannerInput) (PrunePlan, error) {
	if !validDiskClass(input.Disk) || input.CurrentWallTimeUnixMS == 0 || input.DurableWallTimeUnixMS == 0 || input.GraceMS == 0 || input.MaxCandidates == 0 || input.WALExpectedStart == 0 || input.ProofLimits.Validate() != nil {
		return PrunePlan{}, fmt.Errorf("planner input is incomplete")
	}
	plan := PrunePlan{
		PlanVersion: PrunePlanVersion, Disk: input.Disk, CurrentWallTimeUnixMS: input.CurrentWallTimeUnixMS,
		DurableWallTimeUnixMS: input.DurableWallTimeUnixMS, ActiveRecoveryFloor: input.ActiveRecoveryFloor,
		WALExpectedStart: input.WALExpectedStart, GraceMS: input.GraceMS,
	}
	byID := make(map[string]CandidateFact, len(input.Candidates))
	byPath := make(map[string]string, len(input.Candidates))
	for _, candidate := range input.Candidates {
		id, err := candidate.Artifact.StableID()
		if err != nil {
			return PrunePlan{}, fmt.Errorf("candidate identity: %w", err)
		}
		if _, exists := byID[id]; exists {
			return PrunePlan{}, fmt.Errorf("duplicate candidate identity %s", id)
		}
		if prior, exists := byPath[candidate.Artifact.TrustedPath]; exists && prior != id {
			return PrunePlan{}, fmt.Errorf("path collision at %s", candidate.Artifact.TrustedPath)
		}
		byID[id] = candidate
		byPath[candidate.Artifact.TrustedPath] = id
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if input.CurrentWallTimeUnixMS < input.DurableWallTimeUnixMS {
		for _, id := range ids {
			plan.Blocked = append(plan.Blocked, BlockedCandidate{ArtifactID: id, Reason: "clock_regression"})
		}
		canonical, err := plan.CanonicalJSON()
		if err != nil {
			return PrunePlan{}, err
		}
		plan.PlanDigest = PrunePlanDigest(canonical)
		return plan, nil
	}

	reasons := make(map[string]string, len(ids))
	eligible := make(map[string]bool, len(ids))
	for _, id := range ids {
		reason, ok := candidateEligibility(byID[id], input)
		reasons[id] = reason
		eligible[id] = ok
	}
	walIDs := make([]string, 0)
	for _, id := range ids {
		if byID[id].Artifact.Kind == ArtifactWALSegment {
			walIDs = append(walIDs, id)
		}
	}
	sort.Slice(walIDs, func(i, j int) bool {
		left, right := byID[walIDs[i]].Artifact.WALRange, byID[walIDs[j]].Artifact.WALRange
		if left == nil || right == nil {
			return walIDs[i] < walIDs[j]
		}
		if left.StartSequence != right.StartSequence {
			return left.StartSequence < right.StartSequence
		}
		return walIDs[i] < walIDs[j]
	})
	expected := input.WALExpectedStart
	walPrefixExhausted := false
	walAllowed := make(map[string]bool, len(walIDs))
	for index, id := range walIDs {
		candidate := byID[id]
		if walPrefixExhausted || candidate.Artifact.WALRange == nil || candidate.Artifact.WALRange.StartSequence != expected {
			reasons[id] = "wal_prefix_gap"
			for _, later := range walIDs[index:] {
				eligible[later] = false
				if later != id {
					reasons[later] = "wal_prefix_blocked"
				}
			}
			break
		}
		if !eligible[id] {
			for _, later := range walIDs[index+1:] {
				eligible[later] = false
				reasons[later] = "wal_prefix_blocked"
			}
			break
		}
		walAllowed[id] = true
		if candidate.Artifact.WALRange.EndSequence == ^uint64(0) {
			walPrefixExhausted = true
		} else {
			expected = candidate.Artifact.WALRange.EndSequence + 1
		}
	}

	actions := make([]PruneAction, 0, len(ids))
	for _, id := range ids {
		if !eligible[id] || byID[id].Artifact.Kind == ArtifactWALSegment && !walAllowed[id] {
			plan.Blocked = append(plan.Blocked, BlockedCandidate{ArtifactID: id, Reason: reasons[id]})
			continue
		}
		if uint64(len(actions)) >= input.MaxCandidates {
			plan.Blocked = append(plan.Blocked, BlockedCandidate{ArtifactID: id, Reason: "max_prune_candidates"})
			continue
		}
		action, err := makePruneAction(byID[id])
		if err != nil {
			return PrunePlan{}, err
		}
		actions = append(actions, action)
	}
	sort.Slice(actions, func(i, j int) bool { return actions[i].ArtifactID < actions[j].ArtifactID })
	sort.Slice(plan.Blocked, func(i, j int) bool {
		if plan.Blocked[i].ArtifactID != plan.Blocked[j].ArtifactID {
			return plan.Blocked[i].ArtifactID < plan.Blocked[j].ArtifactID
		}
		return plan.Blocked[i].Reason < plan.Blocked[j].Reason
	})
	plan.Actions = actions
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return PrunePlan{}, err
	}
	plan.PlanDigest = PrunePlanDigest(canonical)
	return plan, nil
}
