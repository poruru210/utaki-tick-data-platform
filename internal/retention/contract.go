package retention

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"

	"tick-data-platform/internal/operations"
	"tick-data-platform/internal/protocol"
)

const (
	RetentionProofVersion        = "retention-proof-v1"
	PruneCheckpointVersion       = "prune-checkpoint-v1"
	RetentionProofDomain         = "tick-data-platform/retention-proof/v1\x00"
	PruneCheckpointDomain        = "tick-data-platform/prune-checkpoint/v1\x00"
	PrunePlanDomain              = "tick-data-platform/prune-plan/v1\x00"
	RemoteObservationExact       = "Exact"
	RemoteObservationDifferent   = "Different"
	RemoteObservationAmbiguous   = "Ambiguous"
	RemoteObservationAbsent      = "Absent"
	RemoteObservationOversized   = "Oversized"
	RemoteObservationUnavailable = "Unavailable"
)

type WALRange struct {
	StartSequence  uint64
	EndSequence    uint64
	StartChainRoot [32]byte
	EndChainRoot   [32]byte
}

type ReplayIdentity struct {
	DatasetID                   string
	CampaignID                  string
	Date                        string
	ManifestKey                 string
	ManifestSHA256              [32]byte
	PartSetRoot                 [32]byte
	CanonicalStreamRowChainRoot [32]byte
}

type RemoteObjectObservation struct {
	Class   string
	FullKey string
	SHA256  [32]byte
	Bytes   uint64
}

type ProofLimits struct {
	MaxProofObjects  uint64
	MaxProofBytes    uint64
	MaxManifestNodes uint64
}

type RetentionProof struct {
	ProofVersion             string
	ArtifactKind             string
	TrustedRelativePath      string
	Bytes                    uint64
	ContentSHA256            [32]byte
	ScopeConfigHash          [32]byte
	WALRange                 *WALRange
	Replay                   *ReplayIdentity
	Remote                   RemoteObjectObservation
	CoveringManifestKey      string
	CoveringManifestDigest   [32]byte
	VerificationReportDigest [32]byte
	ObservedWallTimeUnixMS   uint64
	GraceNotBeforeUnixMS     uint64
	Limits                   ProofLimits
}

type PruneCheckpoint struct {
	CheckpointVersion        string
	EndSequence              uint64
	RetainedChainRoot        [32]byte
	LastSegmentSHA256        [32]byte
	RetentionProofDigest     [32]byte
	RetentionProof           RetentionProof
	PreviousCheckpointDigest [32]byte
}

func (r RemoteObjectObservation) value() map[string]any {
	return map[string]any{
		"bytes":    r.Bytes,
		"class":    r.Class,
		"full_key": r.FullKey,
		"sha256":   protocol.EncodeHashHex(r.SHA256),
	}
}

func (w WALRange) value() map[string]any {
	return map[string]any{
		"end_chain_root":   protocol.EncodeHashHex(w.EndChainRoot),
		"end_sequence":     w.EndSequence,
		"start_chain_root": protocol.EncodeHashHex(w.StartChainRoot),
		"start_sequence":   w.StartSequence,
	}
}

func (r ReplayIdentity) value() map[string]any {
	return map[string]any{
		"campaign_id":                     r.CampaignID,
		"canonical_stream_row_chain_root": protocol.EncodeHashHex(r.CanonicalStreamRowChainRoot),
		"dataset_id":                      r.DatasetID,
		"date":                            r.Date,
		"manifest_key":                    r.ManifestKey,
		"manifest_sha256":                 protocol.EncodeHashHex(r.ManifestSHA256),
		"part_set_root":                   protocol.EncodeHashHex(r.PartSetRoot),
	}
}

func (l ProofLimits) value() map[string]any {
	return map[string]any{
		"max_manifest_nodes": l.MaxManifestNodes,
		"max_proof_bytes":    l.MaxProofBytes,
		"max_proof_objects":  l.MaxProofObjects,
	}
}

func (r RetentionProof) Value() map[string]any {
	var wal any
	if r.WALRange != nil {
		wal = r.WALRange.value()
	}
	var replay any
	if r.Replay != nil {
		replay = r.Replay.value()
	}
	return map[string]any{
		"artifact_kind":              r.ArtifactKind,
		"bytes":                      r.Bytes,
		"content_sha256":             protocol.EncodeHashHex(r.ContentSHA256),
		"covering_manifest_digest":   protocol.EncodeHashHex(r.CoveringManifestDigest),
		"covering_manifest_key":      r.CoveringManifestKey,
		"grace_not_before_unix_ms":   r.GraceNotBeforeUnixMS,
		"limits":                     r.Limits.value(),
		"observed_wall_time_unix_ms": r.ObservedWallTimeUnixMS,
		"proof_version":              r.ProofVersion,
		"remote_observation":         r.Remote.value(),
		"replay_identity":            replay,
		"scope_config_hash":          protocol.EncodeHashHex(r.ScopeConfigHash),
		"trusted_relative_path":      r.TrustedRelativePath,
		"verification_report_digest": protocol.EncodeHashHex(r.VerificationReportDigest),
		"wal_range":                  wal,
	}
}

func (r RetentionProof) Validate() error {
	if r.ProofVersion != RetentionProofVersion || r.ArtifactKind == "" {
		return fmt.Errorf("retention proof version or artifact kind is invalid")
	}
	if err := validateRelativePath(r.TrustedRelativePath); err != nil {
		return err
	}
	if r.Bytes == 0 || r.ObservedWallTimeUnixMS == 0 || r.GraceNotBeforeUnixMS < r.ObservedWallTimeUnixMS {
		return fmt.Errorf("retention proof time or size is invalid")
	}
	if err := validateHash("content_sha256", r.ContentSHA256); err != nil {
		return err
	}
	if err := validateHash("scope_config_hash", r.ScopeConfigHash); err != nil {
		return err
	}
	if err := validateHash("covering_manifest_digest", r.CoveringManifestDigest); err != nil {
		return err
	}
	if err := validateHash("verification_report_digest", r.VerificationReportDigest); err != nil {
		return err
	}
	if err := validateRelativePath(r.CoveringManifestKey); err != nil {
		return fmt.Errorf("covering manifest key: %w", err)
	}
	if err := validateRemoteObservation(r.Remote, r.Bytes, r.ContentSHA256); err != nil {
		return err
	}
	if (r.WALRange == nil) == (r.Replay == nil) {
		return fmt.Errorf("retention proof needs exactly one identity")
	}
	if r.WALRange != nil {
		if r.WALRange.StartSequence == 0 || r.WALRange.EndSequence < r.WALRange.StartSequence {
			return fmt.Errorf("WAL range is invalid")
		}
		if r.WALRange.StartSequence == 1 {
			if r.WALRange.StartChainRoot != ([32]byte{}) {
				return fmt.Errorf("genesis WAL range must start at the zero chain root")
			}
		} else if err := validateHash("start_chain_root", r.WALRange.StartChainRoot); err != nil {
			return err
		}
		if err := validateHash("end_chain_root", r.WALRange.EndChainRoot); err != nil {
			return err
		}
	}
	if r.Replay != nil {
		for name, value := range map[string]string{"dataset_id": r.Replay.DatasetID, "campaign_id": r.Replay.CampaignID, "date": r.Replay.Date, "manifest_key": r.Replay.ManifestKey} {
			if value == "" {
				return fmt.Errorf("replay %s is required", name)
			}
		}
		for name, value := range map[string][32]byte{"manifest_sha256": r.Replay.ManifestSHA256, "part_set_root": r.Replay.PartSetRoot, "canonical_stream_row_chain_root": r.Replay.CanonicalStreamRowChainRoot} {
			if err := validateHash(name, value); err != nil {
				return err
			}
		}
	}
	return r.Limits.Validate()
}

func validateRemoteObservation(observation RemoteObjectObservation, bytes uint64, digest [32]byte) error {
	if observation.Class != RemoteObservationExact || observation.Bytes != bytes || observation.SHA256 != digest {
		return fmt.Errorf("retention remote observation is not exact")
	}
	if err := validateRelativePath(observation.FullKey); err != nil {
		return fmt.Errorf("remote observation key: %w", err)
	}
	return nil
}

func validateHash(name string, value [32]byte) error {
	if value == ([32]byte{}) {
		return fmt.Errorf("%s is zero", name)
	}
	return nil
}

func validateRelativePath(value string) error {
	if value == "" || len(value) > protocol.MaxPathBytes || strings.HasPrefix(value, "/") || strings.ContainsAny(value, "\\\r\n") || strings.Contains(value, "//") {
		return fmt.Errorf("trusted relative path is not canonical")
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("trusted relative path contains a forbidden component")
		}
	}
	return nil
}

func (r RetentionProof) CanonicalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(r.Value())
}

func RetentionProofDigest(canonical []byte) [32]byte {
	return sha256.Sum256(append([]byte(RetentionProofDomain), canonical...))
}

func (r RetentionProof) Digest() ([32]byte, error) {
	canonical, err := r.CanonicalJSON()
	if err != nil {
		return [32]byte{}, err
	}
	return RetentionProofDigest(canonical), nil
}

func (c PruneCheckpoint) Value() map[string]any {
	return map[string]any{
		"checkpoint_version":         c.CheckpointVersion,
		"end_sequence":               c.EndSequence,
		"last_segment_sha256":        protocol.EncodeHashHex(c.LastSegmentSHA256),
		"previous_checkpoint_digest": protocol.EncodeHashHex(c.PreviousCheckpointDigest),
		"retained_chain_root":        protocol.EncodeHashHex(c.RetainedChainRoot),
		"retention_proof_digest":     protocol.EncodeHashHex(c.RetentionProofDigest),
		"retention_proof":            c.RetentionProof.Value(),
	}
}

func (c PruneCheckpoint) Validate() error {
	if c.CheckpointVersion != PruneCheckpointVersion || c.EndSequence == 0 {
		return fmt.Errorf("prune checkpoint identity is invalid")
	}
	for name, value := range map[string][32]byte{"retained_chain_root": c.RetainedChainRoot, "last_segment_sha256": c.LastSegmentSHA256, "retention_proof_digest": c.RetentionProofDigest} {
		if err := validateHash(name, value); err != nil {
			return err
		}
	}
	if err := c.RetentionProof.Validate(); err != nil {
		return fmt.Errorf("retention proof: %w", err)
	}
	if c.RetentionProof.ArtifactKind != ArtifactWALSegment || c.RetentionProof.WALRange == nil || c.RetentionProof.WALRange.EndSequence != c.EndSequence || c.RetentionProof.WALRange.EndChainRoot != c.RetainedChainRoot || c.RetentionProof.ContentSHA256 != c.LastSegmentSHA256 {
		return fmt.Errorf("retention proof is not bound to checkpointed WAL")
	}
	proofDigest, err := c.RetentionProof.Digest()
	if err != nil || proofDigest != c.RetentionProofDigest {
		return fmt.Errorf("retention proof digest is not bound to checkpoint")
	}
	return nil
}

func (c PruneCheckpoint) CanonicalJSON() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return protocol.CanonicalJSON(c.Value())
}

func PruneCheckpointDigest(canonical []byte) [32]byte {
	return sha256.Sum256(append([]byte(PruneCheckpointDomain), canonical...))
}

func (c PruneCheckpoint) Digest() ([32]byte, error) {
	canonical, err := c.CanonicalJSON()
	if err != nil {
		return [32]byte{}, err
	}
	return PruneCheckpointDigest(canonical), nil
}

// Validate delegates the embedded proof limits to the shared M4 policy.
func (l ProofLimits) Validate() error {
	return operations.ValidateProofLimits(l.MaxProofObjects, l.MaxProofBytes, l.MaxManifestNodes)
}

func decodeObject(data []byte, expected map[string]bool) (map[string]any, error) {
	value, err := protocol.DecodeCanonicalJSON(data)
	if err != nil {
		return nil, fmt.Errorf("decode canonical JSON: %w", err)
	}
	return objectValue(value, expected)
}

func objectValue(value any, expected map[string]bool) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("canonical value must be an object")
	}
	if len(object) != len(expected) {
		return nil, fmt.Errorf("object has an incomplete field set")
	}
	for key := range object {
		if !expected[key] {
			return nil, fmt.Errorf("object contains unknown field %q", key)
		}
	}
	return object, nil
}

func stringField(object map[string]any, name string) (string, error) {
	value, ok := object[name].(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", name)
	}
	return value, nil
}

func uintField(object map[string]any, name string) (uint64, error) {
	value, ok := object[name].(uint64)
	if !ok {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return value, nil
}

func hashField(object map[string]any, name string, allowZero bool) ([32]byte, error) {
	value, err := stringField(object, name)
	if err != nil {
		return [32]byte{}, err
	}
	result, err := protocol.ParseHashHex(value)
	if err != nil {
		return [32]byte{}, fmt.Errorf("%s is not lowercase SHA-256", name)
	}
	if !allowZero && result == ([32]byte{}) {
		return [32]byte{}, fmt.Errorf("%s is zero", name)
	}
	return result, nil
}

var retentionProofKeys = map[string]bool{
	"artifact_kind": true, "bytes": true, "content_sha256": true,
	"covering_manifest_digest": true, "covering_manifest_key": true,
	"grace_not_before_unix_ms": true, "limits": true,
	"observed_wall_time_unix_ms": true, "proof_version": true,
	"remote_observation": true, "replay_identity": true,
	"scope_config_hash": true, "trusted_relative_path": true, "verification_report_digest": true,
	"wal_range": true,
}

var remoteObservationKeys = map[string]bool{
	"bytes": true, "class": true, "full_key": true, "sha256": true,
}

var proofLimitsKeys = map[string]bool{
	"max_manifest_nodes": true, "max_proof_bytes": true, "max_proof_objects": true,
}

var walRangeKeys = map[string]bool{
	"end_chain_root": true, "end_sequence": true, "start_chain_root": true, "start_sequence": true,
}

var replayIdentityKeys = map[string]bool{
	"campaign_id": true, "canonical_stream_row_chain_root": true, "dataset_id": true,
	"date": true, "manifest_key": true, "manifest_sha256": true, "part_set_root": true,
}

// DecodeRetentionProof strictly verifies one canonical retention proof.
func DecodeRetentionProof(data []byte) (RetentionProof, error) {
	object, err := decodeObject(data, retentionProofKeys)
	if err != nil {
		return RetentionProof{}, err
	}
	var result RetentionProof
	if result.ProofVersion, err = stringField(object, "proof_version"); err != nil {
		return RetentionProof{}, err
	}
	if result.ArtifactKind, err = stringField(object, "artifact_kind"); err != nil {
		return RetentionProof{}, err
	}
	if result.TrustedRelativePath, err = stringField(object, "trusted_relative_path"); err != nil {
		return RetentionProof{}, err
	}
	if result.Bytes, err = uintField(object, "bytes"); err != nil {
		return RetentionProof{}, err
	}
	if result.ContentSHA256, err = hashField(object, "content_sha256", false); err != nil {
		return RetentionProof{}, err
	}
	if result.ScopeConfigHash, err = hashField(object, "scope_config_hash", false); err != nil {
		return RetentionProof{}, err
	}
	if result.CoveringManifestKey, err = stringField(object, "covering_manifest_key"); err != nil {
		return RetentionProof{}, err
	}
	if result.CoveringManifestDigest, err = hashField(object, "covering_manifest_digest", false); err != nil {
		return RetentionProof{}, err
	}
	if result.VerificationReportDigest, err = hashField(object, "verification_report_digest", false); err != nil {
		return RetentionProof{}, err
	}
	if result.ObservedWallTimeUnixMS, err = uintField(object, "observed_wall_time_unix_ms"); err != nil {
		return RetentionProof{}, err
	}
	if result.GraceNotBeforeUnixMS, err = uintField(object, "grace_not_before_unix_ms"); err != nil {
		return RetentionProof{}, err
	}
	remoteObject, ok := object["remote_observation"].(map[string]any)
	if !ok {
		return RetentionProof{}, fmt.Errorf("remote observation must be an object")
	}
	if _, err := objectValue(remoteObject, remoteObservationKeys); err != nil {
		return RetentionProof{}, err
	}
	if result.Remote.Class, err = stringField(remoteObject, "class"); err != nil {
		return RetentionProof{}, err
	}
	if result.Remote.FullKey, err = stringField(remoteObject, "full_key"); err != nil {
		return RetentionProof{}, err
	}
	if result.Remote.SHA256, err = hashField(remoteObject, "sha256", false); err != nil {
		return RetentionProof{}, err
	}
	if result.Remote.Bytes, err = uintField(remoteObject, "bytes"); err != nil {
		return RetentionProof{}, err
	}
	limitsObject, ok := object["limits"].(map[string]any)
	if !ok {
		return RetentionProof{}, fmt.Errorf("proof limits must be an object")
	}
	if _, err := objectValue(limitsObject, proofLimitsKeys); err != nil {
		return RetentionProof{}, err
	}
	if result.Limits.MaxManifestNodes, err = uintField(limitsObject, "max_manifest_nodes"); err != nil {
		return RetentionProof{}, err
	}
	if result.Limits.MaxProofBytes, err = uintField(limitsObject, "max_proof_bytes"); err != nil {
		return RetentionProof{}, err
	}
	if result.Limits.MaxProofObjects, err = uintField(limitsObject, "max_proof_objects"); err != nil {
		return RetentionProof{}, err
	}
	if walValue := object["wal_range"]; walValue != nil {
		walObject, ok := walValue.(map[string]any)
		if !ok {
			return RetentionProof{}, fmt.Errorf("wal range must be an object or null")
		}
		if _, err := objectValue(walObject, walRangeKeys); err != nil {
			return RetentionProof{}, err
		}
		result.WALRange = &WALRange{}
		if result.WALRange.StartSequence, err = uintField(walObject, "start_sequence"); err != nil {
			return RetentionProof{}, err
		}
		if result.WALRange.EndSequence, err = uintField(walObject, "end_sequence"); err != nil {
			return RetentionProof{}, err
		}
		if result.WALRange.StartChainRoot, err = hashField(walObject, "start_chain_root", result.WALRange.StartSequence == 1); err != nil {
			return RetentionProof{}, err
		}
		if result.WALRange.EndChainRoot, err = hashField(walObject, "end_chain_root", false); err != nil {
			return RetentionProof{}, err
		}
	}
	if replayValue := object["replay_identity"]; replayValue != nil {
		replayObject, ok := replayValue.(map[string]any)
		if !ok {
			return RetentionProof{}, fmt.Errorf("replay identity must be an object or null")
		}
		if _, err := objectValue(replayObject, replayIdentityKeys); err != nil {
			return RetentionProof{}, err
		}
		result.Replay = &ReplayIdentity{}
		if result.Replay.DatasetID, err = stringField(replayObject, "dataset_id"); err != nil {
			return RetentionProof{}, err
		}
		if result.Replay.CampaignID, err = stringField(replayObject, "campaign_id"); err != nil {
			return RetentionProof{}, err
		}
		if result.Replay.Date, err = stringField(replayObject, "date"); err != nil {
			return RetentionProof{}, err
		}
		if result.Replay.ManifestKey, err = stringField(replayObject, "manifest_key"); err != nil {
			return RetentionProof{}, err
		}
		if result.Replay.ManifestSHA256, err = hashField(replayObject, "manifest_sha256", false); err != nil {
			return RetentionProof{}, err
		}
		if result.Replay.PartSetRoot, err = hashField(replayObject, "part_set_root", false); err != nil {
			return RetentionProof{}, err
		}
		if result.Replay.CanonicalStreamRowChainRoot, err = hashField(replayObject, "canonical_stream_row_chain_root", false); err != nil {
			return RetentionProof{}, err
		}
	}
	if err := result.Validate(); err != nil {
		return RetentionProof{}, err
	}
	canonical, err := result.CanonicalJSON()
	if err != nil {
		return RetentionProof{}, err
	}
	if !bytes.Equal(canonical, data) {
		return RetentionProof{}, fmt.Errorf("retention proof bytes are not canonical")
	}
	return result, nil
}

var pruneCheckpointKeys = map[string]bool{
	"checkpoint_version": true, "end_sequence": true, "last_segment_sha256": true,
	"previous_checkpoint_digest": true, "retained_chain_root": true, "retention_proof": true, "retention_proof_digest": true,
}

// DecodePruneCheckpoint strictly verifies one canonical checkpoint.
func DecodePruneCheckpoint(data []byte) (PruneCheckpoint, error) {
	object, err := decodeObject(data, pruneCheckpointKeys)
	if err != nil {
		return PruneCheckpoint{}, err
	}
	var result PruneCheckpoint
	if result.CheckpointVersion, err = stringField(object, "checkpoint_version"); err != nil {
		return PruneCheckpoint{}, err
	}
	if result.EndSequence, err = uintField(object, "end_sequence"); err != nil {
		return PruneCheckpoint{}, err
	}
	if result.LastSegmentSHA256, err = hashField(object, "last_segment_sha256", false); err != nil {
		return PruneCheckpoint{}, err
	}
	if result.PreviousCheckpointDigest, err = hashField(object, "previous_checkpoint_digest", true); err != nil {
		return PruneCheckpoint{}, err
	}
	if result.RetainedChainRoot, err = hashField(object, "retained_chain_root", false); err != nil {
		return PruneCheckpoint{}, err
	}
	if result.RetentionProofDigest, err = hashField(object, "retention_proof_digest", false); err != nil {
		return PruneCheckpoint{}, err
	}
	proofObject, ok := object["retention_proof"].(map[string]any)
	if !ok {
		return PruneCheckpoint{}, fmt.Errorf("retention_proof must be an object")
	}
	proofCanonical, err := protocol.CanonicalJSON(proofObject)
	if err != nil {
		return PruneCheckpoint{}, err
	}
	if result.RetentionProof, err = DecodeRetentionProof(proofCanonical); err != nil {
		return PruneCheckpoint{}, err
	}
	if err := result.Validate(); err != nil {
		return PruneCheckpoint{}, err
	}
	canonical, err := result.CanonicalJSON()
	if err != nil {
		return PruneCheckpoint{}, err
	}
	if !bytes.Equal(canonical, data) {
		return PruneCheckpoint{}, fmt.Errorf("prune checkpoint bytes are not canonical")
	}
	return result, nil
}
