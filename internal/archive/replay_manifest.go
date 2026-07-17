package archive

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"tick-data-platform/internal/parquet"
	"tick-data-platform/internal/protocol"
)

// PartManifestInput is an opaque, scope-bound summary emitted after a Parquet
// part has been closed, synced, hashed, reopened, and independently verified.
type PartManifestInput struct {
	scope      protocol.ReplayScope
	conversion ConversionTuple
	artifact   parquet.PartArtifact
}

// PartManifestInputFromArtifact binds the exact replay scope and conversion
// tuple to a verified Parquet artifact before manifest construction. The
// artifact has no caller-supplied manifest provenance fields to trust.
func PartManifestInputFromArtifact(scope protocol.ReplayScope, conversion ConversionTuple, artifact parquet.PartArtifact) (PartManifestInput, error) {
	if err := scope.Validate(); err != nil {
		return PartManifestInput{}, err
	}
	if err := validateConversionTuple(scope, conversion); err != nil {
		return PartManifestInput{}, err
	}
	if err := validateArtifactSummary(scope, artifact); err != nil {
		return PartManifestInput{}, err
	}
	return PartManifestInput{scope: scope, conversion: conversion, artifact: artifact}, nil
}

// BuildPartManifest creates a day-local manifest only from the verified part

// summary. The manifest predecessor is supplied by the caller and is checked
// before it can become part of part_set_root.
func BuildPartManifest(input PartManifestInput, previous *protocol.PartManifest) (protocol.PartManifest, error) {
	if err := input.scope.Validate(); err != nil {
		return protocol.PartManifest{}, err
	}
	if err := validateConversionTuple(input.scope, input.conversion); err != nil {
		return protocol.PartManifest{}, err
	}
	if err := validateArtifactSummary(input.scope, input.artifact); err != nil {
		return protocol.PartManifest{}, err
	}
	artifact := input.artifact
	var predecessor *[32]byte
	if artifact.PartSequence == 0 {
		if previous != nil {
			return protocol.PartManifest{}, fmt.Errorf("first part cannot have a predecessor")
		}
		if artifact.PreviousRowChainHash != ([32]byte{}) {
			return protocol.PartManifest{}, fmt.Errorf("first part artifact has a row-chain predecessor")
		}
	} else {
		if previous == nil || previous.PartSequence == ^uint32(0) || previous.PartSequence+1 != artifact.PartSequence {
			return protocol.PartManifest{}, fmt.Errorf("part predecessor sequence is not adjacent")
		}
		if err := previous.Validate(); err != nil {
			return protocol.PartManifest{}, fmt.Errorf("invalid part predecessor: %w", err)
		}
		if !samePartBinding(*previous, input.scope, input.conversion) {
			return protocol.PartManifest{}, fmt.Errorf("part predecessor binding differs")
		}
		if artifact.PreviousRowChainHash != previous.LastRowChainHash {
			return protocol.PartManifest{}, fmt.Errorf("artifact previous row-chain hash does not match predecessor")
		}
		previousDigest, err := strictPartDigest(*previous)
		if err != nil {
			return protocol.PartManifest{}, err
		}
		predecessor = &previousDigest
	}
	manifest := protocol.PartManifest{
		ManifestVersion: protocol.PartManifestVersion,
		DatasetID:       input.scope.DatasetID, DayDefinitionID: input.scope.DayDefinitionID,
		Date: input.scope.Date, ReplayContractID: input.scope.ReplayContractID, FormatID: input.conversion.FormatID,
		ConversionID: input.conversion.ConversionID, ConverterBuildID: input.conversion.ConverterBuildID,
		DependencyLockHash: input.conversion.DependencyLockHash, WriterConfigurationHash: input.conversion.WriterConfigurationHash,
		TargetPlatformContract: input.conversion.TargetPlatformContract,
		RawDayManifestKey:      input.scope.RawDayManifestKey, RawDayManifestSHA256: input.scope.RawDayManifestSHA256,
		PartSequence: artifact.PartSequence, PartKey: artifact.PartKey, PartSHA256: artifact.PartSHA256, PartBytes: artifact.PartBytes,
		RowCount: artifact.RowCount, CanonicalRowBytes: artifact.CanonicalRowBytes,
		FirstStreamSequence: artifact.FirstStreamSequence, LastStreamSequence: artifact.LastStreamSequence,
		PreviousRowChainHash: artifact.PreviousRowChainHash, FirstRowChainHash: artifact.FirstRowChainHash, LastRowChainHash: artifact.LastRowChainHash,
		PreviousManifestSHA256: predecessor,
	}
	if err := manifest.Validate(); err != nil {
		return protocol.PartManifest{}, err
	}
	digest, err := strictPartDigest(manifest)
	if err != nil {
		return protocol.PartManifest{}, err
	}
	manifest.ManifestSHA256 = digest
	return manifest, nil
}

func validateArtifactSummary(scope protocol.ReplayScope, artifact parquet.PartArtifact) error {
	wantKey, err := protocol.ReplayPartObjectKey(scope, artifact.FirstStreamSequence, artifact.LastStreamSequence, artifact.PartSHA256)
	if err != nil || artifact.PartSHA256 == ([32]byte{}) || artifact.PartKey != wantKey {
		return fmt.Errorf("part artifact key is not bound to its hash")
	}
	if artifact.PartBytes == 0 || artifact.RowCount == 0 || artifact.CanonicalRowBytes == 0 {
		return fmt.Errorf("part artifact has an empty size or row count")
	}
	if artifact.LastStreamSequence < artifact.FirstStreamSequence || artifact.LastStreamSequence-artifact.FirstStreamSequence != artifact.RowCount-1 {
		return fmt.Errorf("part artifact stream range does not equal row count")
	}
	if artifact.FirstRowChainHash == ([32]byte{}) || artifact.LastRowChainHash == ([32]byte{}) {
		return fmt.Errorf("part artifact row-chain anchors must not be zero")
	}
	if artifact.PartSequence == 0 && artifact.FirstStreamSequence != 0 {
		return fmt.Errorf("first part artifact must start at stream sequence zero")
	}
	if artifact.PartSequence > 0 && artifact.PreviousRowChainHash == ([32]byte{}) {
		return fmt.Errorf("successor part artifact requires a previous row-chain hash")
	}
	return nil
}

func samePartBinding(part protocol.PartManifest, scope protocol.ReplayScope, conversion ConversionTuple) bool {
	return part.DatasetID == scope.DatasetID &&
		part.DayDefinitionID == scope.DayDefinitionID && part.Date == scope.Date &&
		part.ReplayContractID == scope.ReplayContractID && part.FormatID == conversion.FormatID &&
		part.ConversionID == conversion.ConversionID && part.ConverterBuildID == conversion.ConverterBuildID &&
		part.DependencyLockHash == conversion.DependencyLockHash &&
		part.WriterConfigurationHash == conversion.WriterConfigurationHash &&
		part.TargetPlatformContract == conversion.TargetPlatformContract &&
		part.RawDayManifestKey == scope.RawDayManifestKey &&
		part.RawDayManifestSHA256 == scope.RawDayManifestSHA256
}

func strictPartDigest(manifest protocol.PartManifest) ([32]byte, error) {
	if err := manifest.Validate(); err != nil {
		return [32]byte{}, err
	}
	if manifest.PartBytes == 0 || manifest.RowCount == 0 || manifest.CanonicalRowBytes == 0 || manifest.FirstRowChainHash == ([32]byte{}) || manifest.LastRowChainHash == ([32]byte{}) {
		return [32]byte{}, fmt.Errorf("part manifest has invalid bounded summary")
	}
	if manifest.LastStreamSequence < manifest.FirstStreamSequence || manifest.RowCount == 0 || manifest.LastStreamSequence-manifest.FirstStreamSequence != manifest.RowCount-1 {
		return [32]byte{}, fmt.Errorf("part manifest row range is inconsistent")
	}
	digest, err := protocol.PartManifestDigest(manifest)
	if err != nil {
		return [32]byte{}, err
	}
	if manifest.ManifestSHA256 != ([32]byte{}) && manifest.ManifestSHA256 != digest {
		return [32]byte{}, fmt.Errorf("part manifest digest mismatch")
	}
	return digest, nil
}

// VerifyPartManifestObject accepts only canonical bytes and the exact
// content-addressed manifest key and digest expected by the caller.
func VerifyPartManifestObject(data []byte, expectedKey string, expectedDigest [32]byte) (protocol.PartManifest, error) {
	if expectedKey == "" {
		return protocol.PartManifest{}, fmt.Errorf("expected part manifest key is required")
	}
	manifest, err := protocol.VerifyPartManifest(data)
	if err != nil {
		return protocol.PartManifest{}, err
	}
	canonical, err := protocol.PartManifestCanonicalJSON(manifest)
	if err != nil || !bytes.Equal(canonical, data) {
		return protocol.PartManifest{}, fmt.Errorf("part manifest bytes are not canonical")
	}
	digest, err := strictPartDigest(manifest)
	if err != nil {
		return protocol.PartManifest{}, err
	}
	if expectedDigest != ([32]byte{}) && digest != expectedDigest {
		return protocol.PartManifest{}, fmt.Errorf("part manifest digest does not match expected digest")
	}
	wantKey, err := protocol.PartManifestKey(manifest)
	if err != nil || expectedKey != wantKey {
		return protocol.PartManifest{}, fmt.Errorf("part manifest key does not match digest and sequence")
	}
	manifest.ManifestSHA256 = digest
	return manifest, nil
}

// ConversionTuple is the manifest-visible portion of ConversionSpec. Keeping
// this small value in archive avoids coupling manifest verification to a
// Parquet writer package.
type ConversionTuple struct {
	ReplayContractID        string
	FormatID                string
	ConversionID            string
	ConverterBuildID        string
	DependencyLockHash      [32]byte
	WriterConfigurationHash [32]byte
	TargetPlatformContract  string
}

// ConversionTupleFromSpec converts the fully validated Parquet conversion
// contract into the manifest-visible tuple. The caller cannot omit or replace
// the dependency and writer hashes because ConversionSpec.Validate recomputes
// them from the pinned writer configuration.
func ConversionTupleFromSpec(spec parquet.ConversionSpec) (ConversionTuple, error) {
	if err := spec.Validate(); err != nil {
		return ConversionTuple{}, err
	}
	return ConversionTuple{
		ReplayContractID:        spec.ReplayContractID,
		FormatID:                spec.FormatID,
		ConversionID:            spec.ConversionID,
		ConverterBuildID:        spec.ConverterBuildID,
		DependencyLockHash:      spec.DependencyLockHash,
		WriterConfigurationHash: spec.WriterConfigurationHash,
		TargetPlatformContract:  spec.TargetPlatformContract,
	}, nil
}

type ReplayDayManifestInput struct {
	Scope                       protocol.ReplayScope
	Conversion                  ConversionTuple
	CompletenessStatus          string
	Revision                    uint64
	Previous                    *protocol.ReplayDayManifest
	Parts                       []protocol.PartManifest
	CanonicalStreamRowChainRoot [32]byte
}

// BuildReplayDayManifest builds the strict M3 form. Part summaries are
// revalidated and reconstructed into the ordered day-local chain before their
// keys and part_set_root are copied into the replay manifest.
func BuildReplayDayManifest(input ReplayDayManifestInput) (protocol.ReplayDayManifest, error) {
	if err := input.Scope.Validate(); err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	if err := validateConversionTuple(input.Scope, input.Conversion); err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	if input.CompletenessStatus != "provisional" && input.CompletenessStatus != "settled_snapshot" {
		return protocol.ReplayDayManifest{}, fmt.Errorf("invalid replay completeness status")
	}
	parts, keys, partRoot, err := validatePartSet(input.Parts, input.Scope, input.Conversion)
	if err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	if len(parts) == 0 && input.CanonicalStreamRowChainRoot != ([32]byte{}) {
		return protocol.ReplayDayManifest{}, fmt.Errorf("empty replay day must have the zero row-chain root")
	}
	if len(parts) > 0 && input.CanonicalStreamRowChainRoot == ([32]byte{}) {
		return protocol.ReplayDayManifest{}, fmt.Errorf("non-empty replay day must have a row-chain root")
	}
	if len(parts) > 0 && input.CanonicalStreamRowChainRoot != parts[len(parts)-1].LastRowChainHash {
		return protocol.ReplayDayManifest{}, fmt.Errorf("replay row-chain root does not match final part")
	}
	revision := input.Revision
	if input.Previous == nil {
		if revision == 0 {
			revision = 1
		}
		if revision != 1 {
			return protocol.ReplayDayManifest{}, fmt.Errorf("genesis replay revision must be 1")
		}
	} else {
		if err := verifyReplaySuccessor(input.Scope, input.Conversion, revision, *input.Previous); err != nil {
			return protocol.ReplayDayManifest{}, err
		}
		if revision == 0 {
			revision = input.Previous.Revision + 1
		}
	}
	manifestID, err := replayManifestID(input.Scope, input.Conversion)
	if err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	manifest := protocol.ReplayDayManifest{
		ManifestVersion: protocol.ReplayDayManifestVersion, ManifestID: manifestID,
		DatasetID:       input.Scope.DatasetID,
		DayDefinitionID: input.Scope.DayDefinitionID, Date: input.Scope.Date, Revision: revision,
		RawDayManifestKey: input.Scope.RawDayManifestKey, RawDayManifestSHA256: input.Scope.RawDayManifestSHA256,
		ReplayContractID: input.Conversion.ReplayContractID, FormatID: input.Conversion.FormatID,
		ConversionID: input.Conversion.ConversionID, ConverterBuildID: input.Conversion.ConverterBuildID,
		DependencyLockHash:      input.Conversion.DependencyLockHash,
		WriterConfigurationHash: input.Conversion.WriterConfigurationHash,
		TargetPlatformContract:  input.Conversion.TargetPlatformContract,
		CompletenessStatus:      input.CompletenessStatus, PartManifestKeys: keys, PartSetRoot: partRoot,
		CanonicalStreamRowChainRoot: input.CanonicalStreamRowChainRoot,
	}
	if input.Previous != nil {
		previousDigest, err := strictReplayDigest(*input.Previous)
		if err != nil {
			return protocol.ReplayDayManifest{}, err
		}
		manifest.PreviousManifestSHA256 = &previousDigest
	}
	if err := manifest.Validate(); err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	digest, err := strictReplayDigest(manifest)
	if err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	manifest.ManifestSHA256 = digest
	return manifest, nil
}

func validateConversionTuple(scope protocol.ReplayScope, conversion ConversionTuple) error {
	if conversion.ReplayContractID != scope.ReplayContractID || conversion.ConversionID != scope.ConversionID {
		return fmt.Errorf("replay conversion tuple does not match scope")
	}
	if conversion.FormatID != protocol.ReplayFormatID || conversion.ConverterBuildID == "" || conversion.TargetPlatformContract == "" || conversion.DependencyLockHash == ([32]byte{}) || conversion.WriterConfigurationHash == ([32]byte{}) {
		return fmt.Errorf("replay conversion tuple is incomplete")
	}
	return nil
}

func validatePartSet(parts []protocol.PartManifest, scope protocol.ReplayScope, conversion ConversionTuple) ([]protocol.PartManifest, []string, [32]byte, error) {
	copyParts := append([]protocol.PartManifest(nil), parts...)
	keys := make([]string, len(copyParts))
	var previousLast uint64
	for index := range copyParts {
		part := &copyParts[index]
		if _, err := strictPartDigest(*part); err != nil {
			return nil, nil, [32]byte{}, fmt.Errorf("part %d: %w", index, err)
		}
		if int(part.PartSequence) != index {
			return nil, nil, [32]byte{}, fmt.Errorf("part sequence has a gap or branch")
		}
		if !samePartBinding(*part, scope, conversion) {
			return nil, nil, [32]byte{}, fmt.Errorf("part %d has a mixed scope, raw binding, or conversion", index)
		}
		if index == 0 {
			if part.FirstStreamSequence != 0 || part.PreviousManifestSHA256 != nil || part.PreviousRowChainHash != ([32]byte{}) {
				return nil, nil, [32]byte{}, fmt.Errorf("first part has an invalid predecessor or stream start")
			}
		} else {
			if previousLast == ^uint64(0) || part.FirstStreamSequence != previousLast+1 || part.PreviousManifestSHA256 == nil || part.PreviousRowChainHash != copyParts[index-1].LastRowChainHash {
				return nil, nil, [32]byte{}, fmt.Errorf("part stream ranges overlap or have a gap")
			}
			previousDigest, err := strictPartDigest(copyParts[index-1])
			if err != nil || *part.PreviousManifestSHA256 != previousDigest {
				return nil, nil, [32]byte{}, fmt.Errorf("part predecessor digest does not match")
			}
		}
		key, err := protocol.PartManifestKey(*part)
		if err != nil {
			return nil, nil, [32]byte{}, err
		}
		keys[index] = key
		previousLast = part.LastStreamSequence
	}
	root, err := protocol.PartSetRoot(copyParts)
	if err != nil {
		return nil, nil, [32]byte{}, err
	}
	return copyParts, keys, root, nil
}

func replayManifestID(scope protocol.ReplayScope, conversion ConversionTuple) (string, error) {
	value, err := protocol.CanonicalJSON(map[string]any{"conversion_id": conversion.ConversionID,
		"dataset_id": scope.DatasetID, "date": scope.Date, "day_definition_id": scope.DayDefinitionID,
		"replay_contract_id": conversion.ReplayContractID, "format_id": conversion.FormatID,
		"converter_build_id": conversion.ConverterBuildID, "dependency_lock_hash": hex.EncodeToString(conversion.DependencyLockHash[:]),
		"writer_configuration_hash": hex.EncodeToString(conversion.WriterConfigurationHash[:]),
		"target_platform_contract":  conversion.TargetPlatformContract,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("tick-data-platform/replay-identity/v1\x00"), value...))
	return "replay-" + hex.EncodeToString(digest[:16]), nil
}

func verifyReplaySuccessor(scope protocol.ReplayScope, conversion ConversionTuple, revision uint64, previous protocol.ReplayDayManifest) error {
	if previous.M0EmptyPartsCompatibility {
		return fmt.Errorf("M0 compatibility manifest cannot be a replay revision predecessor")
	}
	if err := previous.Validate(); err != nil {
		return err
	}
	if revision != 0 && revision != previous.Revision+1 {
		return fmt.Errorf("replay revision must immediately follow predecessor")
	}
	if previous.DatasetID != scope.DatasetID || previous.DayDefinitionID != scope.DayDefinitionID || previous.Date != scope.Date || previous.ReplayContractID != conversion.ReplayContractID || previous.FormatID != conversion.FormatID || previous.ConversionID != conversion.ConversionID || previous.ConverterBuildID != conversion.ConverterBuildID || previous.DependencyLockHash != conversion.DependencyLockHash || previous.WriterConfigurationHash != conversion.WriterConfigurationHash || previous.TargetPlatformContract != conversion.TargetPlatformContract {
		return fmt.Errorf("replay revision changed scope or conversion identity")
	}
	if previous.RawDayManifestKey == scope.RawDayManifestKey || previous.RawDayManifestSHA256 == scope.RawDayManifestSHA256 {
		return fmt.Errorf("replay successor must bind a new raw revision key and hash")
	}
	return nil
}

func strictReplayDigest(manifest protocol.ReplayDayManifest) ([32]byte, error) {
	if err := manifest.Validate(); err != nil {
		return [32]byte{}, err
	}
	digest, err := protocol.ReplayDayManifestDigest(manifest)
	if err != nil {
		return [32]byte{}, err
	}
	if manifest.ManifestSHA256 != ([32]byte{}) && manifest.ManifestSHA256 != digest {
		return [32]byte{}, fmt.Errorf("replay manifest digest mismatch")
	}
	return digest, nil
}

// VerifyReplayDayManifestObject validates canonical bytes, the immutable
// manifest digest, the exact replay key, scope and conversion tuple, and the
// ordered part chain. It does not access R2 or any publication journal.
func VerifyReplayDayManifestObject(data []byte, expectedKey string, expectedScope protocol.ReplayScope, conversion ConversionTuple, parts []protocol.PartManifest, previous *protocol.ReplayDayManifest, expectedRowChainRoot [32]byte) (protocol.ReplayDayManifest, error) {
	if expectedKey == "" {
		return protocol.ReplayDayManifest{}, fmt.Errorf("expected replay manifest key is required")
	}
	manifest, err := protocol.VerifyReplayDayManifest(data)
	if err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	if manifest.M0EmptyPartsCompatibility {
		return protocol.ReplayDayManifest{}, fmt.Errorf("M0 compatibility form is not an M3 manifest")
	}
	canonical, err := protocol.ReplayDayManifestCanonicalJSON(manifest)
	if err != nil || !bytes.Equal(canonical, data) {
		return protocol.ReplayDayManifest{}, fmt.Errorf("replay manifest bytes are not canonical")
	}
	digest, err := strictReplayDigest(manifest)
	if err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	derivedKey, err := protocol.ReplayDayManifestKey(manifest)
	if err != nil || derivedKey != expectedKey {
		return protocol.ReplayDayManifest{}, fmt.Errorf("replay manifest key does not match digest")
	}
	if err := expectedScope.Validate(); err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	if manifest.DatasetID != expectedScope.DatasetID || manifest.DayDefinitionID != expectedScope.DayDefinitionID || manifest.Date != expectedScope.Date || manifest.ReplayContractID != expectedScope.ReplayContractID || manifest.RawDayManifestKey != expectedScope.RawDayManifestKey || manifest.RawDayManifestSHA256 != expectedScope.RawDayManifestSHA256 {
		return protocol.ReplayDayManifest{}, fmt.Errorf("replay manifest scope or raw binding mismatch")
	}
	if err := validateConversionTuple(expectedScope, conversion); err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	expectedManifestID, err := replayManifestID(expectedScope, conversion)
	if err != nil || manifest.ManifestID != expectedManifestID {
		return protocol.ReplayDayManifest{}, fmt.Errorf("replay manifest identity does not match scope and conversion")
	}
	if manifest.FormatID != conversion.FormatID || manifest.ConversionID != conversion.ConversionID || manifest.ConverterBuildID != conversion.ConverterBuildID || manifest.DependencyLockHash != conversion.DependencyLockHash || manifest.WriterConfigurationHash != conversion.WriterConfigurationHash || manifest.TargetPlatformContract != conversion.TargetPlatformContract {
		return protocol.ReplayDayManifest{}, fmt.Errorf("replay manifest conversion tuple mismatch")
	}
	validatedParts, keys, root, err := validatePartSet(parts, expectedScope, conversion)
	if err != nil {
		return protocol.ReplayDayManifest{}, err
	}
	if len(keys) != len(manifest.PartManifestKeys) || !sameStrings(keys, manifest.PartManifestKeys) || root != manifest.PartSetRoot {
		return protocol.ReplayDayManifest{}, fmt.Errorf("replay manifest part set mismatch")
	}
	if len(parts) == 0 && manifest.CanonicalStreamRowChainRoot != ([32]byte{}) || len(parts) > 0 && manifest.CanonicalStreamRowChainRoot == ([32]byte{}) {
		return protocol.ReplayDayManifest{}, fmt.Errorf("replay manifest empty-day root mismatch")
	}
	if manifest.CanonicalStreamRowChainRoot != expectedRowChainRoot {
		return protocol.ReplayDayManifest{}, fmt.Errorf("replay manifest row-chain root does not match verified stream summary")
	}
	if len(validatedParts) > 0 && manifest.CanonicalStreamRowChainRoot != validatedParts[len(validatedParts)-1].LastRowChainHash {
		return protocol.ReplayDayManifest{}, fmt.Errorf("replay manifest row-chain root does not match final part")
	}
	if previous != nil {
		if err := verifyReplaySuccessor(expectedScope, conversion, manifest.Revision, *previous); err != nil {
			return protocol.ReplayDayManifest{}, err
		}
		previousDigest, err := strictReplayDigest(*previous)
		if err != nil || manifest.PreviousManifestSHA256 == nil || *manifest.PreviousManifestSHA256 != previousDigest {
			return protocol.ReplayDayManifest{}, fmt.Errorf("replay predecessor digest mismatch")
		}
	} else if manifest.Revision != 1 || manifest.PreviousManifestSHA256 != nil {
		return protocol.ReplayDayManifest{}, fmt.Errorf("genesis replay manifest has an invalid predecessor")
	}
	manifest.ManifestSHA256 = digest
	return manifest, nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// ReadAndVerifyPartManifest is a convenience for local and later remote
// readers. It does not infer a key from an untrusted full remote path.
func ReadAndVerifyPartManifest(path, expectedKey string, expectedDigest [32]byte) (protocol.PartManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return protocol.PartManifest{}, err
	}
	return VerifyPartManifestObject(data, expectedKey, expectedDigest)
}
